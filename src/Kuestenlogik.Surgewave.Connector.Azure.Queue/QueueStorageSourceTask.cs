using System.Text;
using System.Text.Json;
using Azure;
using Azure.Storage;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Queue;

/// <summary>
/// Task that receives messages from Azure Queue Storage.
/// Uses visibility timeout for at-least-once delivery with commit-based deletion.
/// </summary>
public sealed class QueueStorageSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private QueueClient? _queueClient;
    private string _queueName = "";
    private string _topicPattern = QueueStorageConnectorConfig.DefaultTopicPattern;
    private long _pollIntervalMs = QueueStorageConnectorConfig.DefaultPollIntervalMs;
    private int _maxMessagesPerPoll = QueueStorageConnectorConfig.DefaultMaxMessagesPerPoll;
    private int _visibilityTimeoutSeconds = QueueStorageConnectorConfig.DefaultVisibilityTimeoutSeconds;
    private bool _deleteAfterRead;
    private bool _base64Decode = true;
    private bool _includeMetadata = true;

    private readonly Dictionary<string, object> _sourcePartition = new();
    private readonly List<QueueMessage> _pendingMessages = new();

    public override void Start(IDictionary<string, string> config)
    {
        _queueName = config[QueueStorageConnectorConfig.QueueNameConfig];
        _topicPattern = GetConfigValue(config, QueueStorageConnectorConfig.TopicPatternConfig, QueueStorageConnectorConfig.DefaultTopicPattern);
        _pollIntervalMs = long.Parse(GetConfigValue(config, QueueStorageConnectorConfig.PollIntervalMsConfig, QueueStorageConnectorConfig.DefaultPollIntervalMs.ToString()));
        _maxMessagesPerPoll = Math.Min(32, int.Parse(GetConfigValue(config, QueueStorageConnectorConfig.MaxMessagesPerPollConfig, QueueStorageConnectorConfig.DefaultMaxMessagesPerPoll.ToString())));
        _visibilityTimeoutSeconds = int.Parse(GetConfigValue(config, QueueStorageConnectorConfig.VisibilityTimeoutSecondsConfig, QueueStorageConnectorConfig.DefaultVisibilityTimeoutSeconds.ToString()));
        _deleteAfterRead = bool.Parse(GetConfigValue(config, QueueStorageConnectorConfig.DeleteAfterReadConfig, "false"));
        _base64Decode = bool.Parse(GetConfigValue(config, QueueStorageConnectorConfig.Base64DecodeConfig, "true"));
        _includeMetadata = bool.Parse(GetConfigValue(config, QueueStorageConnectorConfig.IncludeMetadataConfig, "true"));

        _sourcePartition["queue"] = _queueName;

        // Create Queue client
        var connectionString = GetConfigValue(config, QueueStorageConnectorConfig.ConnectionStringConfig, "");
        var accountName = GetConfigValue(config, QueueStorageConnectorConfig.AccountNameConfig, "");
        var accountKey = GetConfigValue(config, QueueStorageConnectorConfig.AccountKeyConfig, "");
        var endpoint = GetConfigValue(config, QueueStorageConnectorConfig.EndpointConfig, "");

        var options = new QueueClientOptions
        {
            MessageEncoding = _base64Decode ? QueueMessageEncoding.Base64 : QueueMessageEncoding.None
        };

        if (!string.IsNullOrEmpty(connectionString))
        {
            _queueClient = new QueueClient(connectionString, _queueName, options);
        }
        else if (!string.IsNullOrEmpty(accountName) && !string.IsNullOrEmpty(accountKey))
        {
            var serviceUri = !string.IsNullOrEmpty(endpoint)
                ? new Uri($"{endpoint.TrimEnd('/')}/{_queueName}")
                : new Uri($"https://{accountName}.queue.core.windows.net/{_queueName}");

            var credential = new StorageSharedKeyCredential(accountName, accountKey);
            _queueClient = new QueueClient(serviceUri, credential, options);
        }
        else if (!string.IsNullOrEmpty(endpoint))
        {
            // For Azurite with default credentials - use connection string format
            var azuriteConnectionString = $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint={endpoint}";
            _queueClient = new QueueClient(azuriteConnectionString, _queueName, options);
        }
        else
        {
            throw new ArgumentException("Connection string or account name/key must be provided");
        }
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        _queueClient = null;
        _pendingMessages.Clear();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_queueClient == null)
            return [];

        var records = new List<SourceRecord>();

        try
        {
            var response = await _queueClient.ReceiveMessagesAsync(
                maxMessages: _maxMessagesPerPoll,
                visibilityTimeout: TimeSpan.FromSeconds(_visibilityTimeoutSeconds),
                cancellationToken: cancellationToken);

            if (response.Value == null || response.Value.Length == 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs), cancellationToken);
                return [];
            }

            foreach (var message in response.Value)
            {
                var record = ConvertToSourceRecord(message);
                records.Add(record);

                if (_deleteAfterRead)
                {
                    // Delete immediately after reading
                    await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, cancellationToken);
                }
                else
                {
                    // Track for deletion on commit
                    _pendingMessages.Add(message);
                }
            }
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // Queue doesn't exist, wait and retry
            await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs), cancellationToken);
        }

        return records;
    }

    private SourceRecord ConvertToSourceRecord(QueueMessage message)
    {
        // Build key
        var key = new Dictionary<string, object>
        {
            ["message_id"] = message.MessageId
        };

        // Get message content
        var content = message.Body?.ToString() ?? "";

        // Build payload
        Dictionary<string, object?> payload;
        if (_includeMetadata)
        {
            payload = new Dictionary<string, object?>
            {
                ["source"] = new Dictionary<string, object>
                {
                    ["queue"] = _queueName,
                    ["message_id"] = message.MessageId,
                    ["pop_receipt"] = message.PopReceipt,
                    ["dequeue_count"] = message.DequeueCount,
                    ["inserted_on"] = message.InsertedOn?.ToString("O") ?? "",
                    ["expires_on"] = message.ExpiresOn?.ToString("O") ?? "",
                    ["next_visible_on"] = message.NextVisibleOn?.ToString("O") ?? ""
                },
                ["data"] = TryParseJson(content),
                ["ts_ms"] = message.InsertedOn?.ToUnixTimeMilliseconds() ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };
        }
        else
        {
            // Try to parse as JSON, otherwise use raw content
            var parsed = TryParseJson(content);
            if (parsed is Dictionary<string, object?> dict)
            {
                payload = dict;
            }
            else
            {
                payload = new Dictionary<string, object?>
                {
                    ["data"] = parsed
                };
            }
        }

        // Build offset
        var offset = new Dictionary<string, object>
        {
            [QueueStorageConnectorConfig.OffsetMessageId] = message.MessageId,
            [QueueStorageConnectorConfig.OffsetPopReceipt] = message.PopReceipt,
            [QueueStorageConnectorConfig.OffsetDequeueCount] = message.DequeueCount
        };

        // Build headers
        var headers = new Dictionary<string, byte[]>
        {
            [QueueStorageConnectorConfig.HeaderQueueName] = Encoding.UTF8.GetBytes(_queueName),
            [QueueStorageConnectorConfig.HeaderMessageId] = Encoding.UTF8.GetBytes(message.MessageId),
            [QueueStorageConnectorConfig.HeaderPopReceipt] = Encoding.UTF8.GetBytes(message.PopReceipt),
            [QueueStorageConnectorConfig.HeaderDequeueCount] = Encoding.UTF8.GetBytes(message.DequeueCount.ToString())
        };

        if (message.InsertedOn.HasValue)
        {
            headers[QueueStorageConnectorConfig.HeaderInsertedOn] = Encoding.UTF8.GetBytes(message.InsertedOn.Value.ToString("O"));
        }
        if (message.ExpiresOn.HasValue)
        {
            headers[QueueStorageConnectorConfig.HeaderExpiresOn] = Encoding.UTF8.GetBytes(message.ExpiresOn.Value.ToString("O"));
        }
        if (message.NextVisibleOn.HasValue)
        {
            headers[QueueStorageConnectorConfig.HeaderNextVisibleOn] = Encoding.UTF8.GetBytes(message.NextVisibleOn.Value.ToString("O"));
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = offset,
            Topic = GetTopicName(),
            Key = JsonSerializer.SerializeToUtf8Bytes(key),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = message.InsertedOn ?? DateTimeOffset.UtcNow,
            Headers = headers
        };
    }

    private static object? TryParseJson(string content)
    {
        if (string.IsNullOrEmpty(content))
            return content;

        try
        {
            return JsonSerializer.Deserialize<Dictionary<string, object?>>(content);
        }
        catch
        {
            return content;
        }
    }

    private string GetTopicName()
    {
        return _topicPattern.Replace("${queue}", _queueName);
    }

    public override async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_queueClient == null || _deleteAfterRead)
            return;

        // Delete all pending messages
        foreach (var message in _pendingMessages)
        {
            try
            {
                await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, cancellationToken);
            }
            catch (RequestFailedException ex) when (ex.Status == 404 || ex.Status == 400)
            {
                // Message already deleted or visibility timeout expired, ignore
            }
        }

        _pendingMessages.Clear();
    }
}
