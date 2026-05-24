using System.Text;
using System.Text.Json;
using Azure;
using Azure.Storage;
using Azure.Storage.Queues;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Queue;

/// <summary>
/// Task that sends messages to Azure Queue Storage.
/// Supports concurrent message sending with retry logic.
/// </summary>
public sealed class QueueStorageSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private QueueClient? _queueClient;
    private string _queueName = "";
    private int _timeToLiveSeconds = QueueStorageConnectorConfig.DefaultTimeToLiveSeconds;
    private int _batchSize = QueueStorageConnectorConfig.DefaultBatchSize;
    private bool _base64Encode = true;
    private bool _autoCreateQueue;
    private int _maxRetryCount = QueueStorageConnectorConfig.DefaultMaxRetryCount;
    private long _retryDelayMs = QueueStorageConnectorConfig.DefaultRetryDelayMs;
    private bool _queueVerified;

    public override void Start(IDictionary<string, string> config)
    {
        _queueName = config[QueueStorageConnectorConfig.QueueNameConfig];
        _timeToLiveSeconds = int.Parse(GetConfigValue(config, QueueStorageConnectorConfig.TimeToLiveSecondsConfig, QueueStorageConnectorConfig.DefaultTimeToLiveSeconds.ToString()));
        _batchSize = int.Parse(GetConfigValue(config, QueueStorageConnectorConfig.BatchSizeConfig, QueueStorageConnectorConfig.DefaultBatchSize.ToString()));
        _base64Encode = bool.Parse(GetConfigValue(config, QueueStorageConnectorConfig.Base64EncodeConfig, "true"));
        _autoCreateQueue = bool.Parse(GetConfigValue(config, QueueStorageConnectorConfig.AutoCreateQueueConfig, "false"));
        _maxRetryCount = int.Parse(GetConfigValue(config, QueueStorageConnectorConfig.MaxRetryCountConfig, QueueStorageConnectorConfig.DefaultMaxRetryCount.ToString()));
        _retryDelayMs = long.Parse(GetConfigValue(config, QueueStorageConnectorConfig.RetryDelayMsConfig, QueueStorageConnectorConfig.DefaultRetryDelayMs.ToString()));

        // Create Queue client
        var connectionString = GetConfigValue(config, QueueStorageConnectorConfig.ConnectionStringConfig, "");
        var accountName = GetConfigValue(config, QueueStorageConnectorConfig.AccountNameConfig, "");
        var accountKey = GetConfigValue(config, QueueStorageConnectorConfig.AccountKeyConfig, "");
        var endpoint = GetConfigValue(config, QueueStorageConnectorConfig.EndpointConfig, "");

        var options = new QueueClientOptions
        {
            MessageEncoding = _base64Encode ? QueueMessageEncoding.Base64 : QueueMessageEncoding.None
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
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_queueClient == null || records.Count == 0)
            return;

        // Ensure queue exists
        if (!_queueVerified)
        {
            await EnsureQueueExistsAsync(cancellationToken);
            _queueVerified = true;
        }

        // Calculate TTL
        TimeSpan? ttl = _timeToLiveSeconds < 0 ? null : TimeSpan.FromSeconds(_timeToLiveSeconds);

        // Process in batches concurrently
        var tasks = new List<Task>();
        foreach (var batch in records.Chunk(_batchSize))
        {
            foreach (var record in batch)
            {
                // Skip tombstones (null/empty value)
                if (record.Value == null || record.Value.Length == 0)
                    continue;

                var task = SendMessageWithRetryAsync(record, ttl, cancellationToken);
                tasks.Add(task);
            }

            // Wait for batch to complete
            await Task.WhenAll(tasks);
            tasks.Clear();
        }
    }

    private async Task SendMessageWithRetryAsync(SinkRecord record, TimeSpan? ttl, CancellationToken cancellationToken)
    {
        var messageContent = GetMessageContent(record);
        var retries = 0;

        while (retries <= _maxRetryCount)
        {
            try
            {
                await _queueClient!.SendMessageAsync(
                    messageContent,
                    visibilityTimeout: null,
                    timeToLive: ttl,
                    cancellationToken: cancellationToken);
                return;
            }
            catch (RequestFailedException ex) when (IsTransient(ex.Status) && retries < _maxRetryCount)
            {
                retries++;
                await Task.Delay(TimeSpan.FromMilliseconds(_retryDelayMs * retries), cancellationToken);
            }
        }
    }

    private string GetMessageContent(SinkRecord record)
    {
        if (record.Value == null || record.Value.Length == 0)
            return "";

        // Try to use the value directly as a string if it's valid UTF-8
        try
        {
            var content = Encoding.UTF8.GetString(record.Value);

            // If it looks like JSON, use it as-is
            if (content.StartsWith('{') || content.StartsWith('['))
            {
                return content;
            }

            // Otherwise wrap it
            return content;
        }
        catch
        {
            // If not valid UTF-8, base64 encode
            return Convert.ToBase64String(record.Value);
        }
    }

    private static bool IsTransient(int statusCode)
    {
        return statusCode == 429 || statusCode == 500 || statusCode == 503;
    }

    private async Task EnsureQueueExistsAsync(CancellationToken cancellationToken)
    {
        if (!_autoCreateQueue)
            return;

        try
        {
            await _queueClient!.CreateIfNotExistsAsync(cancellationToken: cancellationToken);
        }
        catch (RequestFailedException)
        {
            // Queue might already exist or creation failed - continue anyway
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // Queue operations are executed in PutAsync
        return Task.CompletedTask;
    }
}
