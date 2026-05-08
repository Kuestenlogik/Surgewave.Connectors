namespace Kuestenlogik.Surgewave.Connector.Gcp.PubSub;

using Google.Apis.Auth.OAuth2;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Core;
using Encoding = System.Text.Encoding;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that publishes messages to a Google Cloud Pub/Sub topic.
/// </summary>
public sealed class PubSubSinkTask : SinkTask
{
    private PublisherServiceApiClient? _publisherClient;
    private TopicName? _topicName;
    private string _orderingKeyField = "";
    private string _headerPrefix = PubSubConnectorConfig.DefaultHeaderPrefix;
    private int _batchSize = PubSubConnectorConfig.DefaultBatchSize;

    private readonly List<PubsubMessage> _messageBuffer = new();
    private readonly object _bufferLock = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var projectId = config[PubSubConnectorConfig.ProjectIdConfig];
        var topicId = config[PubSubConnectorConfig.PubSubTopicIdConfig];

        _topicName = TopicName.FromProjectTopic(projectId, topicId);
        _orderingKeyField = GetConfigValue(config, PubSubConnectorConfig.OrderingKeyFieldConfig, "");
        _headerPrefix = GetConfigValue(config, PubSubConnectorConfig.HeaderPrefixConfig, PubSubConnectorConfig.DefaultHeaderPrefix);
        _batchSize = GetConfigInt(config, PubSubConnectorConfig.BatchSizeConfig, PubSubConnectorConfig.DefaultBatchSize);

        var credentialsJson = GetConfigValue(config, PubSubConnectorConfig.CredentialsJsonConfig, "");
        var credentialsFile = GetConfigValue(config, PubSubConnectorConfig.CredentialsFileConfig, "");
        var emulatorHost = GetConfigValue(config, PubSubConnectorConfig.EmulatorHostConfig, "");

        _publisherClient = CreatePublisherClient(credentialsJson, credentialsFile, emulatorHost);
    }

    private static PublisherServiceApiClient CreatePublisherClient(
        string credentialsJson,
        string credentialsFile,
        string emulatorHost)
    {
        // Check for emulator first
        if (!string.IsNullOrEmpty(emulatorHost))
        {
            return new PublisherServiceApiClientBuilder
            {
                Endpoint = emulatorHost,
                ChannelCredentials = ChannelCredentials.Insecure
            }.Build();
        }

        // Use explicit credentials if provided
#pragma warning disable CS0618 // GoogleCredential.FromJson/FromFile - CredentialFactory alternative requires internal IGoogleCredential
        if (!string.IsNullOrEmpty(credentialsJson))
        {
            return new PublisherServiceApiClientBuilder
            {
                GoogleCredential = GoogleCredential.FromJson(credentialsJson)
            }.Build();
        }

        if (!string.IsNullOrEmpty(credentialsFile))
        {
            return new PublisherServiceApiClientBuilder
            {
                GoogleCredential = GoogleCredential.FromFile(credentialsFile)
            }.Build();
        }
#pragma warning restore CS0618

        // Use default credentials (ADC - Application Default Credentials)
        return PublisherServiceApiClient.Create();
    }

    public override void Stop()
    {
        // Flush remaining messages
        FlushBufferAsync(CancellationToken.None).GetAwaiter().GetResult();
        _publisherClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            try
            {
                FlushBufferAsync(CancellationToken.None).GetAwaiter().GetResult();
            }
            catch
            {
                // Ignore disposal errors
            }
            _publisherClient = null;
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_publisherClient == null || _topicName == null || records.Count == 0)
            return;

        foreach (var record in records)
        {
            var message = CreatePubsubMessage(record);

            lock (_bufferLock)
            {
                _messageBuffer.Add(message);
            }

            // Flush when batch size is reached
            if (_messageBuffer.Count >= _batchSize)
            {
                await FlushBufferAsync(cancellationToken);
            }
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        await FlushBufferAsync(cancellationToken);
    }

    private async Task FlushBufferAsync(CancellationToken cancellationToken)
    {
        if (_publisherClient == null || _topicName == null)
            return;

        List<PubsubMessage> messages;
        lock (_bufferLock)
        {
            if (_messageBuffer.Count == 0)
                return;

            messages = [.. _messageBuffer];
            _messageBuffer.Clear();
        }

        // Publish in batches (Pub/Sub allows up to 1000 messages per request)
        const int maxBatchSize = 1000;
        for (var i = 0; i < messages.Count; i += maxBatchSize)
        {
            var batch = messages.Skip(i).Take(maxBatchSize).ToList();
            var request = new PublishRequest
            {
                TopicAsTopicName = _topicName,
                Messages = { batch }
            };

            await _publisherClient.PublishAsync(request, cancellationToken);
        }
    }

    private PubsubMessage CreatePubsubMessage(SinkRecord record)
    {
        var message = new PubsubMessage
        {
            Data = ByteString.CopyFrom(record.Value ?? [])
        };

        // Set ordering key if configured
        if (!string.IsNullOrEmpty(_orderingKeyField))
        {
            var orderingKey = GetOrderingKey(record);
            if (!string.IsNullOrEmpty(orderingKey))
            {
                message.OrderingKey = orderingKey;
            }
        }

        // Map Surgewave headers to Pub/Sub attributes
        if (record.Headers != null)
        {
            foreach (var header in record.Headers)
            {
                // Skip internal headers, only map those with the configured prefix
                if (header.Key.StartsWith(_headerPrefix + "attr.", StringComparison.Ordinal))
                {
                    var attrKey = header.Key[(_headerPrefix.Length + 5)..]; // Remove prefix + "attr."
                    message.Attributes[attrKey] = Encoding.UTF8.GetString(header.Value);
                }
                else if (!header.Key.StartsWith(_headerPrefix, StringComparison.Ordinal))
                {
                    // Map non-prefixed headers as attributes
                    message.Attributes[header.Key] = Encoding.UTF8.GetString(header.Value);
                }
            }
        }

        // Add Surgewave metadata as attributes
        if (record.Topic != null)
        {
            message.Attributes["surgewave.topic"] = record.Topic;
        }
        message.Attributes["surgewave.partition"] = record.Partition.ToString();
        message.Attributes["surgewave.offset"] = record.Offset.ToString();

        return message;
    }

    private string? GetOrderingKey(SinkRecord record)
    {
        // First, check if the ordering key is in headers
        if (record.Headers != null && record.Headers.TryGetValue(_orderingKeyField, out var headerValue))
        {
            return Encoding.UTF8.GetString(headerValue);
        }

        // Use record key as ordering key if field matches "key"
        if (_orderingKeyField.Equals("key", StringComparison.OrdinalIgnoreCase) && record.Key != null)
        {
            return Encoding.UTF8.GetString(record.Key);
        }

        return null;
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static int GetConfigInt(IDictionary<string, string> config, string key, int defaultValue)
        => config.TryGetValue(key, out var value) && int.TryParse(value, out var intValue) ? intValue : defaultValue;
}
