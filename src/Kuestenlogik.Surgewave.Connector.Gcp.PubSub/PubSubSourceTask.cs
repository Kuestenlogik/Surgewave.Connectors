namespace Kuestenlogik.Surgewave.Connector.Gcp.PubSub;

using Google.Apis.Auth.OAuth2;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Encoding = System.Text.Encoding;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that pulls messages from a Google Cloud Pub/Sub subscription
/// and produces them to Surgewave topics.
/// </summary>
public sealed class PubSubSourceTask : SourceTask
{
    private SubscriberServiceApiClient? _subscriberClient;
    private SubscriptionName? _subscriptionName;
    private string _surgewaveTopic = "";
    private int _maxMessages = PubSubConnectorConfig.DefaultMaxMessages;
    private bool _autoAck = PubSubConnectorConfig.DefaultAutoAck;
    private string _headerPrefix = PubSubConnectorConfig.DefaultHeaderPrefix;
    private bool _includeMetadata = PubSubConnectorConfig.DefaultIncludeMetadata;

    private readonly Dictionary<string, object> _sourcePartition = new();
    private readonly List<string> _pendingAckIds = new();
    private readonly object _ackLock = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var projectId = config[PubSubConnectorConfig.ProjectIdConfig];
        var subscriptionId = config[PubSubConnectorConfig.SubscriptionIdConfig];
        _surgewaveTopic = config[PubSubConnectorConfig.SurgewaveTopicConfig];

        _maxMessages = GetConfigInt(config, PubSubConnectorConfig.MaxMessagesConfig, PubSubConnectorConfig.DefaultMaxMessages);
        _autoAck = GetConfigBool(config, PubSubConnectorConfig.AutoAckConfig, PubSubConnectorConfig.DefaultAutoAck);
        _headerPrefix = GetConfigValue(config, PubSubConnectorConfig.HeaderPrefixConfig, PubSubConnectorConfig.DefaultHeaderPrefix);
        _includeMetadata = GetConfigBool(config, PubSubConnectorConfig.IncludeMetadataConfig, PubSubConnectorConfig.DefaultIncludeMetadata);

        _subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);

        _sourcePartition["pubsub.project"] = projectId;
        _sourcePartition["pubsub.subscription"] = subscriptionId;

        var credentialsJson = GetConfigValue(config, PubSubConnectorConfig.CredentialsJsonConfig, "");
        var credentialsFile = GetConfigValue(config, PubSubConnectorConfig.CredentialsFileConfig, "");
        var emulatorHost = GetConfigValue(config, PubSubConnectorConfig.EmulatorHostConfig, "");

        _subscriberClient = CreateSubscriberClient(credentialsJson, credentialsFile, emulatorHost);
    }

    private static SubscriberServiceApiClient CreateSubscriberClient(
        string credentialsJson,
        string credentialsFile,
        string emulatorHost)
    {
        // Check for emulator first
        if (!string.IsNullOrEmpty(emulatorHost))
        {
            return new SubscriberServiceApiClientBuilder
            {
                Endpoint = emulatorHost,
                ChannelCredentials = ChannelCredentials.Insecure
            }.Build();
        }

        // Use explicit credentials if provided
#pragma warning disable CS0618 // GoogleCredential.FromJson/FromFile - CredentialFactory alternative requires internal IGoogleCredential
        if (!string.IsNullOrEmpty(credentialsJson))
        {
            return new SubscriberServiceApiClientBuilder
            {
                GoogleCredential = GoogleCredential.FromJson(credentialsJson)
            }.Build();
        }

        if (!string.IsNullOrEmpty(credentialsFile))
        {
            return new SubscriberServiceApiClientBuilder
            {
                GoogleCredential = GoogleCredential.FromFile(credentialsFile)
            }.Build();
        }
#pragma warning restore CS0618

        // Use default credentials (ADC - Application Default Credentials)
        return SubscriberServiceApiClient.Create();
    }

    public override void Stop()
    {
        // Acknowledge any pending messages before stopping
        AcknowledgePendingMessages();
        _subscriberClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            AcknowledgePendingMessages();
            _subscriberClient = null;
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_subscriberClient == null || _subscriptionName == null)
            return [];

        try
        {
            var response = await _subscriberClient.PullAsync(
                _subscriptionName,
                maxMessages: _maxMessages,
                cancellationToken);

            if (response.ReceivedMessages.Count == 0)
                return [];

            var records = new List<SourceRecord>(response.ReceivedMessages.Count);

            foreach (var receivedMessage in response.ReceivedMessages)
            {
                var record = CreateSourceRecord(receivedMessage);
                records.Add(record);

                if (_autoAck)
                {
                    // Acknowledge immediately if auto-ack is enabled
                    await _subscriberClient.AcknowledgeAsync(
                        _subscriptionName,
                        [receivedMessage.AckId],
                        cancellationToken);
                }
                else
                {
                    // Track for later acknowledgment on commit
                    lock (_ackLock)
                    {
                        _pendingAckIds.Add(receivedMessage.AckId);
                    }
                }
            }

            return records;
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
        {
            // No messages available within the deadline, return empty
            return [];
        }
    }

    public override async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_autoAck || _subscriberClient == null || _subscriptionName == null)
            return;

        await AcknowledgePendingMessagesAsync(cancellationToken);
    }

    private void AcknowledgePendingMessages()
    {
        AcknowledgePendingMessagesAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    private async Task AcknowledgePendingMessagesAsync(CancellationToken cancellationToken)
    {
        if (_subscriberClient == null || _subscriptionName == null)
            return;

        List<string> ackIds;
        lock (_ackLock)
        {
            if (_pendingAckIds.Count == 0)
                return;

            ackIds = [.. _pendingAckIds];
            _pendingAckIds.Clear();
        }

        // Acknowledge in batches of 1000 (Pub/Sub limit)
        const int batchSize = 1000;
        for (var i = 0; i < ackIds.Count; i += batchSize)
        {
            var batch = ackIds.Skip(i).Take(batchSize).ToList();
            await _subscriberClient.AcknowledgeAsync(_subscriptionName, batch, cancellationToken);
        }
    }

    private SourceRecord CreateSourceRecord(ReceivedMessage receivedMessage)
    {
        var message = receivedMessage.Message;
        var headers = new Dictionary<string, byte[]>();

        // Add Pub/Sub attributes as headers
        foreach (var attr in message.Attributes)
        {
            headers[$"{_headerPrefix}attr.{attr.Key}"] = Encoding.UTF8.GetBytes(attr.Value);
        }

        // Add metadata headers if configured
        if (_includeMetadata)
        {
            headers[$"{_headerPrefix}messageId"] = Encoding.UTF8.GetBytes(message.MessageId);
            headers[$"{_headerPrefix}publishTime"] = Encoding.UTF8.GetBytes(
                message.PublishTime.ToDateTimeOffset().ToString("O"));

            if (!string.IsNullOrEmpty(message.OrderingKey))
            {
                headers[$"{_headerPrefix}orderingKey"] = Encoding.UTF8.GetBytes(message.OrderingKey);
            }
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = new Dictionary<string, object>
            {
                ["messageId"] = message.MessageId,
                ["ackId"] = receivedMessage.AckId
            },
            Topic = _surgewaveTopic,
            Key = string.IsNullOrEmpty(message.OrderingKey)
                ? null
                : Encoding.UTF8.GetBytes(message.OrderingKey),
            Value = message.Data.ToByteArray(),
            Timestamp = message.PublishTime.ToDateTimeOffset(),
            Headers = headers
        };
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static int GetConfigInt(IDictionary<string, string> config, string key, int defaultValue)
        => config.TryGetValue(key, out var value) && int.TryParse(value, out var intValue) ? intValue : defaultValue;

    private static bool GetConfigBool(IDictionary<string, string> config, string key, bool defaultValue)
        => config.TryGetValue(key, out var value) && bool.TryParse(value, out var boolValue) ? boolValue : defaultValue;
}
