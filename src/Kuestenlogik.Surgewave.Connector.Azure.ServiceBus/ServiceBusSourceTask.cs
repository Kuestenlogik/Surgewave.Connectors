namespace Kuestenlogik.Surgewave.Connector.Azure.ServiceBus;

using System.Text;
using global::Azure.Messaging.ServiceBus;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that receives messages from an Azure Service Bus queue or subscription
/// and produces them to Surgewave topics.
/// </summary>
public sealed class ServiceBusSourceTask : SourceTask
{
    private ServiceBusClient? _client;
    private ServiceBusReceiver? _receiver;
    private string _surgewaveTopic = "";
    private int _maxMessages = ServiceBusConnectorConfig.DefaultMaxMessages;
    private string _headerPrefix = ServiceBusConnectorConfig.DefaultHeaderPrefix;
    private bool _includeMetadata = ServiceBusConnectorConfig.DefaultIncludeMetadata;
    private bool _isPeekLock = true;

    private readonly Dictionary<string, object> _sourcePartition = new();
    private readonly List<ServiceBusReceivedMessage> _pendingMessages = new();
    private readonly object _messageLock = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var connectionString = config[ServiceBusConnectorConfig.ConnectionStringConfig];
        _surgewaveTopic = config[ServiceBusConnectorConfig.SurgewaveTopicConfig];

        var queueName = GetConfigValue(config, ServiceBusConnectorConfig.QueueNameConfig, "");
        var topicName = GetConfigValue(config, ServiceBusConnectorConfig.TopicNameConfig, "");
        var subscriptionName = GetConfigValue(config, ServiceBusConnectorConfig.SubscriptionNameConfig, "");

        var receiveMode = GetConfigValue(config, ServiceBusConnectorConfig.ReceiveModeConfig, ServiceBusConnectorConfig.DefaultReceiveMode);
        var prefetchCount = GetConfigInt(config, ServiceBusConnectorConfig.PrefetchCountConfig, ServiceBusConnectorConfig.DefaultPrefetchCount);
        _maxMessages = GetConfigInt(config, ServiceBusConnectorConfig.MaxMessagesConfig, ServiceBusConnectorConfig.DefaultMaxMessages);
        _headerPrefix = GetConfigValue(config, ServiceBusConnectorConfig.HeaderPrefixConfig, ServiceBusConnectorConfig.DefaultHeaderPrefix);
        _includeMetadata = GetConfigBool(config, ServiceBusConnectorConfig.IncludeMetadataConfig, ServiceBusConnectorConfig.DefaultIncludeMetadata);

        _isPeekLock = receiveMode.Equals("PeekLock", StringComparison.OrdinalIgnoreCase);

        // Set up source partition for offset tracking
        if (!string.IsNullOrEmpty(queueName))
        {
            _sourcePartition["servicebus.queue"] = queueName;
        }
        else
        {
            _sourcePartition["servicebus.topic"] = topicName;
            _sourcePartition["servicebus.subscription"] = subscriptionName;
        }

        _client = new ServiceBusClient(connectionString);

        var receiverOptions = new ServiceBusReceiverOptions
        {
            ReceiveMode = _isPeekLock ? ServiceBusReceiveMode.PeekLock : ServiceBusReceiveMode.ReceiveAndDelete,
            PrefetchCount = prefetchCount
        };

        _receiver = !string.IsNullOrEmpty(queueName)
            ? _client.CreateReceiver(queueName, receiverOptions)
            : _client.CreateReceiver(topicName, subscriptionName, receiverOptions);
    }

    public override void Stop()
    {
        // Complete any pending messages before stopping
        CompletePendingMessagesAsync(CancellationToken.None).GetAwaiter().GetResult();

        _receiver?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _client?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _receiver = null;
        _client = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            try
            {
                CompletePendingMessagesAsync(CancellationToken.None).GetAwaiter().GetResult();
            }
            catch
            {
                // Ignore disposal errors
            }
            _receiver?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _client?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _receiver = null;
            _client = null;
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_receiver == null)
            return [];

        var messages = await _receiver.ReceiveMessagesAsync(
            _maxMessages,
            TimeSpan.FromSeconds(5),
            cancellationToken);

        if (messages.Count == 0)
            return [];

        var records = new List<SourceRecord>(messages.Count);

        foreach (var message in messages)
        {
            var record = CreateSourceRecord(message);
            records.Add(record);

            if (_isPeekLock)
            {
                // Track for later completion on commit
                lock (_messageLock)
                {
                    _pendingMessages.Add(message);
                }
            }
        }

        return records;
    }

    public override async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (!_isPeekLock || _receiver == null)
            return;

        await CompletePendingMessagesAsync(cancellationToken);
    }

    private async Task CompletePendingMessagesAsync(CancellationToken cancellationToken)
    {
        if (_receiver == null)
            return;

        List<ServiceBusReceivedMessage> messages;
        lock (_messageLock)
        {
            if (_pendingMessages.Count == 0)
                return;

            messages = [.. _pendingMessages];
            _pendingMessages.Clear();
        }

        foreach (var message in messages)
        {
            try
            {
                await _receiver.CompleteMessageAsync(message, cancellationToken);
            }
            catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessageLockLost)
            {
                // Message lock was lost, message will be redelivered
            }
        }
    }

    private SourceRecord CreateSourceRecord(ServiceBusReceivedMessage message)
    {
        var headers = new Dictionary<string, byte[]>();

        // Add application properties as headers
        foreach (var prop in message.ApplicationProperties)
        {
            if (prop.Value != null)
            {
                headers[$"{_headerPrefix}prop.{prop.Key}"] = Encoding.UTF8.GetBytes(prop.Value.ToString() ?? "");
            }
        }

        // Add metadata headers if configured
        if (_includeMetadata)
        {
            headers[$"{_headerPrefix}messageId"] = Encoding.UTF8.GetBytes(message.MessageId);
            headers[$"{_headerPrefix}sequenceNumber"] = Encoding.UTF8.GetBytes(message.SequenceNumber.ToString());

            if (!string.IsNullOrEmpty(message.CorrelationId))
            {
                headers[$"{_headerPrefix}correlationId"] = Encoding.UTF8.GetBytes(message.CorrelationId);
            }
            if (!string.IsNullOrEmpty(message.SessionId))
            {
                headers[$"{_headerPrefix}sessionId"] = Encoding.UTF8.GetBytes(message.SessionId);
            }
            if (!string.IsNullOrEmpty(message.PartitionKey))
            {
                headers[$"{_headerPrefix}partitionKey"] = Encoding.UTF8.GetBytes(message.PartitionKey);
            }
            if (!string.IsNullOrEmpty(message.ContentType))
            {
                headers[$"{_headerPrefix}contentType"] = Encoding.UTF8.GetBytes(message.ContentType);
            }
            if (!string.IsNullOrEmpty(message.Subject))
            {
                headers[$"{_headerPrefix}subject"] = Encoding.UTF8.GetBytes(message.Subject);
            }
        }

        // Use session id as key for ordered processing
        byte[]? key = null;
        if (!string.IsNullOrEmpty(message.SessionId))
        {
            key = Encoding.UTF8.GetBytes(message.SessionId);
        }
        else if (!string.IsNullOrEmpty(message.PartitionKey))
        {
            key = Encoding.UTF8.GetBytes(message.PartitionKey);
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = new Dictionary<string, object>
            {
                ["messageId"] = message.MessageId,
                ["sequenceNumber"] = message.SequenceNumber
            },
            Topic = _surgewaveTopic,
            Key = key,
            Value = message.Body.ToArray(),
            Timestamp = message.EnqueuedTime,
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
