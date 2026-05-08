using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.ServiceBus;

/// <summary>
/// A source connector that receives messages from Azure Service Bus queues or subscriptions
/// and produces them to Surgewave topics.
/// </summary>
public sealed class ServiceBusSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(ServiceBusSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(ServiceBusConnectorConfig.ConnectionStringConfig, ConfigType.Password, Importance.High,
            "Azure Service Bus connection string")
        .Define(ServiceBusConnectorConfig.SurgewaveTopicConfig, ConfigType.String, Importance.High,
            "Destination Surgewave topic", EditorHint.Topic)
        .Define(ServiceBusConnectorConfig.QueueNameConfig, ConfigType.String, "", Importance.Medium,
            "Queue name to receive from (mutually exclusive with topic/subscription)")
        .Define(ServiceBusConnectorConfig.TopicNameConfig, ConfigType.String, "", Importance.Medium,
            "Topic name to receive from (requires subscription name)")
        .Define(ServiceBusConnectorConfig.SubscriptionNameConfig, ConfigType.String, "", Importance.Medium,
            "Subscription name for topic")
        .Define(ServiceBusConnectorConfig.ReceiveModeConfig, ConfigType.String, ServiceBusConnectorConfig.DefaultReceiveMode, Importance.Medium,
            "Receive mode: PeekLock (default) or ReceiveAndDelete", EditorHint.Select, options: ["PeekLock", "ReceiveAndDelete"])
        .Define(ServiceBusConnectorConfig.PrefetchCountConfig, ConfigType.Int, (long)ServiceBusConnectorConfig.DefaultPrefetchCount, Importance.Low,
            "Prefetch count for receiver optimization")
        .Define(ServiceBusConnectorConfig.MaxMessagesConfig, ConfigType.Int, (long)ServiceBusConnectorConfig.DefaultMaxMessages, Importance.Medium,
            "Maximum messages to receive per batch")
        .Define(ServiceBusConnectorConfig.HeaderPrefixConfig, ConfigType.String, ServiceBusConnectorConfig.DefaultHeaderPrefix, Importance.Low,
            "Prefix for Service Bus properties in Surgewave headers")
        .Define(ServiceBusConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, ServiceBusConnectorConfig.DefaultIncludeMetadata, Importance.Low,
            "Include Service Bus metadata (messageId, sequenceNumber) in headers");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(ServiceBusConnectorConfig.ConnectionStringConfig, out var connectionString) || string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException($"Missing required config: {ServiceBusConnectorConfig.ConnectionStringConfig}");

        if (!config.TryGetValue(ServiceBusConnectorConfig.SurgewaveTopicConfig, out var surgewaveTopic) || string.IsNullOrWhiteSpace(surgewaveTopic))
            throw new ArgumentException($"Missing required config: {ServiceBusConnectorConfig.SurgewaveTopicConfig}");

        // Validate queue or topic/subscription
        var hasQueue = config.TryGetValue(ServiceBusConnectorConfig.QueueNameConfig, out var queueName) && !string.IsNullOrWhiteSpace(queueName);
        var hasTopic = config.TryGetValue(ServiceBusConnectorConfig.TopicNameConfig, out var topicName) && !string.IsNullOrWhiteSpace(topicName);
        var hasSubscription = config.TryGetValue(ServiceBusConnectorConfig.SubscriptionNameConfig, out var subscriptionName) && !string.IsNullOrWhiteSpace(subscriptionName);

        if (!hasQueue && !(hasTopic && hasSubscription))
            throw new ArgumentException($"Must specify either {ServiceBusConnectorConfig.QueueNameConfig} or both {ServiceBusConnectorConfig.TopicNameConfig} and {ServiceBusConnectorConfig.SubscriptionNameConfig}");

        if (hasQueue && hasTopic)
            throw new ArgumentException($"{ServiceBusConnectorConfig.QueueNameConfig} and {ServiceBusConnectorConfig.TopicNameConfig} are mutually exclusive");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task per queue/subscription
        return [new Dictionary<string, string>(_config)];
    }
}
