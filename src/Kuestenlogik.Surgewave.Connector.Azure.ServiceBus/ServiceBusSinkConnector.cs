using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.ServiceBus;

/// <summary>
/// A sink connector that sends messages from Surgewave topics
/// to Azure Service Bus queues or topics.
/// </summary>
public sealed class ServiceBusSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(ServiceBusSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(ServiceBusConnectorConfig.ConnectionStringConfig, ConfigType.Password, Importance.High,
            "Azure Service Bus connection string")
        .Define(ServiceBusConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Surgewave topics to consume (comma-separated)", EditorHint.Topic)
        .Define(ServiceBusConnectorConfig.QueueNameConfig, ConfigType.String, "", Importance.Medium,
            "Queue name to send to (mutually exclusive with topic name)")
        .Define(ServiceBusConnectorConfig.TopicNameConfig, ConfigType.String, "", Importance.Medium,
            "Topic name to send to (mutually exclusive with queue name)")
        .Define(ServiceBusConnectorConfig.SessionIdFieldConfig, ConfigType.String, "", Importance.Medium,
            "Header or field name to use as SessionId for session-enabled queues/topics")
        .Define(ServiceBusConnectorConfig.PartitionKeyFieldConfig, ConfigType.String, "", Importance.Medium,
            "Header or field name to use as PartitionKey")
        .Define(ServiceBusConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)ServiceBusConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of messages to batch before sending")
        .Define(ServiceBusConnectorConfig.HeaderPrefixConfig, ConfigType.String, ServiceBusConnectorConfig.DefaultHeaderPrefix, Importance.Low,
            "Prefix for mapping Surgewave headers to Service Bus application properties");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(ServiceBusConnectorConfig.ConnectionStringConfig, out var connectionString) || string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException($"Missing required config: {ServiceBusConnectorConfig.ConnectionStringConfig}");

        if (!config.TryGetValue(ServiceBusConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrWhiteSpace(topics))
            throw new ArgumentException($"Missing required config: {ServiceBusConnectorConfig.TopicsConfig}");

        // Validate queue or topic
        var hasQueue = config.TryGetValue(ServiceBusConnectorConfig.QueueNameConfig, out var queueName) && !string.IsNullOrWhiteSpace(queueName);
        var hasTopic = config.TryGetValue(ServiceBusConnectorConfig.TopicNameConfig, out var topicName) && !string.IsNullOrWhiteSpace(topicName);

        if (!hasQueue && !hasTopic)
            throw new ArgumentException($"Must specify either {ServiceBusConnectorConfig.QueueNameConfig} or {ServiceBusConnectorConfig.TopicNameConfig}");

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
        // Single task is sufficient
        return [new Dictionary<string, string>(_config)];
    }
}
