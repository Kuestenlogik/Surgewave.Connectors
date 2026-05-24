using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.RabbitMQ;

/// <summary>
/// Source connector that consumes messages from RabbitMQ queues.
/// Supports queue declaration, prefetch configuration, and dead letter routing.
/// </summary>
public sealed class RabbitMQSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(RabbitMQSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(RabbitMQConnectorConfig.Host, ConfigType.String, RabbitMQConnectorConfig.DefaultHost, Importance.High, "RabbitMQ host")
        .Define(RabbitMQConnectorConfig.Port, ConfigType.Int, RabbitMQConnectorConfig.DefaultPort, Importance.Medium, "RabbitMQ port")
        .Define(RabbitMQConnectorConfig.VirtualHost, ConfigType.String, RabbitMQConnectorConfig.DefaultVirtualHost, Importance.Medium, "RabbitMQ virtual host")
        .Define(RabbitMQConnectorConfig.Username, ConfigType.String, RabbitMQConnectorConfig.DefaultUsername, Importance.Medium, "Username")
        .Define(RabbitMQConnectorConfig.Password, ConfigType.Password, RabbitMQConnectorConfig.DefaultPassword, Importance.Medium, "Password")
        .Define(RabbitMQConnectorConfig.Queue, ConfigType.String, Importance.High, "Queue to consume from")
        .Define(RabbitMQConnectorConfig.Topic, ConfigType.String, Importance.High, "Target Surgewave topic", EditorHint.Topic)
        .Define(RabbitMQConnectorConfig.QueueDurable, ConfigType.Boolean, true, Importance.Low, "Whether queue is durable")
        .Define(RabbitMQConnectorConfig.QueueExclusive, ConfigType.Boolean, false, Importance.Low, "Whether queue is exclusive")
        .Define(RabbitMQConnectorConfig.QueueAutoDelete, ConfigType.Boolean, false, Importance.Low, "Whether queue auto-deletes")
        .Define(RabbitMQConnectorConfig.PrefetchCount, ConfigType.Int, RabbitMQConnectorConfig.DefaultPrefetchCount, Importance.Medium, "Prefetch count")
        .Define(RabbitMQConnectorConfig.AutoAck, ConfigType.Boolean, false, Importance.Medium, "Auto-acknowledge messages")
        .Define(RabbitMQConnectorConfig.BatchSize, ConfigType.Int, RabbitMQConnectorConfig.DefaultBatchSize, Importance.Medium, "Batch size for polling")
        .Define(RabbitMQConnectorConfig.TlsEnabled, ConfigType.Boolean, false, Importance.Medium, "Enable TLS")
        .Define(RabbitMQConnectorConfig.DeadLetterExchange, ConfigType.String, "", Importance.Low, "Dead letter exchange");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(RabbitMQConnectorConfig.Queue, out var queue) || string.IsNullOrEmpty(queue))
            throw new ArgumentException($"Required configuration '{RabbitMQConnectorConfig.Queue}' is missing");

        if (!config.TryGetValue(RabbitMQConnectorConfig.Topic, out var topic) || string.IsNullOrEmpty(topic))
            throw new ArgumentException($"Required configuration '{RabbitMQConnectorConfig.Topic}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task per connector for queue consumption
        return [new Dictionary<string, string>(_config)];
    }
}
