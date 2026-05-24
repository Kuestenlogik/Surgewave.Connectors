using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Amqp;

/// <summary>
/// Source connector that reads from AMQP queues (RabbitMQ, etc.).
/// </summary>
[ConnectorMetadata(
    Name = "amqp-source",
    Description = "Reads messages from AMQP 0.9.1 queues (RabbitMQ compatible)",
    Author = "Surgewave",
    Tags = "amqp, rabbitmq, messaging, queue, source")]
public sealed class AmqpSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(AmqpConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce messages to", EditorHint.Topic)
        .Define(AmqpConnectorConfig.Uri, ConfigType.Password, "", Importance.High,
            "AMQP URI (alternative to individual settings)")
        .Define(AmqpConnectorConfig.Host, ConfigType.String, "localhost", Importance.High,
            "AMQP broker host")
        .Define(AmqpConnectorConfig.Port, ConfigType.Int,
            AmqpConnectorConfig.DefaultPort.ToString(), Importance.Medium,
            "AMQP broker port")
        .Define(AmqpConnectorConfig.VirtualHost, ConfigType.String,
            AmqpConnectorConfig.DefaultVirtualHost, Importance.Medium,
            "AMQP virtual host")
        .Define(AmqpConnectorConfig.Username, ConfigType.String, "guest", Importance.Medium,
            "AMQP username")
        .Define(AmqpConnectorConfig.Password, ConfigType.Password, "guest", Importance.Medium,
            "AMQP password")
        .Define(AmqpConnectorConfig.UseSsl, ConfigType.Boolean, "false", Importance.Medium,
            "Enable SSL/TLS")
        .Define(AmqpConnectorConfig.SourceQueue, ConfigType.String, Importance.High,
            "Queue to consume from")
        .Define(AmqpConnectorConfig.SourceExchange, ConfigType.String, "", Importance.Medium,
            "Exchange to bind queue to (optional)")
        .Define(AmqpConnectorConfig.SourceRoutingKey, ConfigType.String, "#", Importance.Medium,
            "Routing key for queue binding")
        .Define(AmqpConnectorConfig.AutoAck, ConfigType.Boolean, "false", Importance.Medium,
            "Auto-acknowledge messages")
        .Define(AmqpConnectorConfig.PrefetchCount, ConfigType.Int,
            AmqpConnectorConfig.DefaultPrefetchCount.ToString(), Importance.Medium,
            "Prefetch count for QoS")
        .Define(AmqpConnectorConfig.DeclareQueue, ConfigType.Boolean, "true", Importance.Medium,
            "Declare queue on startup")
        .Define(AmqpConnectorConfig.QueueDurable, ConfigType.Boolean, "true", Importance.Medium,
            "Make queue durable")
        .Define(AmqpConnectorConfig.QueueExclusive, ConfigType.Boolean, "false", Importance.Low,
            "Make queue exclusive")
        .Define(AmqpConnectorConfig.QueueAutoDelete, ConfigType.Boolean, "false", Importance.Low,
            "Auto-delete queue when unused");

    public override Type TaskClass => typeof(AmqpSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(AmqpConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{AmqpConnectorConfig.Topic}' is required");
        }

        if (!config.TryGetValue(AmqpConnectorConfig.SourceQueue, out var queue) ||
            string.IsNullOrWhiteSpace(queue))
        {
            throw new ArgumentException($"'{AmqpConnectorConfig.SourceQueue}' is required");
        }
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }

    public override void Stop()
    {
    }
}
