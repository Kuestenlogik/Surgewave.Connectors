using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Amqp;

/// <summary>
/// Sink connector that writes to AMQP exchanges (RabbitMQ, etc.).
/// </summary>
[ConnectorMetadata(
    Name = "amqp-sink",
    Description = "Writes messages to AMQP 0.9.1 exchanges (RabbitMQ compatible)",
    Author = "Surgewave",
    Tags = "amqp, rabbitmq, messaging, queue, sink")]
public sealed class AmqpSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(AmqpConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume from", EditorHint.Topic)
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
        .Define(AmqpConnectorConfig.TargetExchange, ConfigType.String, "", Importance.High,
            "Exchange to publish to (empty = default exchange)")
        .Define(AmqpConnectorConfig.TargetRoutingKey, ConfigType.String, "", Importance.Medium,
            "Default routing key for published messages")
        .Define(AmqpConnectorConfig.ExchangeType, ConfigType.String,
            AmqpConnectorConfig.DefaultExchangeType, Importance.Medium,
            "Exchange type: direct, fanout, topic, headers", EditorHint.Select, options: ["direct", "fanout", "topic", "headers"])
        .Define(AmqpConnectorConfig.DeclareExchange, ConfigType.Boolean, "true", Importance.Medium,
            "Declare exchange on startup")
        .Define(AmqpConnectorConfig.ExchangeDurable, ConfigType.Boolean, "true", Importance.Medium,
            "Make exchange durable")
        .Define(AmqpConnectorConfig.Persistent, ConfigType.Boolean, "true", Importance.Medium,
            "Make messages persistent")
        .Define(AmqpConnectorConfig.BatchSize, ConfigType.Int,
            AmqpConnectorConfig.DefaultBatchSize.ToString(), Importance.Medium,
            "Batch size for confirms");

    public override Type TaskClass => typeof(AmqpSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(AmqpConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{AmqpConnectorConfig.Topics}' is required");
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
