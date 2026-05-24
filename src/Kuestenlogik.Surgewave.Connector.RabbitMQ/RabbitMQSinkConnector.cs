using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.RabbitMQ;

/// <summary>
/// Sink connector that publishes messages to RabbitMQ exchanges.
/// Supports exchange declaration, routing key templating, and delivery confirmation.
/// </summary>
public sealed class RabbitMQSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(RabbitMQSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(RabbitMQConnectorConfig.Host, ConfigType.String, RabbitMQConnectorConfig.DefaultHost, Importance.High, "RabbitMQ host")
        .Define(RabbitMQConnectorConfig.Port, ConfigType.Int, RabbitMQConnectorConfig.DefaultPort, Importance.Medium, "RabbitMQ port")
        .Define(RabbitMQConnectorConfig.VirtualHost, ConfigType.String, RabbitMQConnectorConfig.DefaultVirtualHost, Importance.Medium, "RabbitMQ virtual host")
        .Define(RabbitMQConnectorConfig.Username, ConfigType.String, RabbitMQConnectorConfig.DefaultUsername, Importance.Medium, "Username")
        .Define(RabbitMQConnectorConfig.Password, ConfigType.Password, RabbitMQConnectorConfig.DefaultPassword, Importance.Medium, "Password")
        .Define(RabbitMQConnectorConfig.Topics, ConfigType.List, Importance.High, "Surgewave topics to consume", EditorHint.Topic)
        .Define(RabbitMQConnectorConfig.Exchange, ConfigType.String, "", Importance.High, "Target exchange (empty for default)")
        .Define(RabbitMQConnectorConfig.ExchangeType, ConfigType.String, RabbitMQConnectorConfig.DefaultExchangeType, Importance.Medium, "Exchange type (direct, fanout, topic, headers)", EditorHint.Select, options: ["direct", "fanout", "topic", "headers"])
        .Define(RabbitMQConnectorConfig.ExchangeDurable, ConfigType.Boolean, true, Importance.Low, "Whether exchange is durable")
        .Define(RabbitMQConnectorConfig.ExchangeAutoDelete, ConfigType.Boolean, false, Importance.Low, "Whether exchange auto-deletes")
        .Define(RabbitMQConnectorConfig.RoutingKeyTemplate, ConfigType.String, RabbitMQConnectorConfig.DefaultRoutingKeyTemplate, Importance.Medium, "Routing key template")
        .Define(RabbitMQConnectorConfig.Persistent, ConfigType.Boolean, true, Importance.Medium, "Make messages persistent")
        .Define(RabbitMQConnectorConfig.Mandatory, ConfigType.Boolean, false, Importance.Low, "Return unroutable messages")
        .Define(RabbitMQConnectorConfig.TlsEnabled, ConfigType.Boolean, false, Importance.Medium, "Enable TLS")
        .Define(RabbitMQConnectorConfig.ContentType, ConfigType.String, RabbitMQConnectorConfig.DefaultContentType, Importance.Low, "Default content type")
        .Define(RabbitMQConnectorConfig.MessageTtlMs, ConfigType.Int, 0, Importance.Low, "Message TTL in ms (0 = no expiry)");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(RabbitMQConnectorConfig.Topics, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{RabbitMQConnectorConfig.Topics}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task per connector
        return [new Dictionary<string, string>(_config)];
    }
}
