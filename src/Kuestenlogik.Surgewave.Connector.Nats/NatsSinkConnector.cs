using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nats;

/// <summary>
/// A sink connector that publishes records to NATS JetStream.
/// </summary>
public sealed class NatsSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(NatsSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(NatsConnectorConfig.Topics, ConfigType.String, Importance.High,
            "Surgewave topics to consume from (comma-separated)", EditorHint.Topic)
        .Define(NatsConnectorConfig.Url, ConfigType.String,
            NatsConnectorConfig.DefaultUrl, Importance.High,
            "NATS server URL")
        .Define(NatsConnectorConfig.StreamName, ConfigType.String, Importance.High,
            "JetStream stream name to publish to")
        .Define(NatsConnectorConfig.PublishTimeoutMs, ConfigType.Int,
            NatsConnectorConfig.DefaultPublishTimeoutMs, Importance.Low,
            "Publish timeout in milliseconds")
        .Define(NatsConnectorConfig.Retries, ConfigType.Int,
            NatsConnectorConfig.DefaultRetries, Importance.Low,
            "Number of publish retries on failure")
        .Define(NatsConnectorConfig.CredentialsFile, ConfigType.String,
            "", Importance.Low, "Path to NATS credentials file")
        .Define(NatsConnectorConfig.Token, ConfigType.String,
            "", Importance.Low, "NATS token for authentication")
        .Define(NatsConnectorConfig.Username, ConfigType.String,
            "", Importance.Low, "NATS username")
        .Define(NatsConnectorConfig.Password, ConfigType.String,
            "", Importance.Low, "NATS password")
        .Define(NatsConnectorConfig.UseTls, ConfigType.Boolean,
            NatsConnectorConfig.DefaultUseTls, Importance.Medium,
            "Enable TLS")
        .Define(NatsConnectorConfig.ReconnectWaitMs, ConfigType.Int,
            NatsConnectorConfig.DefaultReconnectWaitMs, Importance.Low,
            "Wait time between reconnection attempts")
        .Define(NatsConnectorConfig.MaxReconnects, ConfigType.Int,
            NatsConnectorConfig.DefaultMaxReconnects, Importance.Low,
            "Maximum reconnection attempts (-1 for unlimited)");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.ContainsKey(NatsConnectorConfig.Topics))
            throw new ArgumentException($"Missing required config: {NatsConnectorConfig.Topics}");
        if (!config.ContainsKey(NatsConnectorConfig.StreamName))
            throw new ArgumentException($"Missing required config: {NatsConnectorConfig.StreamName}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // NATS publisher can run as single task
        return [new Dictionary<string, string>(_config)];
    }
}
