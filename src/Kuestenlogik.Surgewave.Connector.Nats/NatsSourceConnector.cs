using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nats;

/// <summary>
/// A source connector that consumes messages from NATS JetStream and produces them to Surgewave.
/// </summary>
public sealed class NatsSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(NatsSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(NatsConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to write records to", EditorHint.Topic)
        .Define(NatsConnectorConfig.Url, ConfigType.String,
            NatsConnectorConfig.DefaultUrl, Importance.High,
            "NATS server URL")
        .Define(NatsConnectorConfig.StreamName, ConfigType.String, Importance.High,
            "JetStream stream name to consume from")
        .Define(NatsConnectorConfig.ConsumerName, ConfigType.String, Importance.High,
            "JetStream consumer name")
        .Define(NatsConnectorConfig.ConsumerDurable, ConfigType.Boolean,
            NatsConnectorConfig.DefaultConsumerDurable, Importance.Medium,
            "Create durable consumer")
        .Define(NatsConnectorConfig.DeliverPolicy, ConfigType.String,
            NatsConnectorConfig.DefaultDeliverPolicy, Importance.Medium,
            "Deliver policy: all, last, new, by_start_sequence, by_start_time")
        .Define(NatsConnectorConfig.AckPolicy, ConfigType.String,
            NatsConnectorConfig.DefaultAckPolicy, Importance.Medium,
            "Ack policy: explicit, none, all")
        .Define(NatsConnectorConfig.MaxAckPending, ConfigType.Int,
            NatsConnectorConfig.DefaultMaxAckPending, Importance.Low,
            "Maximum outstanding acks")
        .Define(NatsConnectorConfig.FetchBatchSize, ConfigType.Int,
            NatsConnectorConfig.DefaultFetchBatchSize, Importance.Low,
            "Number of messages to fetch per batch")
        .Define(NatsConnectorConfig.FetchTimeoutMs, ConfigType.Int,
            NatsConnectorConfig.DefaultFetchTimeoutMs, Importance.Low,
            "Fetch timeout in milliseconds")
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
        if (!config.ContainsKey(NatsConnectorConfig.Topic))
            throw new ArgumentException($"Missing required config: {NatsConnectorConfig.Topic}");
        if (!config.ContainsKey(NatsConnectorConfig.StreamName))
            throw new ArgumentException($"Missing required config: {NatsConnectorConfig.StreamName}");
        if (!config.ContainsKey(NatsConnectorConfig.ConsumerName))
            throw new ArgumentException($"Missing required config: {NatsConnectorConfig.ConsumerName}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // NATS consumer only supports a single task per consumer
        return [new Dictionary<string, string>(_config)];
    }
}
