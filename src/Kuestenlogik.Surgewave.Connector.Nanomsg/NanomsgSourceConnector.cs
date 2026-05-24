using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Nanomsg;

/// <summary>
/// Source connector that receives messages from nanomsg sockets.
/// </summary>
[ConnectorMetadata(
    Name = "nanomsg-source",
    Description = "Receives messages from nanomsg/nng sockets (SUB, PULL, REP, RESPONDENT, BUS patterns)",
    Author = "Surgewave",
    Tags = "nanomsg, nng, messaging, brokerless, source")]
public sealed class NanomsgSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(NanomsgConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce nanomsg messages to", EditorHint.Topic)
        .Define(NanomsgConnectorConfig.Endpoints, ConfigType.List, Importance.High,
            "nanomsg endpoints (e.g., tcp://localhost:5555, ipc:///tmp/test.ipc)")
        .Define(NanomsgConnectorConfig.SocketType, ConfigType.String,
            NanomsgConnectorConfig.DefaultSocketType, Importance.High,
            "Socket type: SUB, PULL, REP, RESPONDENT, BUS, PAIR", EditorHint.Select, options: ["SUB", "PULL", "REP", "RESPONDENT", "BUS", "PAIR"])
        .Define(NanomsgConnectorConfig.BindMode, ConfigType.Boolean, "false", Importance.Medium,
            "Bind to endpoint (true) or connect (false)")
        .Define(NanomsgConnectorConfig.SubscribeTopic, ConfigType.String, "", Importance.Medium,
            "Topic to subscribe to (SUB socket only, empty = all)")
        .Define(NanomsgConnectorConfig.ReceiveBufferSize, ConfigType.Int,
            NanomsgConnectorConfig.DefaultReceiveBufferSize.ToString(), Importance.Low,
            "Receive buffer size in bytes")
        .Define(NanomsgConnectorConfig.ReconnectIntervalMs, ConfigType.Int,
            NanomsgConnectorConfig.DefaultReconnectIntervalMs.ToString(), Importance.Low,
            "Reconnect interval in milliseconds")
        .Define(NanomsgConnectorConfig.ReceiveTimeoutMs, ConfigType.Int,
            NanomsgConnectorConfig.DefaultReceiveTimeoutMs.ToString(), Importance.Low,
            "Receive timeout in milliseconds");

    public override Type TaskClass => typeof(NanomsgSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(NanomsgConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{NanomsgConnectorConfig.Topic}' is required");
        }

        if (!config.TryGetValue(NanomsgConnectorConfig.Endpoints, out var endpoints) ||
            string.IsNullOrWhiteSpace(endpoints))
        {
            throw new ArgumentException($"'{NanomsgConnectorConfig.Endpoints}' is required");
        }

        var socketType = (config.TryGetValue(NanomsgConnectorConfig.SocketType, out var sockType)
            ? sockType : NanomsgConnectorConfig.DefaultSocketType).ToUpperInvariant();

        var validSourceTypes = new[] { "SUB", "PULL", "REP", "RESPONDENT", "BUS", "PAIR" };
        if (!validSourceTypes.Contains(socketType))
        {
            throw new ArgumentException($"Source socket type must be one of: {string.Join(", ", validSourceTypes)}");
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
