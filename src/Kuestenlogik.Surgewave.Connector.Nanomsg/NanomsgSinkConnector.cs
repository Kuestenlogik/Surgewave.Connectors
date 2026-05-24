using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Nanomsg;

/// <summary>
/// Sink connector that sends messages to nanomsg sockets.
/// </summary>
[ConnectorMetadata(
    Name = "nanomsg-sink",
    Description = "Sends messages to nanomsg/nng sockets (PUB, PUSH, REQ, SURVEYOR, BUS patterns)",
    Author = "Surgewave",
    Tags = "nanomsg, nng, messaging, brokerless, sink")]
public sealed class NanomsgSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(NanomsgConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume messages from", EditorHint.Topic)
        .Define(NanomsgConnectorConfig.Endpoints, ConfigType.List, Importance.High,
            "nanomsg endpoints (e.g., tcp://localhost:5556, ipc:///tmp/test.ipc)")
        .Define(NanomsgConnectorConfig.SocketType, ConfigType.String, "PUB", Importance.High,
            "Socket type: PUB, PUSH, REQ, SURVEYOR, BUS, PAIR", EditorHint.Select, options: ["PUB", "PUSH", "REQ", "SURVEYOR", "BUS", "PAIR"])
        .Define(NanomsgConnectorConfig.BindMode, ConfigType.Boolean, "true", Importance.Medium,
            "Bind to endpoint (true) or connect (false)")
        .Define(NanomsgConnectorConfig.SendBufferSize, ConfigType.Int,
            NanomsgConnectorConfig.DefaultSendBufferSize.ToString(), Importance.Low,
            "Send buffer size in bytes")
        .Define(NanomsgConnectorConfig.ReconnectIntervalMs, ConfigType.Int,
            NanomsgConnectorConfig.DefaultReconnectIntervalMs.ToString(), Importance.Low,
            "Reconnect interval in milliseconds")
        .Define(NanomsgConnectorConfig.SendTimeoutMs, ConfigType.Int,
            NanomsgConnectorConfig.DefaultSendTimeoutMs.ToString(), Importance.Low,
            "Send timeout in milliseconds")
        .Define(NanomsgConnectorConfig.SurveyDeadlineMs, ConfigType.Int,
            NanomsgConnectorConfig.DefaultSurveyDeadlineMs.ToString(), Importance.Low,
            "Survey deadline in milliseconds (SURVEYOR only)");

    public override Type TaskClass => typeof(NanomsgSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(NanomsgConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{NanomsgConnectorConfig.Topics}' is required");
        }

        if (!config.TryGetValue(NanomsgConnectorConfig.Endpoints, out var endpoints) ||
            string.IsNullOrWhiteSpace(endpoints))
        {
            throw new ArgumentException($"'{NanomsgConnectorConfig.Endpoints}' is required");
        }

        var socketType = (config.TryGetValue(NanomsgConnectorConfig.SocketType, out var sockType) ? sockType : "PUB").ToUpperInvariant();

        var validSinkTypes = new[] { "PUB", "PUSH", "REQ", "SURVEYOR", "BUS", "PAIR" };
        if (!validSinkTypes.Contains(socketType))
        {
            throw new ArgumentException($"Sink socket type must be one of: {string.Join(", ", validSinkTypes)}");
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
