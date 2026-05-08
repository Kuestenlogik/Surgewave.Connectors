using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.ZeroMQ;

/// <summary>
/// Sink connector that sends messages to ZeroMQ sockets.
/// </summary>
[ConnectorMetadata(
    Name = "zeromq-sink",
    Description = "Sends messages to ZeroMQ sockets (PUB, PUSH, REQ patterns)",
    Author = "Surgewave",
    Tags = "zeromq, zmq, messaging, brokerless, sink")]
public sealed class ZeroMQSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(ZeroMQConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume messages from", EditorHint.Topic)
        .Define(ZeroMQConnectorConfig.Endpoints, ConfigType.List, Importance.High,
            "ZeroMQ endpoints to connect/bind (e.g., tcp://localhost:5556)")
        .Define(ZeroMQConnectorConfig.SocketType, ConfigType.String, "PUB", Importance.High,
            "Socket type: PUB, PUSH, REQ", EditorHint.Select, options: ["PUB", "PUSH", "REQ", "DEALER", "PAIR"])
        .Define(ZeroMQConnectorConfig.BindMode, ConfigType.Boolean, "true", Importance.Medium,
            "Bind to endpoint (true) or connect (false)")
        .Define(ZeroMQConnectorConfig.PublishTopic, ConfigType.String, "", Importance.Medium,
            "Topic prefix for PUB socket messages")
        .Define(ZeroMQConnectorConfig.HighWaterMark, ConfigType.Int,
            ZeroMQConnectorConfig.DefaultHighWaterMark.ToString(), Importance.Low,
            "High water mark for socket buffer")
        .Define(ZeroMQConnectorConfig.LingerMs, ConfigType.Int,
            ZeroMQConnectorConfig.DefaultLingerMs.ToString(), Importance.Low,
            "Linger time in milliseconds on close")
        .Define(ZeroMQConnectorConfig.SendTimeoutMs, ConfigType.Int,
            ZeroMQConnectorConfig.DefaultSendTimeoutMs.ToString(), Importance.Low,
            "Send timeout in milliseconds");

    public override Type TaskClass => typeof(ZeroMQSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(ZeroMQConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{ZeroMQConnectorConfig.Topics}' is required");
        }

        if (!config.TryGetValue(ZeroMQConnectorConfig.Endpoints, out var endpoints) ||
            string.IsNullOrWhiteSpace(endpoints))
        {
            throw new ArgumentException($"'{ZeroMQConnectorConfig.Endpoints}' is required");
        }

        var socketType = config.GetValueOrDefault(ZeroMQConnectorConfig.SocketType, "PUB")!.ToUpperInvariant();

        if (socketType != "PUB" && socketType != "PUSH" && socketType != "REQ")
        {
            throw new ArgumentException($"Sink socket type must be PUB, PUSH, or REQ");
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
