using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.ZeroMQ;

/// <summary>
/// Source connector that receives messages from ZeroMQ sockets.
/// </summary>
[ConnectorMetadata(
    Name = "zeromq-source",
    Description = "Receives messages from ZeroMQ sockets (SUB, PULL, REP patterns)",
    Author = "Surgewave",
    Tags = "zeromq, zmq, messaging, brokerless, source")]
public sealed class ZeroMQSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(ZeroMQConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce ZeroMQ messages to", EditorHint.Topic)
        .Define(ZeroMQConnectorConfig.Endpoints, ConfigType.List, Importance.High,
            "ZeroMQ endpoints to connect/bind (e.g., tcp://localhost:5555)")
        .Define(ZeroMQConnectorConfig.SocketType, ConfigType.String,
            ZeroMQConnectorConfig.DefaultSocketType, Importance.High,
            "Socket type: SUB, PULL, REP", EditorHint.Select, options: ["SUB", "PULL", "REP", "DEALER", "PAIR"])
        .Define(ZeroMQConnectorConfig.BindMode, ConfigType.Boolean, "false", Importance.Medium,
            "Bind to endpoint (true) or connect (false)")
        .Define(ZeroMQConnectorConfig.SubscribeTopics, ConfigType.List, "", Importance.Medium,
            "Topics to subscribe to (SUB socket only, empty = all)")
        .Define(ZeroMQConnectorConfig.HighWaterMark, ConfigType.Int,
            ZeroMQConnectorConfig.DefaultHighWaterMark.ToString(), Importance.Low,
            "High water mark for socket buffer")
        .Define(ZeroMQConnectorConfig.LingerMs, ConfigType.Int,
            ZeroMQConnectorConfig.DefaultLingerMs.ToString(), Importance.Low,
            "Linger time in milliseconds on close")
        .Define(ZeroMQConnectorConfig.ReceiveTimeoutMs, ConfigType.Int,
            ZeroMQConnectorConfig.DefaultReceiveTimeoutMs.ToString(), Importance.Low,
            "Receive timeout in milliseconds")
        .Define(ZeroMQConnectorConfig.MessageFormat, ConfigType.String,
            ZeroMQConnectorConfig.DefaultMessageFormat, Importance.Medium,
            "Message format: raw, string, json, multipart");

    public override Type TaskClass => typeof(ZeroMQSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(ZeroMQConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{ZeroMQConnectorConfig.Topic}' is required");
        }

        if (!config.TryGetValue(ZeroMQConnectorConfig.Endpoints, out var endpoints) ||
            string.IsNullOrWhiteSpace(endpoints))
        {
            throw new ArgumentException($"'{ZeroMQConnectorConfig.Endpoints}' is required");
        }

        var socketType = config.GetValueOrDefault(ZeroMQConnectorConfig.SocketType,
            ZeroMQConnectorConfig.DefaultSocketType)!.ToUpperInvariant();

        if (socketType != "SUB" && socketType != "PULL" && socketType != "REP")
        {
            throw new ArgumentException($"Source socket type must be SUB, PULL, or REP");
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
