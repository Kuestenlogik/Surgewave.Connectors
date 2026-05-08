using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Udp;

/// <summary>
/// A source connector that receives UDP datagrams.
/// Supports both unicast and multicast modes.
/// </summary>
public sealed class UdpSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(UdpSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(UdpConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Topic to publish received messages to", EditorHint.Topic)
        .Define(UdpConnectorConfig.ListenAddress, ConfigType.String,
            UdpConnectorConfig.DefaultListenAddress, Importance.Medium,
            "Address to bind UDP listener (0.0.0.0 for all interfaces)")
        .Define(UdpConnectorConfig.ListenPort, ConfigType.Int,
            UdpConnectorConfig.DefaultListenPort, Importance.High,
            "Port to listen for UDP datagrams")
        .Define(UdpConnectorConfig.MulticastEnabled, ConfigType.Boolean,
            UdpConnectorConfig.DefaultMulticastEnabled, Importance.Medium,
            "Enable multicast mode")
        .Define(UdpConnectorConfig.MulticastGroup, ConfigType.String,
            "", Importance.Medium,
            "Multicast group address to join (e.g., 239.0.0.1)")
        .Define(UdpConnectorConfig.MaxMessageSize, ConfigType.Int,
            UdpConnectorConfig.DefaultMaxMessageSize, Importance.Low,
            "Maximum message size in bytes")
        .Define(UdpConnectorConfig.IncludeSourceInfo, ConfigType.Boolean,
            UdpConnectorConfig.DefaultIncludeSourceInfo, Importance.Low,
            "Include source IP and port in message headers")
        .Define(UdpConnectorConfig.ReceiveBufferSize, ConfigType.Int,
            UdpConnectorConfig.DefaultReceiveBufferSize, Importance.Low,
            "Socket receive buffer size");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.ContainsKey(UdpConnectorConfig.Topic))
            throw new ArgumentException($"Missing required config: {UdpConnectorConfig.Topic}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // UDP source runs single task to avoid port conflicts
        return [new Dictionary<string, string>(_config)];
    }
}
