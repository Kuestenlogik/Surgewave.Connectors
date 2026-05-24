using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Udp;

/// <summary>
/// A sink connector that sends records as UDP datagrams.
/// Supports both unicast and multicast modes.
/// </summary>
public sealed class UdpSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(UdpSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(UdpConnectorConfig.Topics, ConfigType.String, Importance.High,
            "Topics to consume from (comma-separated)", EditorHint.Topic)
        .Define(UdpConnectorConfig.Host, ConfigType.String, Importance.High,
            "Destination host address")
        .Define(UdpConnectorConfig.Port, ConfigType.Int, Importance.High,
            "Destination port")
        .Define(UdpConnectorConfig.MulticastEnabled, ConfigType.Boolean,
            UdpConnectorConfig.DefaultMulticastEnabled, Importance.Medium,
            "Enable multicast mode")
        .Define(UdpConnectorConfig.MulticastTtl, ConfigType.Int,
            UdpConnectorConfig.DefaultMulticastTtl, Importance.Low,
            "Multicast time-to-live (hop count)")
        .Define(UdpConnectorConfig.MulticastLoopback, ConfigType.Boolean,
            UdpConnectorConfig.DefaultMulticastLoopback, Importance.Low,
            "Enable multicast loopback")
        .Define(UdpConnectorConfig.SendBufferSize, ConfigType.Int,
            UdpConnectorConfig.DefaultSendBufferSize, Importance.Low,
            "Socket send buffer size")
        .Define(UdpConnectorConfig.BatchEnabled, ConfigType.Boolean,
            UdpConnectorConfig.DefaultBatchEnabled, Importance.Low,
            "Enable batching (combine multiple records)")
        .Define(UdpConnectorConfig.BatchDelayMs, ConfigType.Int,
            UdpConnectorConfig.DefaultBatchDelayMs, Importance.Low,
            "Delay between batch sends in milliseconds");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.ContainsKey(UdpConnectorConfig.Topics))
            throw new ArgumentException($"Missing required config: {UdpConnectorConfig.Topics}");
        if (!config.ContainsKey(UdpConnectorConfig.Host))
            throw new ArgumentException($"Missing required config: {UdpConnectorConfig.Host}");
        if (!config.ContainsKey(UdpConnectorConfig.Port))
            throw new ArgumentException($"Missing required config: {UdpConnectorConfig.Port}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // UDP sink can run single task (shared socket is simpler)
        return [new Dictionary<string, string>(_config)];
    }
}
