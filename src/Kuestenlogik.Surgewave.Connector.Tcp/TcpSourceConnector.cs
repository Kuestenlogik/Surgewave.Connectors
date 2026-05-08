using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Tcp;

/// <summary>
/// A source connector that listens for TCP connections and produces received data to a topic.
/// Supports TLS encryption and multiple framing modes (line, length-prefix, delimiter).
/// </summary>
public sealed class TcpSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(TcpSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(TcpConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Topic to write records to", EditorHint.Topic)
        .Define(TcpConnectorConfig.ListenAddress, ConfigType.String,
            TcpConnectorConfig.DefaultListenAddress, Importance.Medium,
            "Address to listen on (default: 0.0.0.0)")
        .Define(TcpConnectorConfig.ListenPort, ConfigType.Int,
            TcpConnectorConfig.DefaultListenPort, Importance.High,
            "Port to listen on")
        .Define(TcpConnectorConfig.MaxConnections, ConfigType.Int,
            TcpConnectorConfig.DefaultMaxConnections, Importance.Low,
            "Maximum concurrent connections")
        .Define(TcpConnectorConfig.Framing, ConfigType.String,
            TcpConnectorConfig.DefaultFraming, Importance.Medium,
            "Message framing: 'line', 'length-prefix', or 'delimiter'")
        .Define(TcpConnectorConfig.Delimiter, ConfigType.String,
            TcpConnectorConfig.DefaultDelimiter, Importance.Low,
            "Delimiter for 'delimiter' framing mode")
        .Define(TcpConnectorConfig.LengthPrefixBytes, ConfigType.Int,
            TcpConnectorConfig.DefaultLengthPrefixBytes, Importance.Low,
            "Number of bytes for length prefix (1, 2, or 4)")
        .Define(TcpConnectorConfig.LengthPrefixBigEndian, ConfigType.Boolean,
            TcpConnectorConfig.DefaultLengthPrefixBigEndian, Importance.Low,
            "Use big-endian byte order for length prefix")
        .Define(TcpConnectorConfig.MaxMessageSize, ConfigType.Int,
            TcpConnectorConfig.DefaultMaxMessageSize, Importance.Low,
            "Maximum message size in bytes")
        .Define(TcpConnectorConfig.UseTls, ConfigType.Boolean,
            TcpConnectorConfig.DefaultUseTls, Importance.Medium,
            "Enable TLS encryption")
        .Define(TcpConnectorConfig.TlsCertificatePath, ConfigType.String,
            "", Importance.Low, "Path to TLS certificate (PFX/P12)", EditorHint.FilePath)
        .Define(TcpConnectorConfig.TlsCertificatePassword, ConfigType.String,
            "", Importance.Low, "Password for TLS certificate");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.ContainsKey(TcpConnectorConfig.Topic))
            throw new ArgumentException($"Missing required config: {TcpConnectorConfig.Topic}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // TCP server only supports a single task
        return [new Dictionary<string, string>(_config)];
    }
}
