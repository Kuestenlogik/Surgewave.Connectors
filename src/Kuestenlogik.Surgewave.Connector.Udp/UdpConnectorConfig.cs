namespace Kuestenlogik.Surgewave.Connector.Udp;

/// <summary>
/// Configuration constants for UDP connectors.
/// </summary>
public static class UdpConnectorConfig
{
    // Topics
    public const string Topic = "topic";
    public const string Topics = "topics";

    // Source configuration (listener)
    public const string ListenAddress = "listen.address";
    public const string DefaultListenAddress = "0.0.0.0";
    public const string ListenPort = "listen.port";
    public const int DefaultListenPort = 9999;

    // Sink configuration (sender)
    public const string Host = "host";
    public const string Port = "port";

    // Multicast configuration
    public const string MulticastEnabled = "multicast.enabled";
    public const bool DefaultMulticastEnabled = false;
    public const string MulticastGroup = "multicast.group";
    public const string MulticastTtl = "multicast.ttl";
    public const int DefaultMulticastTtl = 1;
    public const string MulticastLoopback = "multicast.loopback";
    public const bool DefaultMulticastLoopback = false;

    // Message framing
    public const string MaxMessageSize = "max.message.size";
    public const int DefaultMaxMessageSize = 65507; // Max UDP payload size (65535 - 8 UDP header - 20 IP header)

    // Message handling
    public const string IncludeSourceInfo = "include.source.info";
    public const bool DefaultIncludeSourceInfo = true;

    // Buffer settings
    public const string ReceiveBufferSize = "receive.buffer.size";
    public const int DefaultReceiveBufferSize = 65535;
    public const string SendBufferSize = "send.buffer.size";
    public const int DefaultSendBufferSize = 65535;

    // Batching (for sink)
    public const string BatchEnabled = "batch.enabled";
    public const bool DefaultBatchEnabled = false;
    public const string BatchDelayMs = "batch.delay.ms";
    public const int DefaultBatchDelayMs = 10;
}
