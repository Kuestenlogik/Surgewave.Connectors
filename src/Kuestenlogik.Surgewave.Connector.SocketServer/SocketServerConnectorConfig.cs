namespace Kuestenlogik.Surgewave.Connector.SocketServer;

/// <summary>
/// Configuration constants for the Socket Server connector.
/// </summary>
public static class SocketServerConnectorConfig
{
    // Server settings
    public const string Protocol = "socket.server.protocol";
    public const string ListenAddress = "socket.server.listen.address";
    public const string ListenPort = "socket.server.listen.port";
    public const string UnixSocketPath = "socket.server.unix.path";
    public const string Topic = "socket.server.topic";

    // Connection management
    public const string MaxConnections = "socket.server.max.connections";
    public const string ConnectionTimeoutMs = "socket.server.connection.timeout.ms";
    public const string IdleTimeoutMs = "socket.server.idle.timeout.ms";

    // Framing (TCP/Unix only)
    public const string Framing = "socket.server.framing";
    public const string Delimiter = "socket.server.delimiter";
    public const string LengthPrefixBytes = "socket.server.length.prefix.bytes";
    public const string LengthPrefixBigEndian = "socket.server.length.prefix.big.endian";
    public const string MaxMessageSize = "socket.server.max.message.size";

    // Buffer settings
    public const string ReceiveBufferSize = "socket.server.receive.buffer.size";
    public const string SendBufferSize = "socket.server.send.buffer.size";

    // TLS settings (TCP only)
    public const string TlsEnabled = "socket.server.tls.enabled";
    public const string TlsCertificatePath = "socket.server.tls.certificate.path";
    public const string TlsCertificatePassword = "socket.server.tls.certificate.password";
    public const string TlsRequireClientCert = "socket.server.tls.require.client.cert";

    // UDP settings
    public const string UdpMulticastEnabled = "socket.server.udp.multicast.enabled";
    public const string UdpMulticastGroup = "socket.server.udp.multicast.group";
    public const string UdpIncludeSourceInfo = "socket.server.udp.include.source.info";

    // Metadata
    public const string IncludeClientInfo = "socket.server.include.client.info";

    // Protocols
    public const string ProtocolTcp = "tcp";
    public const string ProtocolUdp = "udp";
    public const string ProtocolUnix = "unix";

    // Framing modes
    public const string FramingLine = "line";
    public const string FramingDelimiter = "delimiter";
    public const string FramingLengthPrefix = "length-prefix";
    public const string FramingRaw = "raw";

    // Defaults
    public const string DefaultProtocol = ProtocolTcp;
    public const string DefaultListenAddress = "0.0.0.0";
    public const int DefaultListenPort = 9999;
    public const string DefaultFraming = FramingLine;
    public const string DefaultDelimiter = "\n";
    public const int DefaultLengthPrefixBytes = 4;
    public const bool DefaultLengthPrefixBigEndian = true;
    public const int DefaultMaxMessageSize = 1024 * 1024; // 1MB
    public const int DefaultMaxConnections = 100;
    public const int DefaultConnectionTimeoutMs = 30000;
    public const int DefaultIdleTimeoutMs = 0; // No idle timeout
    public const int DefaultReceiveBufferSize = 65536;
    public const int DefaultSendBufferSize = 65536;
    public const int DefaultMaxUdpMessageSize = 65507; // UDP max payload
    public const bool DefaultIncludeClientInfo = true;
    public const bool DefaultUdpIncludeSourceInfo = true;
}
