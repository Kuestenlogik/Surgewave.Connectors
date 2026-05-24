namespace Kuestenlogik.Surgewave.Connector.SocketStream;

/// <summary>
/// Configuration constants for the Socket Stream connector.
/// </summary>
public static class SocketStreamConnectorConfig
{
    // Connection settings
    public const string Host = "socket.stream.host";
    public const string Port = "socket.stream.port";
    public const string SocketType = "socket.stream.type";
    public const string UnixSocketPath = "socket.stream.unix.path";
    public const string Topic = "socket.stream.topic";

    // Connection management
    public const string ConnectTimeoutMs = "socket.stream.connect.timeout.ms";
    public const string ReadTimeoutMs = "socket.stream.read.timeout.ms";
    public const string ReconnectEnabled = "socket.stream.reconnect.enabled";
    public const string ReconnectDelayMs = "socket.stream.reconnect.delay.ms";
    public const string ReconnectMaxDelayMs = "socket.stream.reconnect.max.delay.ms";
    public const string ReconnectMaxAttempts = "socket.stream.reconnect.max.attempts";

    // Framing
    public const string Framing = "socket.stream.framing";
    public const string Delimiter = "socket.stream.delimiter";
    public const string LengthPrefixBytes = "socket.stream.length.prefix.bytes";
    public const string LengthPrefixBigEndian = "socket.stream.length.prefix.big.endian";
    public const string MaxMessageSize = "socket.stream.max.message.size";

    // Buffer settings
    public const string ReceiveBufferSize = "socket.stream.receive.buffer.size";
    public const string SendBufferSize = "socket.stream.send.buffer.size";

    // TLS settings
    public const string TlsEnabled = "socket.stream.tls.enabled";
    public const string TlsServerName = "socket.stream.tls.server.name";
    public const string TlsValidateCertificate = "socket.stream.tls.validate.certificate";
    public const string TlsClientCertPath = "socket.stream.tls.client.cert.path";
    public const string TlsClientCertPassword = "socket.stream.tls.client.cert.password";

    // Socket types
    public const string SocketTypeTcp = "tcp";
    public const string SocketTypeUnix = "unix";

    // Framing modes
    public const string FramingLine = "line";
    public const string FramingDelimiter = "delimiter";
    public const string FramingLengthPrefix = "length-prefix";
    public const string FramingRaw = "raw";

    // Defaults
    public const string DefaultHost = "localhost";
    public const int DefaultPort = 9999;
    public const string DefaultSocketType = SocketTypeTcp;
    public const string DefaultFraming = FramingLine;
    public const string DefaultDelimiter = "\n";
    public const int DefaultLengthPrefixBytes = 4;
    public const bool DefaultLengthPrefixBigEndian = true;
    public const int DefaultMaxMessageSize = 1024 * 1024; // 1MB
    public const int DefaultConnectTimeoutMs = 30000;
    public const int DefaultReadTimeoutMs = 0; // No timeout
    public const bool DefaultReconnectEnabled = true;
    public const int DefaultReconnectDelayMs = 1000;
    public const int DefaultReconnectMaxDelayMs = 30000;
    public const int DefaultReconnectMaxAttempts = -1; // Unlimited
    public const int DefaultReceiveBufferSize = 65536;
    public const int DefaultSendBufferSize = 65536;
}
