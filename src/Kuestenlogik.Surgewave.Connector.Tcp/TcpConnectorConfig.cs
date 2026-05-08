namespace Kuestenlogik.Surgewave.Connector.Tcp;

/// <summary>
/// Configuration constants for TCP connectors.
/// </summary>
public static class TcpConnectorConfig
{
    // Common
    public const string Host = "host";
    public const string Port = "port";
    public const string Topic = "topic";
    public const string Topics = "topics";

    // TLS
    public const string UseTls = "tls.enabled";
    public const bool DefaultUseTls = false;
    public const string TlsCertificatePath = "tls.certificate.path";
    public const string TlsCertificatePassword = "tls.certificate.password";
    public const string TlsValidateCertificate = "tls.validate.certificate";
    public const bool DefaultTlsValidateCertificate = true;

    // Source connector (server mode)
    public const string ListenAddress = "listen.address";
    public const string DefaultListenAddress = "0.0.0.0";
    public const string ListenPort = "listen.port";
    public const int DefaultListenPort = 9999;
    public const string MaxConnections = "max.connections";
    public const int DefaultMaxConnections = 100;
    public const string ConnectionTimeout = "connection.timeout.ms";
    public const int DefaultConnectionTimeout = 30000;

    // Framing
    public const string Framing = "framing";
    public const string FramingLine = "line";
    public const string FramingLengthPrefix = "length-prefix";
    public const string FramingDelimiter = "delimiter";
    public const string DefaultFraming = FramingLine;
    public const string Delimiter = "delimiter.bytes";
    public const string DefaultDelimiter = "\n";
    public const string LengthPrefixBytes = "length.prefix.bytes";
    public const int DefaultLengthPrefixBytes = 4;
    public const string LengthPrefixBigEndian = "length.prefix.big.endian";
    public const bool DefaultLengthPrefixBigEndian = true;
    public const string MaxMessageSize = "max.message.size";
    public const int DefaultMaxMessageSize = 1048576; // 1MB

    // Sink connector (client mode)
    public const string Reconnect = "reconnect.enabled";
    public const bool DefaultReconnect = true;
    public const string ReconnectDelayMs = "reconnect.delay.ms";
    public const int DefaultReconnectDelayMs = 1000;
    public const string ReconnectMaxDelayMs = "reconnect.max.delay.ms";
    public const int DefaultReconnectMaxDelayMs = 30000;
    public const string SendBufferSize = "send.buffer.size";
    public const int DefaultSendBufferSize = 65536;
    public const string ReceiveBufferSize = "receive.buffer.size";
    public const int DefaultReceiveBufferSize = 65536;

    // Offset tracking
    public const string OffsetConnectionId = "connection_id";
    public const string OffsetBytesReceived = "bytes_received";
}
