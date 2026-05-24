namespace Kuestenlogik.Surgewave.Connector.ZeroMQ;

/// <summary>
/// Configuration constants for ZeroMQ connector.
/// </summary>
public static class ZeroMQConnectorConfig
{
    // Connection settings
    public const string Endpoints = "zeromq.endpoints";  // Comma-separated endpoints
    public const string SocketType = "zeromq.socket.type";  // SUB, PULL, REP, PUB, PUSH, REQ
    public const string BindMode = "zeromq.bind";  // true = bind, false = connect
    public const string HighWaterMark = "zeromq.hwm";
    public const string LingerMs = "zeromq.linger.ms";

    // Source settings
    public const string Topic = "topic";
    public const string SubscribeTopics = "zeromq.subscribe.topics";  // For SUB socket, comma-separated
    public const string ReceiveTimeoutMs = "zeromq.receive.timeout.ms";
    public const string MessageFormat = "zeromq.message.format";  // raw, string, json, multipart

    // Sink settings
    public const string Topics = "topics";
    public const string PublishTopic = "zeromq.publish.topic";  // For PUB socket prefix
    public const string SendTimeoutMs = "zeromq.send.timeout.ms";

    // Defaults
    public const string DefaultSocketType = "SUB";
    public const int DefaultHighWaterMark = 1000;
    public const int DefaultLingerMs = 0;
    public const int DefaultReceiveTimeoutMs = 1000;
    public const int DefaultSendTimeoutMs = 5000;
    public const string DefaultMessageFormat = "raw";
}
