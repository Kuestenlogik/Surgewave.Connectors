namespace Kuestenlogik.Surgewave.Connector.Nanomsg;

/// <summary>
/// Configuration constants for nanomsg connector.
/// </summary>
public static class NanomsgConnectorConfig
{
    // Connection settings
    public const string Endpoints = "nanomsg.endpoints";  // Comma-separated endpoints
    public const string SocketType = "nanomsg.socket.type";  // SUB, PULL, REP, RESPONDENT, SURVEYOR, BUS, PUB, PUSH, REQ, PAIR
    public const string BindMode = "nanomsg.bind";  // true = bind, false = connect
    public const string SendBufferSize = "nanomsg.sndbuf";
    public const string ReceiveBufferSize = "nanomsg.rcvbuf";
    public const string ReconnectIntervalMs = "nanomsg.reconnect.interval.ms";
    public const string ReconnectIntervalMaxMs = "nanomsg.reconnect.interval.max.ms";

    // Source settings
    public const string Topic = "topic";
    public const string SubscribeTopic = "nanomsg.subscribe.topic";  // For SUB socket
    public const string ReceiveTimeoutMs = "nanomsg.receive.timeout.ms";

    // Sink settings
    public const string Topics = "topics";
    public const string SendTimeoutMs = "nanomsg.send.timeout.ms";

    // Survey settings (for SURVEYOR pattern)
    public const string SurveyDeadlineMs = "nanomsg.survey.deadline.ms";

    // Defaults
    public const string DefaultSocketType = "SUB";
    public const int DefaultSendBufferSize = 128 * 1024;
    public const int DefaultReceiveBufferSize = 128 * 1024;
    public const int DefaultReconnectIntervalMs = 100;
    public const int DefaultReconnectIntervalMaxMs = 0;
    public const int DefaultReceiveTimeoutMs = 1000;
    public const int DefaultSendTimeoutMs = 5000;
    public const int DefaultSurveyDeadlineMs = 1000;
}
