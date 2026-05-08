namespace Kuestenlogik.Surgewave.Connector.SignalR;

/// <summary>
/// Configuration keys for SignalR connectors.
/// </summary>
public static class SignalRConfig
{
    // Common
    public const string Topic = "topic";
    public const string HubUrl = "hub.url";
    public const string Method = "method";
    public const string AccessToken = "access.token";
    public const string Headers = "headers";
    public const string Transport = "transport";

    // Reconnection
    public const string ReconnectEnabled = "reconnect.enabled";
    public const string ReconnectDelayMs = "reconnect.delay.ms";
    public const string ReconnectMaxDelayMs = "reconnect.max.delay.ms";

    // Sink-specific
    public const string MessageFormat = "message.format";
    public const string TargetGroup = "target.group";
    public const string TargetUser = "target.user";
    public const string BatchEnabled = "batch.enabled";
    public const string BatchSize = "batch.size";

    // Defaults
    public const string DefaultMethod = "ReceiveMessage";
    public const string DefaultSendMethod = "SendMessage";
    public const string DefaultTransport = "All";
    public const string DefaultMessageFormat = "key-value";
    public const long DefaultReconnectDelayMs = 1000;
    public const long DefaultReconnectMaxDelayMs = 30000;
    public const int DefaultBatchSize = 100;
}
