using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.SignalR;

/// <summary>
/// SignalR Sink Connector - Sends messages to a SignalR hub.
/// </summary>
public sealed class SignalRSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(SignalRSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(SignalRConfig.HubUrl, ConfigType.String, Importance.High,
            "SignalR hub URL")
        .Define(SignalRConfig.Method, ConfigType.String, SignalRConfig.DefaultSendMethod, Importance.High,
            "Hub method to invoke")
        .Define(SignalRConfig.MessageFormat, ConfigType.String, SignalRConfig.DefaultMessageFormat, Importance.Medium,
            "Message format: key-value, value-only, json", EditorHint.Select, options: ["json", "text"])
        .Define(SignalRConfig.TargetGroup, ConfigType.String, "", Importance.Medium,
            "Target group name (if hub supports groups)")
        .Define(SignalRConfig.TargetUser, ConfigType.String, "", Importance.Medium,
            "Target user ID (if hub supports users)")
        .Define(SignalRConfig.ReconnectEnabled, ConfigType.Boolean, true, Importance.Medium,
            "Enable automatic reconnection")
        .Define(SignalRConfig.ReconnectDelayMs, ConfigType.Long, SignalRConfig.DefaultReconnectDelayMs, Importance.Low,
            "Initial reconnect delay in milliseconds")
        .Define(SignalRConfig.ReconnectMaxDelayMs, ConfigType.Long, SignalRConfig.DefaultReconnectMaxDelayMs, Importance.Low,
            "Maximum reconnect delay in milliseconds")
        .Define(SignalRConfig.AccessToken, ConfigType.Password, "", Importance.Medium,
            "Access token for authentication")
        .Define(SignalRConfig.Headers, ConfigType.String, "", Importance.Low,
            "Custom headers as JSON object", EditorHint.Multiline)
        .Define(SignalRConfig.Transport, ConfigType.String, SignalRConfig.DefaultTransport, Importance.Low,
            "Transport type: WebSockets, ServerSentEvents, LongPolling, All", EditorHint.Select, options: ["All", "WebSockets", "ServerSentEvents", "LongPolling"])
        .Define(SignalRConfig.BatchEnabled, ConfigType.Boolean, false, Importance.Medium,
            "Enable batch sending")
        .Define(SignalRConfig.BatchSize, ConfigType.Int, (long)SignalRConfig.DefaultBatchSize, Importance.Medium,
            "Batch size for batch mode");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(SignalRConfig.HubUrl, out var hubUrl) || string.IsNullOrEmpty(hubUrl))
        {
            throw new ArgumentException($"Missing required config: {SignalRConfig.HubUrl}");
        }

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
