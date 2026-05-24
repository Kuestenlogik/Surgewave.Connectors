using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.SignalR;

/// <summary>
/// SignalR Source Connector - Receives messages from a SignalR hub.
/// </summary>
public sealed class SignalRSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(SignalRSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(SignalRConfig.Topic, ConfigType.String, Importance.High,
            "Topic to write records to", EditorHint.Topic)
        .Define(SignalRConfig.HubUrl, ConfigType.String, Importance.High,
            "SignalR hub URL")
        .Define(SignalRConfig.Method, ConfigType.String, "ReceiveMessage", Importance.High,
            "Hub method to subscribe to")
        .Define(SignalRConfig.ReconnectEnabled, ConfigType.Boolean, true, Importance.Medium,
            "Enable automatic reconnection")
        .Define(SignalRConfig.ReconnectDelayMs, ConfigType.Long, 1000L, Importance.Low,
            "Initial reconnect delay in milliseconds")
        .Define(SignalRConfig.ReconnectMaxDelayMs, ConfigType.Long, 30000L, Importance.Low,
            "Maximum reconnect delay in milliseconds")
        .Define(SignalRConfig.AccessToken, ConfigType.Password, "", Importance.Medium,
            "Access token for authentication")
        .Define(SignalRConfig.Headers, ConfigType.String, "", Importance.Low,
            "Custom headers as JSON object", EditorHint.Multiline)
        .Define(SignalRConfig.Transport, ConfigType.String, "All", Importance.Low,
            "Transport type: WebSockets, ServerSentEvents, LongPolling, All", EditorHint.Select, options: ["All", "WebSockets", "ServerSentEvents", "LongPolling"]);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(SignalRConfig.HubUrl, out var hubUrl) || string.IsNullOrEmpty(hubUrl))
        {
            throw new ArgumentException($"Missing required config: {SignalRConfig.HubUrl}");
        }

        if (!config.TryGetValue(SignalRConfig.Topic, out var topic) || string.IsNullOrEmpty(topic))
        {
            throw new ArgumentException($"Missing required config: {SignalRConfig.Topic}");
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        var taskConfig = new Dictionary<string, string>(_config);
        return [taskConfig];
    }
}
