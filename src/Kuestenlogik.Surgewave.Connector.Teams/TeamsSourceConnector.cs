using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Teams;

/// <summary>
/// Source connector that receives messages from Microsoft Teams channels via polling.
/// </summary>
public sealed class TeamsSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(TeamsSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(TeamsConnectorConfig.Topic, ConfigType.String, Importance.High, "Target topic for produced records", EditorHint.Topic)
        .Define(TeamsConnectorConfig.TenantId, ConfigType.String, Importance.High, "Azure AD tenant ID")
        .Define(TeamsConnectorConfig.ClientId, ConfigType.String, Importance.High, "Azure AD application client ID")
        .Define(TeamsConnectorConfig.ClientSecret, ConfigType.Password, Importance.High, "Azure AD application client secret")
        .Define(TeamsConnectorConfig.TeamId, ConfigType.String, Importance.High, "Source Teams team ID")
        .Define(TeamsConnectorConfig.ChannelId, ConfigType.String, Importance.High, "Source Teams channel ID")
        .Define(TeamsConnectorConfig.PollIntervalMs, ConfigType.Int, TeamsConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in milliseconds")
        .Define(TeamsConnectorConfig.IncludeReplies, ConfigType.Boolean, TeamsConnectorConfig.DefaultIncludeReplies, Importance.Low, "Include message replies");

    private string _topic = "";
    private string _tenantId = "";
    private string _clientId = "";
    private string _clientSecret = "";
    private string _teamId = "";
    private string _channelId = "";
    private int _pollIntervalMs = TeamsConnectorConfig.DefaultPollIntervalMs;
    private bool _includeReplies = TeamsConnectorConfig.DefaultIncludeReplies;

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(TeamsConnectorConfig.Topic, out _topic!) || string.IsNullOrEmpty(_topic))
            throw new ArgumentException($"Missing required config: {TeamsConnectorConfig.Topic}");

        if (!config.TryGetValue(TeamsConnectorConfig.TenantId, out _tenantId!) || string.IsNullOrEmpty(_tenantId))
            throw new ArgumentException($"Missing required config: {TeamsConnectorConfig.TenantId}");

        if (!config.TryGetValue(TeamsConnectorConfig.ClientId, out _clientId!) || string.IsNullOrEmpty(_clientId))
            throw new ArgumentException($"Missing required config: {TeamsConnectorConfig.ClientId}");

        if (!config.TryGetValue(TeamsConnectorConfig.ClientSecret, out _clientSecret!) || string.IsNullOrEmpty(_clientSecret))
            throw new ArgumentException($"Missing required config: {TeamsConnectorConfig.ClientSecret}");

        if (!config.TryGetValue(TeamsConnectorConfig.TeamId, out _teamId!) || string.IsNullOrEmpty(_teamId))
            throw new ArgumentException($"Missing required config: {TeamsConnectorConfig.TeamId}");

        if (!config.TryGetValue(TeamsConnectorConfig.ChannelId, out _channelId!) || string.IsNullOrEmpty(_channelId))
            throw new ArgumentException($"Missing required config: {TeamsConnectorConfig.ChannelId}");

        if (config.TryGetValue(TeamsConnectorConfig.PollIntervalMs, out var pollMs))
            _pollIntervalMs = int.Parse(pollMs);

        if (config.TryGetValue(TeamsConnectorConfig.IncludeReplies, out var replies))
            _includeReplies = bool.Parse(replies);
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topic] = _topic,
            [TeamsConnectorConfig.TenantId] = _tenantId,
            [TeamsConnectorConfig.ClientId] = _clientId,
            [TeamsConnectorConfig.ClientSecret] = _clientSecret,
            [TeamsConnectorConfig.TeamId] = _teamId,
            [TeamsConnectorConfig.ChannelId] = _channelId,
            [TeamsConnectorConfig.PollIntervalMs] = _pollIntervalMs.ToString(),
            [TeamsConnectorConfig.IncludeReplies] = _includeReplies.ToString()
        }];
    }
}
