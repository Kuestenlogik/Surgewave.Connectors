using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Teams;

/// <summary>
/// Sink connector that sends messages to Microsoft Teams channels or chats.
/// </summary>
public sealed class TeamsSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(TeamsSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(TeamsConnectorConfig.Topics, ConfigType.String, Importance.High, "Source topics to consume", EditorHint.Topic)
        .Define(TeamsConnectorConfig.TenantId, ConfigType.String, Importance.High, "Azure AD tenant ID")
        .Define(TeamsConnectorConfig.ClientId, ConfigType.String, Importance.High, "Azure AD application client ID")
        .Define(TeamsConnectorConfig.ClientSecret, ConfigType.Password, Importance.High, "Azure AD application client secret")
        .Define(TeamsConnectorConfig.TeamId, ConfigType.String, Importance.Medium, "Target Teams team ID (for channel messages)")
        .Define(TeamsConnectorConfig.ChannelId, ConfigType.String, Importance.Medium, "Target Teams channel ID (for channel messages)")
        .Define(TeamsConnectorConfig.ChatId, ConfigType.String, Importance.Medium, "Target Teams chat ID (for direct/group chats)")
        .Define(TeamsConnectorConfig.MessageFormat, ConfigType.String, TeamsConnectorConfig.DefaultMessageFormat, Importance.Low, "Message format: text, html, adaptivecard")
        .Define(TeamsConnectorConfig.DefaultSubject, ConfigType.String, "", Importance.Low, "Default subject for channel messages");

    private string _topics = "";
    private string _tenantId = "";
    private string _clientId = "";
    private string _clientSecret = "";
    private string? _teamId;
    private string? _channelId;
    private string? _chatId;
    private string _messageFormat = TeamsConnectorConfig.DefaultMessageFormat;
    private string _defaultSubject = "";

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(TeamsConnectorConfig.Topics, out _topics!) || string.IsNullOrEmpty(_topics))
            throw new ArgumentException($"Missing required config: {TeamsConnectorConfig.Topics}");

        if (!config.TryGetValue(TeamsConnectorConfig.TenantId, out _tenantId!) || string.IsNullOrEmpty(_tenantId))
            throw new ArgumentException($"Missing required config: {TeamsConnectorConfig.TenantId}");

        if (!config.TryGetValue(TeamsConnectorConfig.ClientId, out _clientId!) || string.IsNullOrEmpty(_clientId))
            throw new ArgumentException($"Missing required config: {TeamsConnectorConfig.ClientId}");

        if (!config.TryGetValue(TeamsConnectorConfig.ClientSecret, out _clientSecret!) || string.IsNullOrEmpty(_clientSecret))
            throw new ArgumentException($"Missing required config: {TeamsConnectorConfig.ClientSecret}");

        config.TryGetValue(TeamsConnectorConfig.TeamId, out _teamId);
        config.TryGetValue(TeamsConnectorConfig.ChannelId, out _channelId);
        config.TryGetValue(TeamsConnectorConfig.ChatId, out _chatId);

        if (string.IsNullOrEmpty(_chatId) && (string.IsNullOrEmpty(_teamId) || string.IsNullOrEmpty(_channelId)))
            throw new ArgumentException($"Either {TeamsConnectorConfig.ChatId} or both {TeamsConnectorConfig.TeamId} and {TeamsConnectorConfig.ChannelId} must be specified");

        if (config.TryGetValue(TeamsConnectorConfig.MessageFormat, out var format))
            _messageFormat = format;

        config.TryGetValue(TeamsConnectorConfig.DefaultSubject, out _defaultSubject!);
        _defaultSubject ??= "";
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        var config = new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topics] = _topics,
            [TeamsConnectorConfig.TenantId] = _tenantId,
            [TeamsConnectorConfig.ClientId] = _clientId,
            [TeamsConnectorConfig.ClientSecret] = _clientSecret,
            [TeamsConnectorConfig.MessageFormat] = _messageFormat,
            [TeamsConnectorConfig.DefaultSubject] = _defaultSubject
        };

        if (!string.IsNullOrEmpty(_teamId))
            config[TeamsConnectorConfig.TeamId] = _teamId;
        if (!string.IsNullOrEmpty(_channelId))
            config[TeamsConnectorConfig.ChannelId] = _channelId;
        if (!string.IsNullOrEmpty(_chatId))
            config[TeamsConnectorConfig.ChatId] = _chatId;

        return [config];
    }
}
