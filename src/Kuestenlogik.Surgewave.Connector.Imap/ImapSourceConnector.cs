using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Imap;

/// <summary>
/// Source connector that reads emails from an IMAP server.
/// </summary>
public class ImapSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(ImapSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        // Topic
        .Define(ImapConnectorConfig.TopicConfig, ConfigType.String, Importance.High,
            "Destination topic for email messages", EditorHint.Topic)

        // IMAP server
        .Define(ImapConnectorConfig.HostConfig, ConfigType.String, Importance.High,
            "IMAP server hostname")
        .Define(ImapConnectorConfig.PortConfig, ConfigType.Int, ImapConnectorConfig.DefaultPort, Importance.Medium,
            "IMAP server port (default: 993 for SSL, 143 for non-SSL)")
        .Define(ImapConnectorConfig.UsernameConfig, ConfigType.String, Importance.High,
            "IMAP authentication username")
        .Define(ImapConnectorConfig.PasswordConfig, ConfigType.Password, Importance.High,
            "IMAP authentication password")
        .Define(ImapConnectorConfig.UseSslConfig, ConfigType.Boolean, ImapConnectorConfig.DefaultUseSsl, Importance.Medium,
            "Use SSL/TLS connection")
        .Define(ImapConnectorConfig.TimeoutSecondsConfig, ConfigType.Int, ImapConnectorConfig.DefaultTimeoutSeconds, Importance.Low,
            "Connection and operation timeout in seconds")
        .Define(ImapConnectorConfig.AcceptInvalidCertificatesConfig, ConfigType.Boolean, false, Importance.Low,
            "Accept invalid SSL certificates (not recommended for production)")

        // Folder settings
        .Define(ImapConnectorConfig.FolderConfig, ConfigType.String, ImapConnectorConfig.DefaultFolder, Importance.Medium,
            "IMAP folder to monitor (default: INBOX)")
        .Define(ImapConnectorConfig.FoldersConfig, ConfigType.String, Importance.Low,
            "Comma-separated list of folders to monitor")
        .Define(ImapConnectorConfig.RecursiveConfig, ConfigType.Boolean, false, Importance.Low,
            "Recursively monitor subfolders")

        // Polling settings
        .Define(ImapConnectorConfig.PollIntervalMsConfig, ConfigType.Int, ImapConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Poll interval in milliseconds")
        .Define(ImapConnectorConfig.UseIdleConfig, ConfigType.Boolean, ImapConnectorConfig.DefaultUseIdle, Importance.Medium,
            "Use IMAP IDLE for push notifications (if supported)")
        .Define(ImapConnectorConfig.IdleTimeoutMinutesConfig, ConfigType.Int, ImapConnectorConfig.DefaultIdleTimeoutMinutes, Importance.Low,
            "IDLE timeout in minutes before reconnecting")
        .Define(ImapConnectorConfig.BatchSizeConfig, ConfigType.Int, ImapConnectorConfig.DefaultBatchSize, Importance.Low,
            "Maximum messages to fetch per poll")

        // Message handling
        .Define(ImapConnectorConfig.MarkAsReadConfig, ConfigType.Boolean, ImapConnectorConfig.DefaultMarkAsRead, Importance.Medium,
            "Mark messages as read after processing")
        .Define(ImapConnectorConfig.DeleteAfterReadConfig, ConfigType.Boolean, ImapConnectorConfig.DefaultDeleteAfterRead, Importance.Medium,
            "Delete messages after processing")
        .Define(ImapConnectorConfig.MoveAfterReadConfig, ConfigType.Boolean, false, Importance.Medium,
            "Move messages to another folder after processing")
        .Define(ImapConnectorConfig.MoveToFolderConfig, ConfigType.String, Importance.Low,
            "Destination folder for processed messages")
        .Define(ImapConnectorConfig.StartFromConfig, ConfigType.String, ImapConnectorConfig.DefaultStartFrom, Importance.Medium,
            "Where to start: 'latest' (new messages) or 'earliest' (all messages)")

        // Message filtering
        .Define(ImapConnectorConfig.UnseenOnlyConfig, ConfigType.Boolean, ImapConnectorConfig.DefaultUnseenOnly, Importance.Medium,
            "Only fetch unseen (unread) messages")
        .Define(ImapConnectorConfig.SinceConfig, ConfigType.String, Importance.Low,
            "Only fetch messages since date (ISO 8601 format)")
        .Define(ImapConnectorConfig.SubjectFilterConfig, ConfigType.String, Importance.Low,
            "Filter messages by subject (contains match)")
        .Define(ImapConnectorConfig.FromFilterConfig, ConfigType.String, Importance.Low,
            "Filter messages by sender (contains match)")

        // Output settings
        .Define(ImapConnectorConfig.IncludeBodyConfig, ConfigType.Boolean, ImapConnectorConfig.DefaultIncludeBody, Importance.Medium,
            "Include message body in output")
        .Define(ImapConnectorConfig.IncludeAttachmentsConfig, ConfigType.Boolean, ImapConnectorConfig.DefaultIncludeAttachments, Importance.Medium,
            "Include attachments in output (base64 encoded)")
        .Define(ImapConnectorConfig.MaxAttachmentSizeBytesConfig, ConfigType.Long, ImapConnectorConfig.DefaultMaxAttachmentSizeBytes, Importance.Low,
            "Maximum attachment size to include (bytes)")
        .Define(ImapConnectorConfig.PreferHtmlConfig, ConfigType.Boolean, ImapConnectorConfig.DefaultPreferHtml, Importance.Low,
            "Prefer HTML body over plain text when available");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        // Validate required config
        if (!config.TryGetValue(ImapConnectorConfig.TopicConfig, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"Missing required configuration: {ImapConnectorConfig.TopicConfig}");
        }

        if (!config.TryGetValue(ImapConnectorConfig.HostConfig, out var host) ||
            string.IsNullOrWhiteSpace(host))
        {
            throw new ArgumentException($"Missing required configuration: {ImapConnectorConfig.HostConfig}");
        }

        if (!config.TryGetValue(ImapConnectorConfig.UsernameConfig, out var username) ||
            string.IsNullOrWhiteSpace(username))
        {
            throw new ArgumentException($"Missing required configuration: {ImapConnectorConfig.UsernameConfig}");
        }

        // Validate move configuration
        if (config.TryGetValue(ImapConnectorConfig.MoveAfterReadConfig, out var move) &&
            bool.Parse(move) &&
            (!config.TryGetValue(ImapConnectorConfig.MoveToFolderConfig, out var moveFolder) ||
             string.IsNullOrWhiteSpace(moveFolder)))
        {
            throw new ArgumentException($"When {ImapConnectorConfig.MoveAfterReadConfig} is true, " +
                                       $"{ImapConnectorConfig.MoveToFolderConfig} must be specified");
        }
    }

    public override void Stop()
    {
        // No cleanup needed
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // IMAP connector uses a single task per connection
        return [new Dictionary<string, string>(_config)];
    }
}
