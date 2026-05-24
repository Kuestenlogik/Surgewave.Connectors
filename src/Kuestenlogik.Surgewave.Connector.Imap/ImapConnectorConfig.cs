namespace Kuestenlogik.Surgewave.Connector.Imap;

/// <summary>
/// Configuration constants for the IMAP connector.
/// </summary>
public static class ImapConnectorConfig
{
    // Common config
    public const string TopicConfig = "topic";

    // IMAP server settings
    public const string HostConfig = "imap.host";
    public const string PortConfig = "imap.port";
    public const string UsernameConfig = "imap.username";
    public const string PasswordConfig = "imap.password";
    public const string UseSslConfig = "imap.use.ssl";
    public const string TimeoutSecondsConfig = "imap.timeout.seconds";
    public const string AcceptInvalidCertificatesConfig = "imap.accept.invalid.certificates";

    // Folder settings
    public const string FolderConfig = "imap.folder";
    public const string FoldersConfig = "imap.folders";
    public const string RecursiveConfig = "imap.recursive";

    // Polling settings
    public const string PollIntervalMsConfig = "imap.poll.interval.ms";
    public const string UseIdleConfig = "imap.use.idle";
    public const string IdleTimeoutMinutesConfig = "imap.idle.timeout.minutes";
    public const string BatchSizeConfig = "imap.batch.size";

    // Message handling
    public const string MarkAsReadConfig = "imap.mark.as.read";
    public const string DeleteAfterReadConfig = "imap.delete.after.read";
    public const string MoveAfterReadConfig = "imap.move.after.read";
    public const string MoveToFolderConfig = "imap.move.to.folder";
    public const string StartFromConfig = "imap.start.from";

    // Message filtering
    public const string UnseenOnlyConfig = "imap.unseen.only";
    public const string SinceConfig = "imap.since";
    public const string SubjectFilterConfig = "imap.subject.filter";
    public const string FromFilterConfig = "imap.from.filter";

    // Output settings
    public const string IncludeBodyConfig = "imap.include.body";
    public const string IncludeAttachmentsConfig = "imap.include.attachments";
    public const string MaxAttachmentSizeBytesConfig = "imap.max.attachment.size.bytes";
    public const string PreferHtmlConfig = "imap.prefer.html";

    // Default values
    public const int DefaultPort = 993;
    public const int DefaultPortNoSsl = 143;
    public const int DefaultTimeoutSeconds = 30;
    public const bool DefaultUseSsl = true;
    public const string DefaultFolder = "INBOX";
    public const int DefaultPollIntervalMs = 30000;
    public const bool DefaultUseIdle = true;
    public const int DefaultIdleTimeoutMinutes = 29;
    public const int DefaultBatchSize = 100;
    public const bool DefaultMarkAsRead = false;
    public const bool DefaultDeleteAfterRead = false;
    public const bool DefaultUnseenOnly = true;
    public const bool DefaultIncludeBody = true;
    public const bool DefaultIncludeAttachments = false;
    public const long DefaultMaxAttachmentSizeBytes = 10 * 1024 * 1024; // 10MB
    public const bool DefaultPreferHtml = false;
    public const string DefaultStartFrom = "latest";

    // Start from values
    public const string StartFromLatest = "latest";
    public const string StartFromEarliest = "earliest";

    // Offset keys
    public const string OffsetUid = "uid";
    public const string OffsetFolder = "folder";
}
