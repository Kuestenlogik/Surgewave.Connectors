namespace Kuestenlogik.Surgewave.Connector.Sftp;

/// <summary>
/// Configuration constants for SFTP connectors.
/// </summary>
public static class SftpConnectorConfig
{
    // Common
    public const string TopicConfig = "topic";
    public const string TopicsConfig = "topics";

    // Connection
    public const string HostConfig = "sftp.host";
    public const string PortConfig = "sftp.port";
    public const string UsernameConfig = "sftp.username";
    public const string PasswordConfig = "sftp.password";
    public const string PrivateKeyPathConfig = "sftp.private.key.path";
    public const string PrivateKeyPassphraseConfig = "sftp.private.key.passphrase";
    public const string PrivateKeyContentConfig = "sftp.private.key.content";
    public const string HostKeyFingerprintConfig = "sftp.host.key.fingerprint";
    public const string TimeoutSecondsConfig = "sftp.timeout.seconds";

    // Source
    public const string RemotePathConfig = "sftp.remote.path";
    public const string FilePatternConfig = "sftp.file.pattern";
    public const string RecursiveConfig = "sftp.recursive";
    public const string PollIntervalMsConfig = "sftp.poll.interval.ms";
    public const string DeleteAfterReadConfig = "sftp.delete.after.read";
    public const string MoveAfterReadConfig = "sftp.move.after.read";
    public const string MoveToPathConfig = "sftp.move.to.path";
    public const string IncludeMetadataConfig = "sftp.include.metadata";
    public const string MaxFileSizeBytesConfig = "sftp.max.file.size.bytes";
    public const string MinFileSizeBytesConfig = "sftp.min.file.size.bytes";
    public const string StartFromConfig = "sftp.start.from";

    // Sink
    public const string OutputPathConfig = "sftp.output.path";
    public const string OutputModeConfig = "sftp.output.mode";
    public const string FileNameFieldConfig = "sftp.file.name.field";
    public const string ContentFieldConfig = "sftp.content.field";
    public const string CreateDirectoriesConfig = "sftp.create.directories";
    public const string OverwriteConfig = "sftp.overwrite";
    public const string TempSuffixConfig = "sftp.temp.suffix";
    public const string FlushIntervalMsConfig = "sftp.flush.interval.ms";

    // Output modes
    public const string OutputModeFile = "file";
    public const string OutputModeAppend = "append";

    // Start from options
    public const string StartFromLatest = "latest";
    public const string StartFromEarliest = "earliest";

    // Defaults
    public const int DefaultPort = 22;
    public const int DefaultTimeoutSeconds = 30;
    public const int DefaultPollIntervalMs = 30000;
    public const int DefaultFlushIntervalMs = 10000;
    public const string DefaultFilePattern = "*";
    public const string DefaultOutputMode = OutputModeFile;
    public const string DefaultStartFrom = StartFromLatest;
    public const string DefaultTempSuffix = ".tmp";
    public const long DefaultMaxFileSizeBytes = 104857600; // 100 MB

    // Offset keys
    public const string OffsetLastModified = "last_modified";
    public const string OffsetLastFileName = "last_file_name";
    public const string OffsetLastPoll = "last_poll";
}
