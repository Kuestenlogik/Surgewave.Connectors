using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sftp;

/// <summary>
/// Source connector for SFTP servers.
/// Polls remote directories for files and emits their contents as records.
/// </summary>
public sealed class SftpSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(SftpSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        .Define(SftpConnectorConfig.TopicConfig, ConfigType.String, Importance.High,
            "Destination topic for file contents", EditorHint.Topic)
        .Define(SftpConnectorConfig.HostConfig, ConfigType.String, Importance.High,
            "SFTP server hostname or IP address")
        .Define(SftpConnectorConfig.PortConfig, ConfigType.Int, SftpConnectorConfig.DefaultPort, Importance.Medium,
            "SFTP server port")
        .Define(SftpConnectorConfig.UsernameConfig, ConfigType.String, Importance.High,
            "Username for authentication")
        .Define(SftpConnectorConfig.PasswordConfig, ConfigType.Password, "", Importance.Medium,
            "Password for authentication")
        .Define(SftpConnectorConfig.PrivateKeyPathConfig, ConfigType.String, "", Importance.Medium,
            "Path to SSH private key file", EditorHint.FilePath)
        .Define(SftpConnectorConfig.PrivateKeyPassphraseConfig, ConfigType.Password, "", Importance.Low,
            "Passphrase for SSH private key")
        .Define(SftpConnectorConfig.PrivateKeyContentConfig, ConfigType.Password, "", Importance.Low,
            "SSH private key content (alternative to file path)")
        .Define(SftpConnectorConfig.HostKeyFingerprintConfig, ConfigType.String, "", Importance.Low,
            "Expected host key fingerprint for verification")
        .Define(SftpConnectorConfig.TimeoutSecondsConfig, ConfigType.Int, SftpConnectorConfig.DefaultTimeoutSeconds, Importance.Low,
            "Connection timeout in seconds")
        .Define(SftpConnectorConfig.RemotePathConfig, ConfigType.String, "/", Importance.High,
            "Remote directory path to poll", EditorHint.FilePath)
        .Define(SftpConnectorConfig.FilePatternConfig, ConfigType.String, SftpConnectorConfig.DefaultFilePattern, Importance.Medium,
            "Glob pattern to filter files (e.g., '*.csv', '*.json')")
        .Define(SftpConnectorConfig.RecursiveConfig, ConfigType.Boolean, false, Importance.Low,
            "Recursively scan subdirectories")
        .Define(SftpConnectorConfig.PollIntervalMsConfig, ConfigType.Int, SftpConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Poll interval in milliseconds")
        .Define(SftpConnectorConfig.DeleteAfterReadConfig, ConfigType.Boolean, false, Importance.Medium,
            "Delete files after reading")
        .Define(SftpConnectorConfig.MoveAfterReadConfig, ConfigType.Boolean, false, Importance.Medium,
            "Move files after reading")
        .Define(SftpConnectorConfig.MoveToPathConfig, ConfigType.String, "", Importance.Low,
            "Destination path for moved files (required if move.after.read is true)", EditorHint.FilePath)
        .Define(SftpConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, true, Importance.Low,
            "Include file metadata in records")
        .Define(SftpConnectorConfig.MaxFileSizeBytesConfig, ConfigType.Long, SftpConnectorConfig.DefaultMaxFileSizeBytes, Importance.Low,
            "Maximum file size to process in bytes")
        .Define(SftpConnectorConfig.MinFileSizeBytesConfig, ConfigType.Long, 0L, Importance.Low,
            "Minimum file size to process in bytes")
        .Define(SftpConnectorConfig.StartFromConfig, ConfigType.String, SftpConnectorConfig.DefaultStartFrom, Importance.Medium,
            "Where to start: 'latest' (new files only) or 'earliest' (all files)", EditorHint.Select, options: ["beginning", "end"]);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(SftpConnectorConfig.TopicConfig, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"Missing required config: {SftpConnectorConfig.TopicConfig}");
        }

        if (!config.TryGetValue(SftpConnectorConfig.HostConfig, out var host) ||
            string.IsNullOrWhiteSpace(host))
        {
            throw new ArgumentException($"Missing required config: {SftpConnectorConfig.HostConfig}");
        }

        if (!config.TryGetValue(SftpConnectorConfig.UsernameConfig, out var username) ||
            string.IsNullOrWhiteSpace(username))
        {
            throw new ArgumentException($"Missing required config: {SftpConnectorConfig.UsernameConfig}");
        }

        // Validate move configuration
        if (config.TryGetValue(SftpConnectorConfig.MoveAfterReadConfig, out var move) &&
            bool.Parse(move))
        {
            if (!config.TryGetValue(SftpConnectorConfig.MoveToPathConfig, out var moveTo) ||
                string.IsNullOrWhiteSpace(moveTo))
            {
                throw new ArgumentException($"Move after read requires {SftpConnectorConfig.MoveToPathConfig}");
            }
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
