using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sftp;

/// <summary>
/// Sink connector for SFTP servers.
/// Uploads files to a remote SFTP server.
/// </summary>
public sealed class SftpSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(SftpSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        .Define(SftpConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
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
        .Define(SftpConnectorConfig.OutputPathConfig, ConfigType.String, "/", Importance.High,
            "Remote output path template (supports ${topic}, ${key}, ${timestamp})", EditorHint.FilePath)
        .Define(SftpConnectorConfig.OutputModeConfig, ConfigType.String, SftpConnectorConfig.DefaultOutputMode, Importance.Medium,
            "Output mode: 'file' (one file per record) or 'append' (append to existing)", EditorHint.Select, options: ["file", "append"])
        .Define(SftpConnectorConfig.FileNameFieldConfig, ConfigType.String, "", Importance.Medium,
            "JSON field containing the file name")
        .Define(SftpConnectorConfig.ContentFieldConfig, ConfigType.String, "", Importance.Medium,
            "JSON field containing the file content (base64 or text)")
        .Define(SftpConnectorConfig.CreateDirectoriesConfig, ConfigType.Boolean, true, Importance.Low,
            "Create parent directories if they don't exist")
        .Define(SftpConnectorConfig.OverwriteConfig, ConfigType.Boolean, true, Importance.Low,
            "Overwrite existing files")
        .Define(SftpConnectorConfig.TempSuffixConfig, ConfigType.String, SftpConnectorConfig.DefaultTempSuffix, Importance.Low,
            "Temporary file suffix during upload (empty to disable)")
        .Define(SftpConnectorConfig.FlushIntervalMsConfig, ConfigType.Int, SftpConnectorConfig.DefaultFlushIntervalMs, Importance.Low,
            "Flush interval in milliseconds for append mode");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(SftpConnectorConfig.TopicsConfig, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"Missing required config: {SftpConnectorConfig.TopicsConfig}");
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
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
