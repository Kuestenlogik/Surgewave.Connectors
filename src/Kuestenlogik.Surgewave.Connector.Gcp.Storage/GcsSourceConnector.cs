namespace Kuestenlogik.Surgewave.Connector.Gcp.Storage;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A source connector that reads objects from Google Cloud Storage.
/// Supports listing and reading objects with various formats.
/// </summary>
public sealed class GcsSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(GcsSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(GcsConnectorConfig.ProjectIdConfig, ConfigType.String, "", Importance.High,
            "Google Cloud project ID (uses default if empty)")
        .Define(GcsConnectorConfig.BucketNameConfig, ConfigType.String, Importance.High,
            "GCS bucket name")
        .Define(GcsConnectorConfig.TopicConfig, ConfigType.String, Importance.High,
            "Destination Surgewave topic", EditorHint.Topic)
        .Define(GcsConnectorConfig.CredentialsJsonConfig, ConfigType.Password, "", Importance.Medium,
            "Service account JSON credentials (inline)")
        .Define(GcsConnectorConfig.CredentialsFileConfig, ConfigType.String, "", Importance.Medium,
            "Path to service account JSON credentials file", EditorHint.FilePath)
        .Define(GcsConnectorConfig.PrefixConfig, ConfigType.String, "", Importance.Medium,
            "Object prefix filter")
        .Define(GcsConnectorConfig.FormatConfig, ConfigType.String, GcsConnectorConfig.DefaultFormat, Importance.Medium,
            "Object format: json, jsonlines, csv, raw")
        .Define(GcsConnectorConfig.PollIntervalMsConfig, ConfigType.Long, GcsConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Poll interval in milliseconds")
        .Define(GcsConnectorConfig.DeleteAfterReadConfig, ConfigType.Boolean, GcsConnectorConfig.DefaultDeleteAfterRead, Importance.Medium,
            "Delete objects after reading");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(GcsConnectorConfig.BucketNameConfig, out var bucket) ||
            string.IsNullOrWhiteSpace(bucket))
        {
            throw new ArgumentException($"Missing required config: {GcsConnectorConfig.BucketNameConfig}");
        }

        if (!config.TryGetValue(GcsConnectorConfig.TopicConfig, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"Missing required config: {GcsConnectorConfig.TopicConfig}");
        }

        // Validate format
        var format = GetConfigValue(config, GcsConnectorConfig.FormatConfig, GcsConnectorConfig.DefaultFormat);
        if (format is not (GcsConnectorConfig.FormatJson or GcsConnectorConfig.FormatJsonLines or
            GcsConnectorConfig.FormatCsv or GcsConnectorConfig.FormatRaw))
        {
            throw new ArgumentException(
                $"Invalid format '{format}'. Must be '{GcsConnectorConfig.FormatJson}', " +
                $"'{GcsConnectorConfig.FormatJsonLines}', '{GcsConnectorConfig.FormatCsv}', or '{GcsConnectorConfig.FormatRaw}'.");
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
        // Single task for simplicity. Could partition by prefix for parallelism.
        return [new Dictionary<string, string>(_config)];
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;
}
