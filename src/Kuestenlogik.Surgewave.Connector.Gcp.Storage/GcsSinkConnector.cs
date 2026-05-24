namespace Kuestenlogik.Surgewave.Connector.Gcp.Storage;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that writes records to Google Cloud Storage.
/// Supports various formats and partitioning strategies.
/// </summary>
public sealed class GcsSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(GcsSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(GcsConnectorConfig.ProjectIdConfig, ConfigType.String, "", Importance.High,
            "Google Cloud project ID (uses default if empty)")
        .Define(GcsConnectorConfig.BucketNameConfig, ConfigType.String, Importance.High,
            "GCS bucket name")
        .Define(GcsConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Surgewave topics to consume from", EditorHint.Topic)
        .Define(GcsConnectorConfig.CredentialsJsonConfig, ConfigType.Password, "", Importance.Medium,
            "Service account JSON credentials (inline)")
        .Define(GcsConnectorConfig.CredentialsFileConfig, ConfigType.String, "", Importance.Medium,
            "Path to service account JSON credentials file", EditorHint.FilePath)
        .Define(GcsConnectorConfig.PrefixConfig, ConfigType.String, "", Importance.Medium,
            "Object prefix for uploaded files")
        .Define(GcsConnectorConfig.FormatConfig, ConfigType.String, GcsConnectorConfig.DefaultFormat, Importance.Medium,
            "Output format: json, jsonlines")
        .Define(GcsConnectorConfig.PartitionerConfig, ConfigType.String, GcsConnectorConfig.DefaultPartitioner, Importance.Medium,
            "Partitioner: default, time, field")
        .Define(GcsConnectorConfig.FlushSizeConfig, ConfigType.Int, (long)GcsConnectorConfig.DefaultFlushSize, Importance.Medium,
            "Number of records before flushing to GCS")
        .Define(GcsConnectorConfig.RotateIntervalMsConfig, ConfigType.Long, GcsConnectorConfig.DefaultRotateIntervalMs, Importance.Medium,
            "Maximum time before rotating file (ms)");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(GcsConnectorConfig.BucketNameConfig, out var bucket) ||
            string.IsNullOrWhiteSpace(bucket))
        {
            throw new ArgumentException($"Missing required config: {GcsConnectorConfig.BucketNameConfig}");
        }

        if (!config.TryGetValue(GcsConnectorConfig.TopicsConfig, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"Missing required config: {GcsConnectorConfig.TopicsConfig}");
        }

        // Validate format
        var format = GetConfigValue(config, GcsConnectorConfig.FormatConfig, GcsConnectorConfig.DefaultFormat);
        if (format is not (GcsConnectorConfig.FormatJson or GcsConnectorConfig.FormatJsonLines))
        {
            throw new ArgumentException(
                $"Invalid format '{format}'. Must be '{GcsConnectorConfig.FormatJson}' or '{GcsConnectorConfig.FormatJsonLines}'.");
        }

        // Validate partitioner
        var partitioner = GetConfigValue(config, GcsConnectorConfig.PartitionerConfig, GcsConnectorConfig.DefaultPartitioner);
        if (partitioner is not (GcsConnectorConfig.PartitionerDefault or
            GcsConnectorConfig.PartitionerTime or GcsConnectorConfig.PartitionerField))
        {
            throw new ArgumentException(
                $"Invalid partitioner '{partitioner}'. Must be '{GcsConnectorConfig.PartitionerDefault}', " +
                $"'{GcsConnectorConfig.PartitionerTime}', or '{GcsConnectorConfig.PartitionerField}'.");
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
        // Each task gets a copy of the full config
        var configs = new List<IDictionary<string, string>>();

        for (var i = 0; i < maxTasks; i++)
        {
            configs.Add(new Dictionary<string, string>(_config)
            {
                ["task.id"] = i.ToString()
            });
        }

        return configs;
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;
}
