namespace Kuestenlogik.Surgewave.Connector.Azure.Blob;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that writes records to Azure Blob Storage.
/// Supports various formats and partitioning strategies.
/// </summary>
public sealed class AzureBlobSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(AzureBlobSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(AzureBlobConnectorConfig.ConnectionStringConfig, ConfigType.Password, "", Importance.High,
            "Azure Storage connection string (alternative to account name/key)")
        .Define(AzureBlobConnectorConfig.AccountNameConfig, ConfigType.String, "", Importance.High,
            "Azure Storage account name")
        .Define(AzureBlobConnectorConfig.AccountKeyConfig, ConfigType.Password, "", Importance.High,
            "Azure Storage account key")
        .Define(AzureBlobConnectorConfig.ContainerNameConfig, ConfigType.String, Importance.High,
            "Azure Blob container name")
        .Define(AzureBlobConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Surgewave topics to consume from", EditorHint.Topic)
        .Define(AzureBlobConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low,
            "Custom Azure Storage endpoint (for Azurite emulator)")
        .Define(AzureBlobConnectorConfig.PrefixConfig, ConfigType.String, "", Importance.Medium,
            "Blob prefix for uploaded files")
        .Define(AzureBlobConnectorConfig.FormatConfig, ConfigType.String, AzureBlobConnectorConfig.DefaultFormat, Importance.Medium,
            "Output format: json, jsonlines", EditorHint.Select, options: ["json", "jsonlines"])
        .Define(AzureBlobConnectorConfig.PartitionerConfig, ConfigType.String, AzureBlobConnectorConfig.DefaultPartitioner, Importance.Medium,
            "Partitioner: default, time, field", EditorHint.Select, options: ["default", "time", "field"])
        .Define(AzureBlobConnectorConfig.FlushSizeConfig, ConfigType.Int, (long)AzureBlobConnectorConfig.DefaultFlushSize, Importance.Medium,
            "Number of records before flushing to blob storage")
        .Define(AzureBlobConnectorConfig.RotateIntervalMsConfig, ConfigType.Long, AzureBlobConnectorConfig.DefaultRotateIntervalMs, Importance.Medium,
            "Maximum time before rotating file (ms)");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate connection - either connection string or account name/key
        var connectionString = GetConfigValue(config, AzureBlobConnectorConfig.ConnectionStringConfig, "");
        var accountName = GetConfigValue(config, AzureBlobConnectorConfig.AccountNameConfig, "");
        var accountKey = GetConfigValue(config, AzureBlobConnectorConfig.AccountKeyConfig, "");

        if (string.IsNullOrWhiteSpace(connectionString) &&
            (string.IsNullOrWhiteSpace(accountName) || string.IsNullOrWhiteSpace(accountKey)))
        {
            throw new ArgumentException(
                $"Either {AzureBlobConnectorConfig.ConnectionStringConfig} or both " +
                $"{AzureBlobConnectorConfig.AccountNameConfig} and {AzureBlobConnectorConfig.AccountKeyConfig} must be specified");
        }

        if (!config.TryGetValue(AzureBlobConnectorConfig.ContainerNameConfig, out var container) ||
            string.IsNullOrWhiteSpace(container))
        {
            throw new ArgumentException($"Missing required config: {AzureBlobConnectorConfig.ContainerNameConfig}");
        }

        if (!config.TryGetValue(AzureBlobConnectorConfig.TopicsConfig, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"Missing required config: {AzureBlobConnectorConfig.TopicsConfig}");
        }

        // Validate format
        var format = GetConfigValue(config, AzureBlobConnectorConfig.FormatConfig, AzureBlobConnectorConfig.DefaultFormat);
        if (format is not (AzureBlobConnectorConfig.FormatJson or AzureBlobConnectorConfig.FormatJsonLines))
        {
            throw new ArgumentException(
                $"Invalid format '{format}'. Must be '{AzureBlobConnectorConfig.FormatJson}' or '{AzureBlobConnectorConfig.FormatJsonLines}'.");
        }

        // Validate partitioner
        var partitioner = GetConfigValue(config, AzureBlobConnectorConfig.PartitionerConfig, AzureBlobConnectorConfig.DefaultPartitioner);
        if (partitioner is not (AzureBlobConnectorConfig.PartitionerDefault or
            AzureBlobConnectorConfig.PartitionerTime or AzureBlobConnectorConfig.PartitionerField))
        {
            throw new ArgumentException(
                $"Invalid partitioner '{partitioner}'. Must be '{AzureBlobConnectorConfig.PartitionerDefault}', " +
                $"'{AzureBlobConnectorConfig.PartitionerTime}', or '{AzureBlobConnectorConfig.PartitionerField}'.");
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
