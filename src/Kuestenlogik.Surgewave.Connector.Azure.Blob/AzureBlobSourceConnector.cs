namespace Kuestenlogik.Surgewave.Connector.Azure.Blob;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A source connector that reads blobs from Azure Blob Storage.
/// Supports listing and reading blobs with various formats.
/// </summary>
public sealed class AzureBlobSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(AzureBlobSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(AzureBlobConnectorConfig.ConnectionStringConfig, ConfigType.Password, "", Importance.High,
            "Azure Storage connection string (alternative to account name/key)")
        .Define(AzureBlobConnectorConfig.AccountNameConfig, ConfigType.String, "", Importance.High,
            "Azure Storage account name")
        .Define(AzureBlobConnectorConfig.AccountKeyConfig, ConfigType.Password, "", Importance.High,
            "Azure Storage account key")
        .Define(AzureBlobConnectorConfig.ContainerNameConfig, ConfigType.String, Importance.High,
            "Azure Blob container name")
        .Define(AzureBlobConnectorConfig.TopicConfig, ConfigType.String, Importance.High,
            "Destination Surgewave topic", EditorHint.Topic)
        .Define(AzureBlobConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low,
            "Custom Azure Storage endpoint (for Azurite emulator)")
        .Define(AzureBlobConnectorConfig.PrefixConfig, ConfigType.String, "", Importance.Medium,
            "Blob prefix filter")
        .Define(AzureBlobConnectorConfig.FormatConfig, ConfigType.String, AzureBlobConnectorConfig.DefaultFormat, Importance.Medium,
            "Blob format: json, jsonlines, csv, raw", EditorHint.Select, options: ["json", "jsonlines", "csv", "raw"])
        .Define(AzureBlobConnectorConfig.PollIntervalMsConfig, ConfigType.Long, AzureBlobConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Poll interval in milliseconds")
        .Define(AzureBlobConnectorConfig.DeleteAfterReadConfig, ConfigType.Boolean, AzureBlobConnectorConfig.DefaultDeleteAfterRead, Importance.Medium,
            "Delete blobs after reading");

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

        if (!config.TryGetValue(AzureBlobConnectorConfig.TopicConfig, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"Missing required config: {AzureBlobConnectorConfig.TopicConfig}");
        }

        // Validate format
        var format = GetConfigValue(config, AzureBlobConnectorConfig.FormatConfig, AzureBlobConnectorConfig.DefaultFormat);
        if (format is not (AzureBlobConnectorConfig.FormatJson or AzureBlobConnectorConfig.FormatJsonLines or
            AzureBlobConnectorConfig.FormatCsv or AzureBlobConnectorConfig.FormatRaw))
        {
            throw new ArgumentException(
                $"Invalid format '{format}'. Must be '{AzureBlobConnectorConfig.FormatJson}', " +
                $"'{AzureBlobConnectorConfig.FormatJsonLines}', '{AzureBlobConnectorConfig.FormatCsv}', or '{AzureBlobConnectorConfig.FormatRaw}'.");
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
