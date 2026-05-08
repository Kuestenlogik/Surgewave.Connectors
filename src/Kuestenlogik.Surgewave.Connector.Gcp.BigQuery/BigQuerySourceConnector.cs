using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.BigQuery;

/// <summary>
/// Source connector that reads data from BigQuery tables or queries.
/// Supports table polling and custom SQL query modes.
/// </summary>
public sealed class BigQuerySourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(BigQuerySourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(BigQueryConnectorConfig.ProjectIdConfig, ConfigType.String, Importance.High, "GCP project ID")
        .Define(BigQueryConnectorConfig.CredentialsJsonConfig, ConfigType.Password, "", Importance.High, "Service account JSON credentials (inline)")
        .Define(BigQueryConnectorConfig.CredentialsFileConfig, ConfigType.String, "", Importance.High, "Path to service account JSON file", EditorHint.FilePath)
        .Define(BigQueryConnectorConfig.DatasetConfig, ConfigType.String, Importance.High, "BigQuery dataset name")
        .Define(BigQueryConnectorConfig.LocationConfig, ConfigType.String, BigQueryConnectorConfig.DefaultLocation, Importance.Medium, "Dataset location (US, EU, etc.)")
        .Define(BigQueryConnectorConfig.ModeConfig, ConfigType.String, BigQueryConnectorConfig.DefaultMode, Importance.Medium, "Mode: table, query")
        .Define(BigQueryConnectorConfig.TableConfig, ConfigType.String, "", Importance.High, "Table name (for table mode)")
        .Define(BigQueryConnectorConfig.QueryConfig, ConfigType.String, "", Importance.Medium, "Custom SQL query (for query mode)", EditorHint.Code, "sql")
        .Define(BigQueryConnectorConfig.TopicPatternConfig, ConfigType.String, BigQueryConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern")
        .Define(BigQueryConnectorConfig.PollIntervalMsConfig, ConfigType.Int, (int)BigQueryConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in milliseconds")
        .Define(BigQueryConnectorConfig.MaxRowsPerPollConfig, ConfigType.Int, BigQueryConnectorConfig.DefaultMaxRowsPerPoll, Importance.Low, "Max rows per poll")
        .Define(BigQueryConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, true, Importance.Low, "Include BigQuery metadata in output")
        .Define(BigQueryConnectorConfig.TimestampColumnConfig, ConfigType.String, "", Importance.Low, "Timestamp column for incremental polling")
        .Define(BigQueryConnectorConfig.PartitionFieldConfig, ConfigType.String, "", Importance.Low, "Partition field for efficient querying")
        .Define(BigQueryConnectorConfig.UseStandardSqlConfig, ConfigType.Boolean, true, Importance.Low, "Use standard SQL (vs legacy SQL)");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(BigQueryConnectorConfig.ProjectIdConfig, out var projectId) || string.IsNullOrEmpty(projectId))
            throw new ArgumentException($"Required configuration '{BigQueryConnectorConfig.ProjectIdConfig}' is missing");

        if (!config.TryGetValue(BigQueryConnectorConfig.DatasetConfig, out var dataset) || string.IsNullOrEmpty(dataset))
            throw new ArgumentException($"Required configuration '{BigQueryConnectorConfig.DatasetConfig}' is missing");

        var mode = GetConfigValue(config, BigQueryConnectorConfig.ModeConfig, BigQueryConnectorConfig.DefaultMode);

        // Validate mode-specific requirements
        if (mode.Equals("table", StringComparison.OrdinalIgnoreCase))
        {
            if (!config.TryGetValue(BigQueryConnectorConfig.TableConfig, out var table) || string.IsNullOrEmpty(table))
                throw new ArgumentException($"Required configuration '{BigQueryConnectorConfig.TableConfig}' is missing for table mode");
        }
        else if (mode.Equals("query", StringComparison.OrdinalIgnoreCase))
        {
            if (!config.TryGetValue(BigQueryConnectorConfig.QueryConfig, out var query) || string.IsNullOrEmpty(query))
                throw new ArgumentException($"Required configuration '{BigQueryConnectorConfig.QueryConfig}' is missing for query mode");
        }

        _config = new Dictionary<string, string>(config);
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - BigQuery handles query execution
        return [new Dictionary<string, string>(_config)];
    }
}
