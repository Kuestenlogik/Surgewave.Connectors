using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.BigQuery;

/// <summary>
/// Sink connector that writes records to BigQuery tables.
/// Supports streaming inserts and batch load jobs.
/// </summary>
public sealed class BigQuerySinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(BigQuerySinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(BigQueryConnectorConfig.ProjectIdConfig, ConfigType.String, Importance.High, "GCP project ID")
        .Define(BigQueryConnectorConfig.CredentialsJsonConfig, ConfigType.Password, "", Importance.High, "Service account JSON credentials (inline)")
        .Define(BigQueryConnectorConfig.CredentialsFileConfig, ConfigType.String, "", Importance.High, "Path to service account JSON file", EditorHint.FilePath)
        .Define(BigQueryConnectorConfig.DatasetConfig, ConfigType.String, Importance.High, "BigQuery dataset name")
        .Define(BigQueryConnectorConfig.LocationConfig, ConfigType.String, BigQueryConnectorConfig.DefaultLocation, Importance.Medium, "Dataset location (US, EU, etc.)")
        .Define(BigQueryConnectorConfig.TableConfig, ConfigType.String, Importance.High, "Target table name")
        .Define(BigQueryConnectorConfig.TopicsConfig, ConfigType.String, Importance.High, "Surgewave topics to consume (comma-separated)", EditorHint.Topic)
        .Define(BigQueryConnectorConfig.WriteModeConfig, ConfigType.String, BigQueryConnectorConfig.DefaultWriteMode, Importance.Medium, "Write mode: insert, append, truncate")
        .Define(BigQueryConnectorConfig.BatchSizeConfig, ConfigType.Int, BigQueryConnectorConfig.DefaultBatchSize, Importance.Low, "Batch size for inserts")
        .Define(BigQueryConnectorConfig.MaxRetryCountConfig, ConfigType.Int, BigQueryConnectorConfig.DefaultMaxRetryCount, Importance.Low, "Max retry count for transient failures")
        .Define(BigQueryConnectorConfig.RetryDelayMsConfig, ConfigType.Int, (int)BigQueryConnectorConfig.DefaultRetryDelayMs, Importance.Low, "Retry delay in milliseconds")
        .Define(BigQueryConnectorConfig.AutoCreateTableConfig, ConfigType.Boolean, false, Importance.Low, "Auto-create table if not exists")
        .Define(BigQueryConnectorConfig.AutoCreateDatasetConfig, ConfigType.Boolean, false, Importance.Low, "Auto-create dataset if not exists")
        .Define(BigQueryConnectorConfig.UseStreamingConfig, ConfigType.Boolean, true, Importance.Medium, "Use streaming inserts (vs load jobs)")
        .Define(BigQueryConnectorConfig.SchemaUpdateOptionsConfig, ConfigType.String, "", Importance.Low, "Schema update options (comma-separated)")
        .Define(BigQueryConnectorConfig.TimePartitioningConfig, ConfigType.String, "", Importance.Low, "Time partitioning: DAY, HOUR, MONTH, YEAR")
        .Define(BigQueryConnectorConfig.ClusteringFieldsConfig, ConfigType.String, "", Importance.Low, "Clustering fields (comma-separated)");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(BigQueryConnectorConfig.ProjectIdConfig, out var projectId) || string.IsNullOrEmpty(projectId))
            throw new ArgumentException($"Required configuration '{BigQueryConnectorConfig.ProjectIdConfig}' is missing");

        if (!config.TryGetValue(BigQueryConnectorConfig.DatasetConfig, out var dataset) || string.IsNullOrEmpty(dataset))
            throw new ArgumentException($"Required configuration '{BigQueryConnectorConfig.DatasetConfig}' is missing");

        if (!config.TryGetValue(BigQueryConnectorConfig.TableConfig, out var table) || string.IsNullOrEmpty(table))
            throw new ArgumentException($"Required configuration '{BigQueryConnectorConfig.TableConfig}' is missing");

        if (!config.TryGetValue(BigQueryConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{BigQueryConnectorConfig.TopicsConfig}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task for simplicity
        return [new Dictionary<string, string>(_config)];
    }
}
