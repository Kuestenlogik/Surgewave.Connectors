using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Spanner;

/// <summary>
/// Sink connector that writes to Google Cloud Spanner.
/// </summary>
[ConnectorMetadata(
    Name = "gcp-spanner-sink",
    Description = "Writes rows to Google Cloud Spanner distributed relational database",
    Author = "Surgewave",
    Tags = "gcp, spanner, sql, relational, google, sink")]
public sealed class SpannerSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(SpannerConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume from", EditorHint.Topic)
        .Define(SpannerConnectorConfig.ProjectId, ConfigType.String, Importance.High,
            "GCP project ID")
        .Define(SpannerConnectorConfig.InstanceId, ConfigType.String, Importance.High,
            "Spanner instance ID")
        .Define(SpannerConnectorConfig.DatabaseId, ConfigType.String, Importance.High,
            "Spanner database ID")
        .Define(SpannerConnectorConfig.CredentialsJson, ConfigType.Password, "", Importance.Medium,
            "GCP credentials JSON (inline)")
        .Define(SpannerConnectorConfig.CredentialsFile, ConfigType.String, "", Importance.Medium,
            "Path to GCP credentials file", EditorHint.FilePath)
        .Define(SpannerConnectorConfig.EmulatorHost, ConfigType.String, "", Importance.Low,
            "Spanner emulator host for testing")
        .Define(SpannerConnectorConfig.TargetTable, ConfigType.String, Importance.High,
            "Target table for writes")
        .Define(SpannerConnectorConfig.WriteMode, ConfigType.String,
            SpannerConnectorConfig.DefaultWriteMode, Importance.Medium,
            "Write mode: insert, update, upsert, delete")
        .Define(SpannerConnectorConfig.KeyColumns, ConfigType.List, "", Importance.Medium,
            "Primary key columns (comma-separated, required for update/delete)")
        .Define(SpannerConnectorConfig.BatchSize, ConfigType.Int,
            SpannerConnectorConfig.DefaultBatchSize.ToString(), Importance.Medium,
            "Batch size for mutations")
        .Define(SpannerConnectorConfig.CommitTimeout, ConfigType.Int,
            SpannerConnectorConfig.DefaultCommitTimeoutSeconds.ToString(), Importance.Low,
            "Commit timeout in seconds");

    public override Type TaskClass => typeof(SpannerSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(SpannerConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{SpannerConnectorConfig.Topics}' is required");
        }

        if (!config.TryGetValue(SpannerConnectorConfig.ProjectId, out var projectId) ||
            string.IsNullOrWhiteSpace(projectId))
        {
            throw new ArgumentException($"'{SpannerConnectorConfig.ProjectId}' is required");
        }

        if (!config.TryGetValue(SpannerConnectorConfig.InstanceId, out var instanceId) ||
            string.IsNullOrWhiteSpace(instanceId))
        {
            throw new ArgumentException($"'{SpannerConnectorConfig.InstanceId}' is required");
        }

        if (!config.TryGetValue(SpannerConnectorConfig.DatabaseId, out var databaseId) ||
            string.IsNullOrWhiteSpace(databaseId))
        {
            throw new ArgumentException($"'{SpannerConnectorConfig.DatabaseId}' is required");
        }

        if (!config.TryGetValue(SpannerConnectorConfig.TargetTable, out var table) ||
            string.IsNullOrWhiteSpace(table))
        {
            throw new ArgumentException($"'{SpannerConnectorConfig.TargetTable}' is required");
        }
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }

    public override void Stop()
    {
    }
}
