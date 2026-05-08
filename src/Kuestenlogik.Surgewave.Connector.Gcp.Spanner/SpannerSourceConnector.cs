using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Spanner;

/// <summary>
/// Source connector that reads from Google Cloud Spanner.
/// </summary>
[ConnectorMetadata(
    Name = "gcp-spanner-source",
    Description = "Reads rows from Google Cloud Spanner distributed relational database",
    Author = "Surgewave",
    Tags = "gcp, spanner, sql, relational, google, source")]
public sealed class SpannerSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(SpannerConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce Spanner rows to", EditorHint.Topic)
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
        .Define(SpannerConnectorConfig.Query, ConfigType.String, "", Importance.Medium,
            "SQL query to execute (alternative to table)")
        .Define(SpannerConnectorConfig.Table, ConfigType.String, "", Importance.Medium,
            "Table to read from (alternative to query)")
        .Define(SpannerConnectorConfig.Columns, ConfigType.List, "", Importance.Medium,
            "Columns to read (comma-separated, empty = all)")
        .Define(SpannerConnectorConfig.PollIntervalMs, ConfigType.Int,
            SpannerConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(SpannerConnectorConfig.IncrementalColumn, ConfigType.String, "", Importance.Medium,
            "Column for incremental reads (timestamp or int)")
        .Define(SpannerConnectorConfig.TimestampBound, ConfigType.String,
            SpannerConnectorConfig.DefaultTimestampBound, Importance.Low,
            "Timestamp bound: exact, strong, bounded_staleness")
        .Define(SpannerConnectorConfig.MaxStalenessSeconds, ConfigType.Int,
            SpannerConnectorConfig.DefaultMaxStalenessSeconds.ToString(), Importance.Low,
            "Max staleness seconds for bounded_staleness")
        .Define(SpannerConnectorConfig.RowLimit, ConfigType.Int,
            SpannerConnectorConfig.DefaultRowLimit.ToString(), Importance.Medium,
            "Maximum rows per poll");

    public override Type TaskClass => typeof(SpannerSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(SpannerConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{SpannerConnectorConfig.Topic}' is required");
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

        // Either query or table must be specified
        var query = config.GetValueOrDefault(SpannerConnectorConfig.Query, "");
        var table = config.GetValueOrDefault(SpannerConnectorConfig.Table, "");
        if (string.IsNullOrWhiteSpace(query) && string.IsNullOrWhiteSpace(table))
        {
            throw new ArgumentException($"Either '{SpannerConnectorConfig.Query}' or '{SpannerConnectorConfig.Table}' is required");
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
