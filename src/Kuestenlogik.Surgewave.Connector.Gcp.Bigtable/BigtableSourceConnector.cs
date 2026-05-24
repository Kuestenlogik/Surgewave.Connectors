using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Bigtable;

/// <summary>
/// Source connector that reads from Google Cloud Bigtable.
/// </summary>
[ConnectorMetadata(
    Name = "gcp-bigtable-source",
    Description = "Reads rows from Google Cloud Bigtable wide-column database",
    Author = "Surgewave",
    Tags = "gcp, bigtable, nosql, wide-column, google, source")]
public sealed class BigtableSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(BigtableConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce Bigtable rows to", EditorHint.Topic)
        .Define(BigtableConnectorConfig.ProjectId, ConfigType.String, Importance.High,
            "GCP project ID")
        .Define(BigtableConnectorConfig.InstanceId, ConfigType.String, Importance.High,
            "Bigtable instance ID")
        .Define(BigtableConnectorConfig.TableId, ConfigType.String, Importance.High,
            "Bigtable table ID")
        .Define(BigtableConnectorConfig.CredentialsJson, ConfigType.Password, "", Importance.Medium,
            "GCP credentials JSON (inline)")
        .Define(BigtableConnectorConfig.CredentialsFile, ConfigType.String, "", Importance.Medium,
            "Path to GCP credentials file", EditorHint.FilePath)
        .Define(BigtableConnectorConfig.EmulatorHost, ConfigType.String, "", Importance.Low,
            "Bigtable emulator host for testing")
        .Define(BigtableConnectorConfig.PollIntervalMs, ConfigType.Int,
            BigtableConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(BigtableConnectorConfig.RowKeyPrefix, ConfigType.String, "", Importance.Medium,
            "Row key prefix filter")
        .Define(BigtableConnectorConfig.RowKeyStart, ConfigType.String, "", Importance.Medium,
            "Start row key (inclusive)")
        .Define(BigtableConnectorConfig.RowKeyEnd, ConfigType.String, "", Importance.Medium,
            "End row key (exclusive)")
        .Define(BigtableConnectorConfig.ColumnFamily, ConfigType.String, "", Importance.Medium,
            "Column family to read (empty = all)")
        .Define(BigtableConnectorConfig.Columns, ConfigType.List, "", Importance.Medium,
            "Column qualifiers to read (comma-separated, empty = all)")
        .Define(BigtableConnectorConfig.RowLimit, ConfigType.Int,
            BigtableConnectorConfig.DefaultRowLimit.ToString(), Importance.Medium,
            "Maximum rows per poll")
        .Define(BigtableConnectorConfig.IncludeTimestamp, ConfigType.Boolean, "true", Importance.Low,
            "Include cell timestamps in output");

    public override Type TaskClass => typeof(BigtableSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(BigtableConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{BigtableConnectorConfig.Topic}' is required");
        }

        if (!config.TryGetValue(BigtableConnectorConfig.ProjectId, out var projectId) ||
            string.IsNullOrWhiteSpace(projectId))
        {
            throw new ArgumentException($"'{BigtableConnectorConfig.ProjectId}' is required");
        }

        if (!config.TryGetValue(BigtableConnectorConfig.InstanceId, out var instanceId) ||
            string.IsNullOrWhiteSpace(instanceId))
        {
            throw new ArgumentException($"'{BigtableConnectorConfig.InstanceId}' is required");
        }

        if (!config.TryGetValue(BigtableConnectorConfig.TableId, out var tableId) ||
            string.IsNullOrWhiteSpace(tableId))
        {
            throw new ArgumentException($"'{BigtableConnectorConfig.TableId}' is required");
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
