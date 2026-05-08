using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Bigtable;

/// <summary>
/// Sink connector that writes to Google Cloud Bigtable.
/// </summary>
[ConnectorMetadata(
    Name = "gcp-bigtable-sink",
    Description = "Writes rows to Google Cloud Bigtable wide-column database",
    Author = "Surgewave",
    Tags = "gcp, bigtable, nosql, wide-column, google, sink")]
public sealed class BigtableSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(BigtableConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume from", EditorHint.Topic)
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
        .Define(BigtableConnectorConfig.RowKeyField, ConfigType.String, "rowKey", Importance.Medium,
            "JSON field containing row key")
        .Define(BigtableConnectorConfig.DefaultColumnFamily, ConfigType.String,
            BigtableConnectorConfig.DefaultColumnFamilyName, Importance.Medium,
            "Default column family for writes")
        .Define(BigtableConnectorConfig.WriteMode, ConfigType.String,
            BigtableConnectorConfig.DefaultWriteMode, Importance.Medium,
            "Write mode: set, append, increment")
        .Define(BigtableConnectorConfig.BatchSize, ConfigType.Int,
            BigtableConnectorConfig.DefaultBatchSize.ToString(), Importance.Medium,
            "Batch size for mutations");

    public override Type TaskClass => typeof(BigtableSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(BigtableConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{BigtableConnectorConfig.Topics}' is required");
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
