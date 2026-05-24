using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.TigerBeetle;

/// <summary>
/// Sink connector that writes to TigerBeetle accounting database.
/// </summary>
[ConnectorMetadata(
    Name = "tigerbeetle-sink",
    Description = "Creates accounts and transfers in TigerBeetle financial accounting database",
    Author = "Surgewave",
    Tags = "tigerbeetle, accounting, financial, ledger, oltp, sink")]
public sealed class TigerBeetleSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(TigerBeetleConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume from", EditorHint.Topic)
        .Define(TigerBeetleConnectorConfig.ClusterAddresses, ConfigType.List, Importance.High,
            "TigerBeetle cluster addresses (comma-separated)")
        .Define(TigerBeetleConnectorConfig.ClusterId, ConfigType.String, "0", Importance.High,
            "TigerBeetle cluster ID")
        .Define(TigerBeetleConnectorConfig.MaxConcurrency, ConfigType.Int,
            TigerBeetleConnectorConfig.DefaultMaxConcurrency.ToString(), Importance.Medium,
            "Max concurrent requests")
        .Define(TigerBeetleConnectorConfig.OperationType, ConfigType.String,
            TigerBeetleConnectorConfig.DefaultOperationType, Importance.Medium,
            "Operation type: create_account, create_transfer")
        .Define(TigerBeetleConnectorConfig.BatchSize, ConfigType.Int,
            TigerBeetleConnectorConfig.DefaultBatchSize.ToString(), Importance.Medium,
            "Batch size for operations");

    public override Type TaskClass => typeof(TigerBeetleSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(TigerBeetleConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{TigerBeetleConnectorConfig.Topics}' is required");
        }

        if (!config.TryGetValue(TigerBeetleConnectorConfig.ClusterAddresses, out var addresses) ||
            string.IsNullOrWhiteSpace(addresses))
        {
            throw new ArgumentException($"'{TigerBeetleConnectorConfig.ClusterAddresses}' is required");
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
