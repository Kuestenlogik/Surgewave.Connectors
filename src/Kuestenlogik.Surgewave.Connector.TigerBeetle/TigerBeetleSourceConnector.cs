using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.TigerBeetle;

/// <summary>
/// Source connector that reads from TigerBeetle accounting database.
/// </summary>
[ConnectorMetadata(
    Name = "tigerbeetle-source",
    Description = "Reads accounts and transfers from TigerBeetle financial accounting database",
    Author = "Surgewave",
    Tags = "tigerbeetle, accounting, financial, ledger, oltp, source")]
public sealed class TigerBeetleSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(TigerBeetleConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce account/transfer data to", EditorHint.Topic)
        .Define(TigerBeetleConnectorConfig.ClusterAddresses, ConfigType.List, Importance.High,
            "TigerBeetle cluster addresses (comma-separated, e.g., '127.0.0.1:3000,127.0.0.1:3001')")
        .Define(TigerBeetleConnectorConfig.ClusterId, ConfigType.String, "0", Importance.High,
            "TigerBeetle cluster ID")
        .Define(TigerBeetleConnectorConfig.MaxConcurrency, ConfigType.Int,
            TigerBeetleConnectorConfig.DefaultMaxConcurrency.ToString(), Importance.Medium,
            "Max concurrent requests")
        .Define(TigerBeetleConnectorConfig.WatchAccounts, ConfigType.List, "", Importance.Medium,
            "Account IDs to watch (comma-separated UInt128 values)")
        .Define(TigerBeetleConnectorConfig.PollIntervalMs, ConfigType.Int,
            TigerBeetleConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(TigerBeetleConnectorConfig.IncludeTransfers, ConfigType.Boolean, "true", Importance.Medium,
            "Include transfer lookups for watched accounts")
        .Define(TigerBeetleConnectorConfig.LookupBatchSize, ConfigType.Int,
            TigerBeetleConnectorConfig.DefaultLookupBatchSize.ToString(), Importance.Low,
            "Batch size for lookups");

    public override Type TaskClass => typeof(TigerBeetleSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(TigerBeetleConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{TigerBeetleConnectorConfig.Topic}' is required");
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
