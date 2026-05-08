namespace Kuestenlogik.Surgewave.Connector.TigerBeetle;

/// <summary>
/// Configuration constants for TigerBeetle connector.
/// </summary>
public static class TigerBeetleConnectorConfig
{
    // Connection settings
    public const string ClusterAddresses = "tigerbeetle.cluster.addresses";  // Comma-separated addresses
    public const string ClusterId = "tigerbeetle.cluster.id";
    public const string MaxConcurrency = "tigerbeetle.max.concurrency";

    // Source settings
    public const string Topic = "topic";
    public const string PollIntervalMs = "poll.interval.ms";
    public const string WatchAccounts = "tigerbeetle.watch.accounts";  // Comma-separated account IDs to watch
    public const string IncludeTransfers = "tigerbeetle.include.transfers";
    public const string LookupBatchSize = "tigerbeetle.lookup.batch.size";

    // Sink settings
    public const string Topics = "topics";
    public const string OperationType = "tigerbeetle.operation.type";  // create_account, create_transfer
    public const string BatchSize = "tigerbeetle.batch.size";

    // Defaults
    public const int DefaultPollIntervalMs = 1000;
    public const int DefaultMaxConcurrency = 32;
    public const int DefaultLookupBatchSize = 8190;  // TigerBeetle max batch size
    public const int DefaultBatchSize = 8190;
    public const string DefaultOperationType = "create_transfer";
}
