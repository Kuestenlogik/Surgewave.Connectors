using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Cassandra;

/// <summary>
/// Sink connector that writes data to Cassandra tables.
/// Supports batch inserts and upserts with configurable consistency levels.
/// </summary>
public sealed class CassandraSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(CassandraSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(CassandraConnectorConfig.ContactPointsConfig, ConfigType.String, Importance.High, "Comma-separated list of Cassandra contact points")
        .Define(CassandraConnectorConfig.PortConfig, ConfigType.Int, CassandraConnectorConfig.DefaultPort, Importance.Medium, "Cassandra native transport port")
        .Define(CassandraConnectorConfig.DatacenterConfig, ConfigType.String, "", Importance.High, "Local datacenter name for DCAwareRoundRobinPolicy")
        .Define(CassandraConnectorConfig.KeyspaceConfig, ConfigType.String, Importance.High, "Cassandra keyspace name")
        .Define(CassandraConnectorConfig.TableConfig, ConfigType.String, Importance.High, "Target table name")
        .Define(CassandraConnectorConfig.UsernameConfig, ConfigType.String, "", Importance.Medium, "Cassandra username")
        .Define(CassandraConnectorConfig.PasswordConfig, ConfigType.Password, "", Importance.Medium, "Cassandra password")
        .Define(CassandraConnectorConfig.ConsistencyLevelConfig, ConfigType.String, CassandraConnectorConfig.DefaultConsistencyLevel, Importance.Low, "Write consistency level")
        .Define(CassandraConnectorConfig.SslEnabledConfig, ConfigType.Boolean, false, Importance.Low, "Enable SSL/TLS connection")
        .Define(CassandraConnectorConfig.TopicsConfig, ConfigType.String, Importance.High, "Comma-separated list of topics to consume", EditorHint.Topic)
        .Define(CassandraConnectorConfig.WriteModeConfig, ConfigType.String, CassandraConnectorConfig.DefaultWriteMode, Importance.Medium, "Write mode: insert, upsert", EditorHint.Select, options: ["insert", "upsert"])
        .Define(CassandraConnectorConfig.BatchSizeConfig, ConfigType.Int, CassandraConnectorConfig.DefaultBatchSize, Importance.Low, "Batch size for writes")
        .Define(CassandraConnectorConfig.MaxRetryCountConfig, ConfigType.Int, CassandraConnectorConfig.DefaultMaxRetryCount, Importance.Low, "Maximum retry attempts")
        .Define(CassandraConnectorConfig.RetryDelayMsConfig, ConfigType.Int, (int)CassandraConnectorConfig.DefaultRetryDelayMs, Importance.Low, "Delay between retries in milliseconds")
        .Define(CassandraConnectorConfig.BatchTypeConfig, ConfigType.String, CassandraConnectorConfig.DefaultBatchType, Importance.Low, "Batch type: logged, unlogged, counter", EditorHint.Select, options: ["logged", "unlogged", "counter"])
        .Define(CassandraConnectorConfig.TtlSecondsConfig, ConfigType.Int, CassandraConnectorConfig.DefaultTtlSeconds, Importance.Low, "TTL in seconds (0 = no TTL)")
        .Define(CassandraConnectorConfig.PartitionKeyColumnsConfig, ConfigType.String, "", Importance.Low, "Partition key columns (comma-separated)")
        .Define(CassandraConnectorConfig.ClusteringKeyColumnsConfig, ConfigType.String, "", Importance.Low, "Clustering key columns (comma-separated)");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(CassandraConnectorConfig.ContactPointsConfig, out var contactPoints) || string.IsNullOrEmpty(contactPoints))
            throw new ArgumentException($"Required configuration '{CassandraConnectorConfig.ContactPointsConfig}' is missing");

        if (!config.TryGetValue(CassandraConnectorConfig.KeyspaceConfig, out var keyspace) || string.IsNullOrEmpty(keyspace))
            throw new ArgumentException($"Required configuration '{CassandraConnectorConfig.KeyspaceConfig}' is missing");

        if (!config.TryGetValue(CassandraConnectorConfig.TableConfig, out var table) || string.IsNullOrEmpty(table))
            throw new ArgumentException($"Required configuration '{CassandraConnectorConfig.TableConfig}' is missing");

        if (!config.TryGetValue(CassandraConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{CassandraConnectorConfig.TopicsConfig}' is missing");

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
        // Single task - Cassandra handles distributed writes internally
        return [new Dictionary<string, string>(_config)];
    }
}
