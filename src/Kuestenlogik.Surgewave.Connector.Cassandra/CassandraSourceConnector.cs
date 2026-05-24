using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Cassandra;

/// <summary>
/// Source connector that reads data from Cassandra tables.
/// Supports table polling and custom CQL query modes.
/// </summary>
public sealed class CassandraSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(CassandraSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(CassandraConnectorConfig.ContactPointsConfig, ConfigType.String, Importance.High, "Comma-separated list of Cassandra contact points")
        .Define(CassandraConnectorConfig.PortConfig, ConfigType.Int, CassandraConnectorConfig.DefaultPort, Importance.Medium, "Cassandra native transport port")
        .Define(CassandraConnectorConfig.DatacenterConfig, ConfigType.String, "", Importance.High, "Local datacenter name for DCAwareRoundRobinPolicy")
        .Define(CassandraConnectorConfig.KeyspaceConfig, ConfigType.String, Importance.High, "Cassandra keyspace name")
        .Define(CassandraConnectorConfig.UsernameConfig, ConfigType.String, "", Importance.Medium, "Cassandra username")
        .Define(CassandraConnectorConfig.PasswordConfig, ConfigType.Password, "", Importance.Medium, "Cassandra password")
        .Define(CassandraConnectorConfig.ConsistencyLevelConfig, ConfigType.String, CassandraConnectorConfig.DefaultConsistencyLevel, Importance.Low, "Read consistency level")
        .Define(CassandraConnectorConfig.SslEnabledConfig, ConfigType.Boolean, false, Importance.Low, "Enable SSL/TLS connection")
        .Define(CassandraConnectorConfig.ModeConfig, ConfigType.String, CassandraConnectorConfig.DefaultMode, Importance.Medium, "Mode: table, query", EditorHint.Select, options: ["table", "query"])
        .Define(CassandraConnectorConfig.TableConfig, ConfigType.String, "", Importance.High, "Table name (for table mode)")
        .Define(CassandraConnectorConfig.QueryConfig, ConfigType.String, "", Importance.Medium, "Custom CQL query (for query mode)", EditorHint.Code, "sql")
        .Define(CassandraConnectorConfig.TopicPatternConfig, ConfigType.String, CassandraConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern")
        .Define(CassandraConnectorConfig.PollIntervalMsConfig, ConfigType.Int, (int)CassandraConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in milliseconds")
        .Define(CassandraConnectorConfig.MaxRowsPerPollConfig, ConfigType.Int, CassandraConnectorConfig.DefaultMaxRowsPerPoll, Importance.Low, "Max rows per poll")
        .Define(CassandraConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, true, Importance.Low, "Include Cassandra metadata in output")
        .Define(CassandraConnectorConfig.TimestampColumnConfig, ConfigType.String, "", Importance.Low, "Timestamp column for incremental polling")
        .Define(CassandraConnectorConfig.PartitionKeyColumnsConfig, ConfigType.String, "", Importance.Low, "Partition key columns (comma-separated)")
        .Define(CassandraConnectorConfig.ClusteringKeyColumnsConfig, ConfigType.String, "", Importance.Low, "Clustering key columns (comma-separated)");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(CassandraConnectorConfig.ContactPointsConfig, out var contactPoints) || string.IsNullOrEmpty(contactPoints))
            throw new ArgumentException($"Required configuration '{CassandraConnectorConfig.ContactPointsConfig}' is missing");

        if (!config.TryGetValue(CassandraConnectorConfig.KeyspaceConfig, out var keyspace) || string.IsNullOrEmpty(keyspace))
            throw new ArgumentException($"Required configuration '{CassandraConnectorConfig.KeyspaceConfig}' is missing");

        var mode = GetConfigValue(config, CassandraConnectorConfig.ModeConfig, CassandraConnectorConfig.DefaultMode);

        // Validate mode-specific requirements
        if (mode.Equals("table", StringComparison.OrdinalIgnoreCase))
        {
            if (!config.TryGetValue(CassandraConnectorConfig.TableConfig, out var table) || string.IsNullOrEmpty(table))
                throw new ArgumentException($"Required configuration '{CassandraConnectorConfig.TableConfig}' is missing for table mode");
        }
        else if (mode.Equals("query", StringComparison.OrdinalIgnoreCase))
        {
            if (!config.TryGetValue(CassandraConnectorConfig.QueryConfig, out var query) || string.IsNullOrEmpty(query))
                throw new ArgumentException($"Required configuration '{CassandraConnectorConfig.QueryConfig}' is missing for query mode");
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
        // Single task - Cassandra handles distributed queries internally
        return [new Dictionary<string, string>(_config)];
    }
}
