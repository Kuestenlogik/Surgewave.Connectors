namespace Kuestenlogik.Surgewave.Connector.PostgreSql;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A source connector that captures changes from PostgreSQL using logical replication.
/// Uses the pgoutput plugin (built into PostgreSQL 10+) for CDC.
/// </summary>
public sealed class PostgreSqlCdcSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(PostgreSqlCdcSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(PostgreSqlConnectorConfig.ConnectionConfig, ConfigType.Password, Importance.High,
            "PostgreSQL connection string")
        .Define(PostgreSqlConnectorConfig.TablesConfig, ConfigType.String, Importance.High,
            "Comma-separated list of tables to capture (schema.table format)")
        .Define(PostgreSqlConnectorConfig.SlotNameConfig, ConfigType.String, PostgreSqlConnectorConfig.DefaultSlotName, Importance.High,
            "Replication slot name")
        .Define(PostgreSqlConnectorConfig.PublicationNameConfig, ConfigType.String, PostgreSqlConnectorConfig.DefaultPublicationName, Importance.High,
            "Publication name for logical replication")
        .Define(PostgreSqlConnectorConfig.CreateSlotConfig, ConfigType.Boolean, true, Importance.Medium,
            "Whether to auto-create the replication slot")
        .Define(PostgreSqlConnectorConfig.CreatePublicationConfig, ConfigType.Boolean, true, Importance.Medium,
            "Whether to auto-create the publication")
        .Define(PostgreSqlConnectorConfig.TopicPrefixConfig, ConfigType.String, "", Importance.Medium,
            "Prefix for generated topic names", EditorHint.Topic)
        .Define(PostgreSqlConnectorConfig.TopicPatternConfig, ConfigType.String, PostgreSqlConnectorConfig.DefaultTopicPattern, Importance.Medium,
            "Topic naming pattern (supports ${schema} and ${table})", EditorHint.Topic)
        .Define(PostgreSqlConnectorConfig.IncludeSchemaConfig, ConfigType.Boolean, true, Importance.Medium,
            "Whether to include schema in topic name")
        .Define(PostgreSqlConnectorConfig.IncludeBeforeValuesConfig, ConfigType.Boolean, true, Importance.Medium,
            "Whether to include 'before' values for UPDATE/DELETE")
        .Define(PostgreSqlConnectorConfig.SnapshotModeConfig, ConfigType.String, PostgreSqlConnectorConfig.SnapshotModeInitial, Importance.Medium,
            "Snapshot mode: 'initial' (snapshot once), 'never', or 'always'", EditorHint.Select, options: ["initial", "never", "always"])
        .Define(PostgreSqlConnectorConfig.PollIntervalMsConfig, ConfigType.Long, PostgreSqlConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Polling interval in milliseconds")
        .Define(PostgreSqlConnectorConfig.BatchMaxRecordsConfig, ConfigType.Int, (long)PostgreSqlConnectorConfig.DefaultBatchMaxRecords, Importance.Medium,
            "Maximum records per poll");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(PostgreSqlConnectorConfig.ConnectionConfig, out _))
            throw new ArgumentException($"Missing required config: {PostgreSqlConnectorConfig.ConnectionConfig}");

        if (!config.TryGetValue(PostgreSqlConnectorConfig.TablesConfig, out _))
            throw new ArgumentException($"Missing required config: {PostgreSqlConnectorConfig.TablesConfig}");

        // Validate snapshot mode
        var snapshotMode = config.TryGetValue(PostgreSqlConnectorConfig.SnapshotModeConfig, out var mode)
            ? mode
            : PostgreSqlConnectorConfig.SnapshotModeInitial;

        if (snapshotMode is not (PostgreSqlConnectorConfig.SnapshotModeInitial or PostgreSqlConnectorConfig.SnapshotModeNever or PostgreSqlConnectorConfig.SnapshotModeAlways))
            throw new ArgumentException($"Invalid snapshot mode '{snapshotMode}'. Must be '{PostgreSqlConnectorConfig.SnapshotModeInitial}', '{PostgreSqlConnectorConfig.SnapshotModeNever}', or '{PostgreSqlConnectorConfig.SnapshotModeAlways}'");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // CDC replication slot can only be consumed by a single consumer
        // So we always return a single task config
        return [new Dictionary<string, string>(_config)];
    }
}
