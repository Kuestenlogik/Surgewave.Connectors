namespace Kuestenlogik.Surgewave.Connector.PostgreSql;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that writes records to PostgreSQL tables.
/// Supports both plain insert and upsert (ON CONFLICT DO UPDATE) modes.
/// </summary>
public sealed class PostgreSqlSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(PostgreSqlSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(PostgreSqlConnectorConfig.ConnectionConfig, ConfigType.Password, Importance.High,
            "PostgreSQL connection string")
        .Define(PostgreSqlConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
        .Define(PostgreSqlConnectorConfig.TableConfig, ConfigType.String, Importance.High,
            "Target table name")
        .Define(PostgreSqlConnectorConfig.SchemaConfig, ConfigType.String, PostgreSqlConnectorConfig.DefaultSchema, Importance.Medium,
            "Target schema name")
        .Define(PostgreSqlConnectorConfig.InsertModeConfig, ConfigType.String, PostgreSqlConnectorConfig.InsertModeInsert, Importance.Medium,
            "Insert mode: 'insert' or 'upsert'", EditorHint.Select, options: ["insert", "upsert"])
        .Define(PostgreSqlConnectorConfig.PkModeConfig, ConfigType.String, PostgreSqlConnectorConfig.PkModeRecordKey, Importance.Medium,
            "Primary key mode: 'record_key' or 'record_value'", EditorHint.Select, options: ["record_key", "record_value"])
        .Define(PostgreSqlConnectorConfig.PkFieldsConfig, ConfigType.String, "", Importance.Medium,
            "Comma-separated primary key field names (required for upsert)")
        .Define(PostgreSqlConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)PostgreSqlConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to buffer before flushing")
        .Define(PostgreSqlConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)PostgreSqlConnectorConfig.DefaultRetryMax, Importance.Medium,
            "Maximum retry attempts on failure")
        .Define(PostgreSqlConnectorConfig.RetryBackoffMsConfig, ConfigType.Long, PostgreSqlConnectorConfig.DefaultRetryBackoffMs, Importance.Medium,
            "Backoff time between retries in milliseconds")
        // pgvector configs
        .Define(PostgreSqlConnectorConfig.VectorFieldConfig, ConfigType.String, "", Importance.Medium,
            "JSON field containing vector embedding array (enables pgvector support)")
        .Define(PostgreSqlConnectorConfig.VectorDimensionsConfig, ConfigType.Int, (long)PostgreSqlConnectorConfig.DefaultVectorDimensions, Importance.Medium,
            "Vector dimensions (default: 1536 for OpenAI text-embedding-3-small)")
        .Define(PostgreSqlConnectorConfig.VectorCreateExtensionConfig, ConfigType.Boolean, true, Importance.Low,
            "Auto-create pgvector extension if not exists")
        .Define(PostgreSqlConnectorConfig.VectorIndexTypeConfig, ConfigType.String, PostgreSqlConnectorConfig.VectorIndexNone, Importance.Low,
            "Vector index type: 'none', 'ivfflat', or 'hnsw'")
        .Define(PostgreSqlConnectorConfig.VectorDistanceMetricConfig, ConfigType.String, PostgreSqlConnectorConfig.VectorDistanceCosine, Importance.Low,
            "Distance metric for index: 'cosine', 'l2', or 'inner_product'");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(PostgreSqlConnectorConfig.ConnectionConfig, out _))
            throw new ArgumentException($"Missing required config: {PostgreSqlConnectorConfig.ConnectionConfig}");

        if (!config.TryGetValue(PostgreSqlConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {PostgreSqlConnectorConfig.TopicsConfig}");

        if (!config.TryGetValue(PostgreSqlConnectorConfig.TableConfig, out _))
            throw new ArgumentException($"Missing required config: {PostgreSqlConnectorConfig.TableConfig}");

        // Validate insert mode
        var insertMode = config.TryGetValue(PostgreSqlConnectorConfig.InsertModeConfig, out var mode)
            ? mode
            : PostgreSqlConnectorConfig.InsertModeInsert;

        if (insertMode is not (PostgreSqlConnectorConfig.InsertModeInsert or PostgreSqlConnectorConfig.InsertModeUpsert))
            throw new ArgumentException($"Invalid insert mode '{insertMode}'. Must be '{PostgreSqlConnectorConfig.InsertModeInsert}' or '{PostgreSqlConnectorConfig.InsertModeUpsert}'");

        // Validate upsert requires pk.fields
        if (insertMode == PostgreSqlConnectorConfig.InsertModeUpsert)
        {
            if (!config.TryGetValue(PostgreSqlConnectorConfig.PkFieldsConfig, out var pkFields) || string.IsNullOrWhiteSpace(pkFields))
                throw new ArgumentException($"Upsert mode requires '{PostgreSqlConnectorConfig.PkFieldsConfig}' to be specified");
        }

        // Validate pk mode
        var pkMode = config.TryGetValue(PostgreSqlConnectorConfig.PkModeConfig, out var pk)
            ? pk
            : PostgreSqlConnectorConfig.PkModeRecordKey;

        if (pkMode is not (PostgreSqlConnectorConfig.PkModeRecordKey or PostgreSqlConnectorConfig.PkModeRecordValue))
            throw new ArgumentException($"Invalid pk.mode '{pkMode}'. Must be '{PostgreSqlConnectorConfig.PkModeRecordKey}' or '{PostgreSqlConnectorConfig.PkModeRecordValue}'");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task for simplicity
        return [new Dictionary<string, string>(_config)];
    }
}
