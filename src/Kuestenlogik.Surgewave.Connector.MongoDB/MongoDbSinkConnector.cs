namespace Kuestenlogik.Surgewave.Connector.MongoDB;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that writes records to MongoDB collections.
/// Supports insert, upsert, and replace write modes with configurable document ID strategies.
/// </summary>
public sealed class MongoDbSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(MongoDbSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(MongoDbConnectorConfig.ConnectionString, ConfigType.Password, Importance.High,
            "MongoDB connection string")
        .Define(MongoDbConnectorConfig.Database, ConfigType.String, Importance.High,
            "Database name")
        .Define(MongoDbConnectorConfig.Collection, ConfigType.String, Importance.High,
            "Target collection name")
        .Define(MongoDbConnectorConfig.Topics, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
        .Define(MongoDbConnectorConfig.WriteMode, ConfigType.String, MongoDbConnectorConfig.WriteModeInsert, Importance.Medium,
            "Write mode: 'insert', 'upsert', or 'replace'")
        .Define(MongoDbConnectorConfig.DocumentIdStrategy, ConfigType.String, MongoDbConnectorConfig.DocumentIdStrategyAuto, Importance.Medium,
            "Document ID strategy: 'auto', 'key', or 'field'")
        .Define(MongoDbConnectorConfig.DocumentIdField, ConfigType.String, MongoDbConnectorConfig.DefaultDocumentIdField, Importance.Medium,
            "Field name for document ID when strategy is 'field'")
        .Define(MongoDbConnectorConfig.BatchSize, ConfigType.Int, (long)MongoDbConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to buffer before flushing")
        .Define(MongoDbConnectorConfig.WriteConcern, ConfigType.String, MongoDbConnectorConfig.WriteConcernMajority, Importance.Medium,
            "Write concern: 'w1', 'majority', or 'unacknowledged'")
        .Define(MongoDbConnectorConfig.RetryMax, ConfigType.Int, (long)MongoDbConnectorConfig.DefaultRetryMax, Importance.Medium,
            "Maximum retry attempts on failure")
        .Define(MongoDbConnectorConfig.RetryBackoffMs, ConfigType.Long, MongoDbConnectorConfig.DefaultRetryBackoffMs, Importance.Medium,
            "Backoff time between retries in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(MongoDbConnectorConfig.ConnectionString, out _))
            throw new ArgumentException($"Missing required config: {MongoDbConnectorConfig.ConnectionString}");

        if (!config.TryGetValue(MongoDbConnectorConfig.Database, out _))
            throw new ArgumentException($"Missing required config: {MongoDbConnectorConfig.Database}");

        if (!config.TryGetValue(MongoDbConnectorConfig.Collection, out _))
            throw new ArgumentException($"Missing required config: {MongoDbConnectorConfig.Collection}");

        if (!config.TryGetValue(MongoDbConnectorConfig.Topics, out _))
            throw new ArgumentException($"Missing required config: {MongoDbConnectorConfig.Topics}");

        // Validate write mode
        var writeMode = config.TryGetValue(MongoDbConnectorConfig.WriteMode, out var mode)
            ? mode
            : MongoDbConnectorConfig.WriteModeInsert;

        if (writeMode is not (MongoDbConnectorConfig.WriteModeInsert or MongoDbConnectorConfig.WriteModeUpsert or MongoDbConnectorConfig.WriteModeReplace))
            throw new ArgumentException($"Invalid write mode '{writeMode}'. Must be '{MongoDbConnectorConfig.WriteModeInsert}', '{MongoDbConnectorConfig.WriteModeUpsert}', or '{MongoDbConnectorConfig.WriteModeReplace}'");

        // Validate document ID strategy
        var docIdStrategy = config.TryGetValue(MongoDbConnectorConfig.DocumentIdStrategy, out var strategy)
            ? strategy
            : MongoDbConnectorConfig.DocumentIdStrategyAuto;

        if (docIdStrategy is not (MongoDbConnectorConfig.DocumentIdStrategyAuto or MongoDbConnectorConfig.DocumentIdStrategyKey or MongoDbConnectorConfig.DocumentIdStrategyField))
            throw new ArgumentException($"Invalid document ID strategy '{docIdStrategy}'. Must be '{MongoDbConnectorConfig.DocumentIdStrategyAuto}', '{MongoDbConnectorConfig.DocumentIdStrategyKey}', or '{MongoDbConnectorConfig.DocumentIdStrategyField}'");

        // Validate write concern
        var writeConcern = config.TryGetValue(MongoDbConnectorConfig.WriteConcern, out var wc)
            ? wc
            : MongoDbConnectorConfig.WriteConcernMajority;

        if (writeConcern is not (MongoDbConnectorConfig.WriteConcernW1 or MongoDbConnectorConfig.WriteConcernMajority or MongoDbConnectorConfig.WriteConcernUnacknowledged))
            throw new ArgumentException($"Invalid write concern '{writeConcern}'. Must be '{MongoDbConnectorConfig.WriteConcernW1}', '{MongoDbConnectorConfig.WriteConcernMajority}', or '{MongoDbConnectorConfig.WriteConcernUnacknowledged}'");

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
