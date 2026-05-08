namespace Kuestenlogik.Surgewave.Connector.MongoDB;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A source connector that captures changes from MongoDB using Change Streams or polling.
/// Supports real-time CDC via Change Streams (MongoDB 3.6+) or incremental polling.
/// </summary>
public sealed class MongoDbSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(MongoDbSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(MongoDbConnectorConfig.ConnectionString, ConfigType.Password, Importance.High,
            "MongoDB connection string")
        .Define(MongoDbConnectorConfig.Database, ConfigType.String, Importance.High,
            "Database name")
        .Define(MongoDbConnectorConfig.Collection, ConfigType.String, Importance.High,
            "Collection name (or * for all collections)")
        .Define(MongoDbConnectorConfig.SourceMode, ConfigType.String, MongoDbConnectorConfig.SourceModeChangeStream, Importance.High,
            "Source mode: 'change_stream' or 'poll'")
        .Define(MongoDbConnectorConfig.TopicPrefix, ConfigType.String, "", Importance.Medium,
            "Prefix for generated topic names", EditorHint.Topic)
        .Define(MongoDbConnectorConfig.TopicPattern, ConfigType.String, MongoDbConnectorConfig.DefaultTopicPattern, Importance.Medium,
            "Topic naming pattern (supports ${database} and ${collection})", EditorHint.Topic)
        .Define(MongoDbConnectorConfig.ChangeStreamFullDocument, ConfigType.String, MongoDbConnectorConfig.FullDocumentUpdateLookup, Importance.Medium,
            "Full document mode: 'default', 'updateLookup', or 'whenAvailable'")
        .Define(MongoDbConnectorConfig.PollField, ConfigType.String, MongoDbConnectorConfig.DefaultPollField, Importance.Medium,
            "Field to track for polling (ObjectId or timestamp field)")
        .Define(MongoDbConnectorConfig.PollIntervalMs, ConfigType.Long, MongoDbConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Polling interval in milliseconds")
        .Define(MongoDbConnectorConfig.BatchMaxRecords, ConfigType.Int, (long)MongoDbConnectorConfig.DefaultBatchMaxRecords, Importance.Medium,
            "Maximum records per poll")
        .Define(MongoDbConnectorConfig.Pipeline, ConfigType.String, "", Importance.Low,
            "Optional aggregation pipeline JSON for filtering changes");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(MongoDbConnectorConfig.ConnectionString, out _))
            throw new ArgumentException($"Missing required config: {MongoDbConnectorConfig.ConnectionString}");

        if (!config.TryGetValue(MongoDbConnectorConfig.Database, out _))
            throw new ArgumentException($"Missing required config: {MongoDbConnectorConfig.Database}");

        if (!config.TryGetValue(MongoDbConnectorConfig.Collection, out _))
            throw new ArgumentException($"Missing required config: {MongoDbConnectorConfig.Collection}");

        // Validate source mode
        var sourceMode = config.TryGetValue(MongoDbConnectorConfig.SourceMode, out var mode)
            ? mode
            : MongoDbConnectorConfig.SourceModeChangeStream;

        if (sourceMode is not (MongoDbConnectorConfig.SourceModeChangeStream or MongoDbConnectorConfig.SourceModePoll))
            throw new ArgumentException($"Invalid source mode '{sourceMode}'. Must be '{MongoDbConnectorConfig.SourceModeChangeStream}' or '{MongoDbConnectorConfig.SourceModePoll}'");

        // Validate full document mode
        var fullDocMode = config.TryGetValue(MongoDbConnectorConfig.ChangeStreamFullDocument, out var fdm)
            ? fdm
            : MongoDbConnectorConfig.FullDocumentUpdateLookup;

        if (fullDocMode is not (MongoDbConnectorConfig.FullDocumentDefault or MongoDbConnectorConfig.FullDocumentUpdateLookup or MongoDbConnectorConfig.FullDocumentWhenAvailable))
            throw new ArgumentException($"Invalid full document mode '{fullDocMode}'. Must be '{MongoDbConnectorConfig.FullDocumentDefault}', '{MongoDbConnectorConfig.FullDocumentUpdateLookup}', or '{MongoDbConnectorConfig.FullDocumentWhenAvailable}'");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Change streams and polling both use a single cursor
        // So we always return a single task config
        return [new Dictionary<string, string>(_config)];
    }
}
