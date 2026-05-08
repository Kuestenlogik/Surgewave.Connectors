namespace Kuestenlogik.Surgewave.Connector.Batching;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that applies batching policies to aggregate messages before
/// forwarding them to downstream processing.
/// </summary>
public sealed class BatchingSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(BatchingSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Topics
        .Define(BatchingConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
        // Batching policy
        .Define(BatchingConnectorConfig.BatchMaxMessagesConfig, ConfigType.Int, (long)BatchingConnectorConfig.DefaultBatchMaxMessages, Importance.High,
            "Maximum number of messages per batch")
        .Define(BatchingConnectorConfig.BatchMaxBytesConfig, ConfigType.Int, BatchingConnectorConfig.DefaultBatchMaxBytes, Importance.Medium,
            "Maximum batch size in bytes")
        .Define(BatchingConnectorConfig.BatchTimeoutMsConfig, ConfigType.Int, (long)BatchingConnectorConfig.DefaultBatchTimeoutMs, Importance.Medium,
            "Maximum time to wait for batch completion in milliseconds")
        // Batch format
        .Define(BatchingConnectorConfig.BatchFormatConfig, ConfigType.String, BatchingConnectorConfig.DefaultBatchFormat, Importance.Medium,
            "Output format: 'json-array' (default), 'json-lines', or 'raw'", EditorHint.Select, options: ["json-array", "json-lines", "raw"])
        // Key handling
        .Define(BatchingConnectorConfig.KeyStrategyConfig, ConfigType.String, BatchingConnectorConfig.DefaultKeyStrategy, Importance.Low,
            "Key strategy: 'first' (default), 'last', 'null', or 'concat'", EditorHint.Select, options: ["first", "last", "null", "concat"])
        // Metadata
        .Define(BatchingConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, BatchingConnectorConfig.DefaultIncludeMetadata, Importance.Low,
            "Include message metadata in batched output")
        // Separator
        .Define(BatchingConnectorConfig.SeparatorConfig, ConfigType.String, BatchingConnectorConfig.DefaultSeparator, Importance.Low,
            "Separator for JSON-lines and concat key strategies")
        // Flush on key change
        .Define(BatchingConnectorConfig.FlushOnKeyChangeConfig, ConfigType.Boolean, BatchingConnectorConfig.DefaultFlushOnKeyChange, Importance.Low,
            "Flush batch when message key changes")
        // Compression
        .Define(BatchingConnectorConfig.CompressionConfig, ConfigType.String, BatchingConnectorConfig.DefaultCompression, Importance.Low,
            "Compression: 'none' (default) or 'gzip'", EditorHint.Select, options: ["none", "gzip"]);

    public override void Start(IDictionary<string, string> config)
    {
        // Validate topics
        if (!config.TryGetValue(BatchingConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {BatchingConnectorConfig.TopicsConfig}");

        // Validate batch format
        if (config.TryGetValue(BatchingConnectorConfig.BatchFormatConfig, out var format))
        {
            var validFormats = new[]
            {
                BatchingConnectorConfig.FormatJsonArray,
                BatchingConnectorConfig.FormatJsonLines,
                BatchingConnectorConfig.FormatRaw
            };

            if (!validFormats.Contains(format))
                throw new ArgumentException($"Invalid batch format: {format}. Must be one of: {string.Join(", ", validFormats)}");
        }

        // Validate key strategy
        if (config.TryGetValue(BatchingConnectorConfig.KeyStrategyConfig, out var keyStrategy))
        {
            var validStrategies = new[]
            {
                BatchingConnectorConfig.KeyStrategyFirst,
                BatchingConnectorConfig.KeyStrategyLast,
                BatchingConnectorConfig.KeyStrategyNull,
                BatchingConnectorConfig.KeyStrategyConcat
            };

            if (!validStrategies.Contains(keyStrategy))
                throw new ArgumentException($"Invalid key strategy: {keyStrategy}. Must be one of: {string.Join(", ", validStrategies)}");
        }

        // Validate compression
        if (config.TryGetValue(BatchingConnectorConfig.CompressionConfig, out var compression))
        {
            var validCompressions = new[]
            {
                BatchingConnectorConfig.CompressionNone,
                BatchingConnectorConfig.CompressionGzip
            };

            if (!validCompressions.Contains(compression))
                throw new ArgumentException($"Invalid compression: {compression}. Must be one of: {string.Join(", ", validCompressions)}");
        }

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
