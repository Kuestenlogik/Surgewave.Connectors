namespace Kuestenlogik.Surgewave.Connector.Elasticsearch;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that writes records to Elasticsearch.
/// Supports multiple indexing strategies and document ID generation options.
/// </summary>
public sealed class ElasticsearchSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(ElasticsearchSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Connection configs
        .Define(ElasticsearchConnectorConfig.UrlConfig, ConfigType.Password, Importance.High,
            "Elasticsearch URL(s), comma-separated (e.g., 'https://localhost:9200')")
        .Define(ElasticsearchConnectorConfig.ApiKeyConfig, ConfigType.Password, "", Importance.High,
            "API key for authentication (recommended)")
        .Define(ElasticsearchConnectorConfig.UsernameConfig, ConfigType.String, "", Importance.Medium,
            "Username for basic authentication")
        .Define(ElasticsearchConnectorConfig.PasswordConfig, ConfigType.Password, "", Importance.Medium,
            "Password for basic authentication")
        .Define(ElasticsearchConnectorConfig.CloudIdConfig, ConfigType.String, "", Importance.Medium,
            "Elastic Cloud ID (alternative to URL)")
        // Topics
        .Define(ElasticsearchConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
        // Indexing configs
        .Define(ElasticsearchConnectorConfig.IndexConfig, ConfigType.String, ElasticsearchConnectorConfig.DefaultIndexPattern, Importance.High,
            "Index name pattern (supports ${topic} substitution)")
        .Define(ElasticsearchConnectorConfig.IndexStrategyConfig, ConfigType.String, ElasticsearchConnectorConfig.IndexStrategyTopic, Importance.High,
            "Indexing strategy: 'static', 'topic', 'time', or 'field'", EditorHint.Select, options: ["static", "topic", "time", "field"])
        .Define(ElasticsearchConnectorConfig.IndexTimeFormatConfig, ConfigType.String, ElasticsearchConnectorConfig.DefaultTimeFormat, Importance.Medium,
            "Date format for time-based indexing (e.g., 'yyyy.MM.dd')")
        .Define(ElasticsearchConnectorConfig.IndexFieldConfig, ConfigType.String, "", Importance.Medium,
            "Field name for field-based indexing")
        // Document ID configs
        .Define(ElasticsearchConnectorConfig.DocumentIdStrategyConfig, ConfigType.String, ElasticsearchConnectorConfig.DocIdStrategyAuto, Importance.High,
            "Document ID strategy: 'auto', 'key', 'field', or 'composite'", EditorHint.Select, options: ["auto", "key", "field", "composite"])
        .Define(ElasticsearchConnectorConfig.DocumentIdFieldConfig, ConfigType.String, "", Importance.Medium,
            "Field name for document ID extraction")
        .Define(ElasticsearchConnectorConfig.DocumentIdCompositeFieldsConfig, ConfigType.String, "", Importance.Medium,
            "Comma-separated field names for composite document ID")
        .Define(ElasticsearchConnectorConfig.DocumentIdCompositeDelimiterConfig, ConfigType.String, ElasticsearchConnectorConfig.DefaultCompositeDelimiter, Importance.Low,
            "Delimiter for composite document IDs")
        // Write behavior
        .Define(ElasticsearchConnectorConfig.WriteMethodConfig, ConfigType.String, ElasticsearchConnectorConfig.WriteMethodIndex, Importance.Medium,
            "Write method: 'index', 'create', or 'upsert'", EditorHint.Select, options: ["index", "create", "upsert"])
        .Define(ElasticsearchConnectorConfig.BehaviorOnMalformedConfig, ConfigType.String, ElasticsearchConnectorConfig.BehaviorWarn, Importance.Medium,
            "Behavior on malformed documents: 'ignore', 'warn', or 'fail'", EditorHint.Select, options: ["ignore", "warn", "fail"])
        // Batching and retry
        .Define(ElasticsearchConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)ElasticsearchConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to buffer before flushing")
        .Define(ElasticsearchConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)ElasticsearchConnectorConfig.DefaultRetryMax, Importance.Medium,
            "Maximum retry attempts on failure")
        .Define(ElasticsearchConnectorConfig.RetryBackoffMsConfig, ConfigType.Long, ElasticsearchConnectorConfig.DefaultRetryBackoffMs, Importance.Medium,
            "Backoff time between retries in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate: URL or CloudId required
        var hasUrl = config.TryGetValue(ElasticsearchConnectorConfig.UrlConfig, out var url) && !string.IsNullOrEmpty(url);
        var hasCloudId = config.TryGetValue(ElasticsearchConnectorConfig.CloudIdConfig, out var cloudId) && !string.IsNullOrEmpty(cloudId);

        if (!hasUrl && !hasCloudId)
            throw new ArgumentException($"Either '{ElasticsearchConnectorConfig.UrlConfig}' or '{ElasticsearchConnectorConfig.CloudIdConfig}' must be specified");

        // Validate topics
        if (!config.TryGetValue(ElasticsearchConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {ElasticsearchConnectorConfig.TopicsConfig}");

        // Validate index strategy
        var indexStrategy = config.TryGetValue(ElasticsearchConnectorConfig.IndexStrategyConfig, out var strategy)
            ? strategy
            : ElasticsearchConnectorConfig.IndexStrategyTopic;

        if (indexStrategy is not (ElasticsearchConnectorConfig.IndexStrategyStatic
            or ElasticsearchConnectorConfig.IndexStrategyTopic
            or ElasticsearchConnectorConfig.IndexStrategyTime
            or ElasticsearchConnectorConfig.IndexStrategyField))
        {
            throw new ArgumentException($"Invalid index strategy '{indexStrategy}'. Must be 'static', 'topic', 'time', or 'field'");
        }

        // Validate field-based indexing requires field name
        if (indexStrategy == ElasticsearchConnectorConfig.IndexStrategyField)
        {
            if (!config.TryGetValue(ElasticsearchConnectorConfig.IndexFieldConfig, out var field) || string.IsNullOrEmpty(field))
                throw new ArgumentException($"Field-based indexing requires '{ElasticsearchConnectorConfig.IndexFieldConfig}' to be specified");
        }

        // Validate document ID strategy
        var docIdStrategy = config.TryGetValue(ElasticsearchConnectorConfig.DocumentIdStrategyConfig, out var docStrategy)
            ? docStrategy
            : ElasticsearchConnectorConfig.DocIdStrategyAuto;

        if (docIdStrategy is not (ElasticsearchConnectorConfig.DocIdStrategyAuto
            or ElasticsearchConnectorConfig.DocIdStrategyKey
            or ElasticsearchConnectorConfig.DocIdStrategyField
            or ElasticsearchConnectorConfig.DocIdStrategyComposite))
        {
            throw new ArgumentException($"Invalid document ID strategy '{docIdStrategy}'. Must be 'auto', 'key', 'field', or 'composite'");
        }

        // Validate field-based doc ID requires field name
        if (docIdStrategy == ElasticsearchConnectorConfig.DocIdStrategyField)
        {
            if (!config.TryGetValue(ElasticsearchConnectorConfig.DocumentIdFieldConfig, out var field) || string.IsNullOrEmpty(field))
                throw new ArgumentException($"Field-based document ID requires '{ElasticsearchConnectorConfig.DocumentIdFieldConfig}' to be specified");
        }

        // Validate composite doc ID requires fields
        if (docIdStrategy == ElasticsearchConnectorConfig.DocIdStrategyComposite)
        {
            if (!config.TryGetValue(ElasticsearchConnectorConfig.DocumentIdCompositeFieldsConfig, out var fields) || string.IsNullOrEmpty(fields))
                throw new ArgumentException($"Composite document ID requires '{ElasticsearchConnectorConfig.DocumentIdCompositeFieldsConfig}' to be specified");
        }

        // Validate write method
        var writeMethod = config.TryGetValue(ElasticsearchConnectorConfig.WriteMethodConfig, out var method)
            ? method
            : ElasticsearchConnectorConfig.WriteMethodIndex;

        if (writeMethod is not (ElasticsearchConnectorConfig.WriteMethodIndex
            or ElasticsearchConnectorConfig.WriteMethodCreate
            or ElasticsearchConnectorConfig.WriteMethodUpsert))
        {
            throw new ArgumentException($"Invalid write method '{writeMethod}'. Must be 'index', 'create', or 'upsert'");
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
        // Single task for simplicity; could partition by topic for parallelism
        return [new Dictionary<string, string>(_config)];
    }
}
