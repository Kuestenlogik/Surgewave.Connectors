namespace Kuestenlogik.Surgewave.Connector.Elasticsearch;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A source connector that reads documents from Elasticsearch indices.
/// Supports scroll API and search_after pagination modes.
/// </summary>
public sealed class ElasticsearchSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(ElasticsearchSourceTask);

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
        // Topic
        .Define(ElasticsearchConnectorConfig.TopicConfig, ConfigType.String, Importance.High,
            "Destination Surgewave topic for produced records", EditorHint.Topic)
        // Index and query
        .Define(ElasticsearchConnectorConfig.IndexConfig, ConfigType.String, Importance.High,
            "Index or index pattern to read from (e.g., 'logs-*')")
        .Define(ElasticsearchConnectorConfig.QueryConfig, ConfigType.String, "*", Importance.High,
            "Query string or DSL JSON to filter documents", EditorHint.Code, "json")
        // Scrolling
        .Define(ElasticsearchConnectorConfig.ScrollModeConfig, ConfigType.String, ElasticsearchConnectorConfig.ScrollModeSearchAfter, Importance.Medium,
            "Scrolling mode: 'scroll' or 'search_after'")
        .Define(ElasticsearchConnectorConfig.ScrollSizeConfig, ConfigType.Int, (long)ElasticsearchConnectorConfig.DefaultScrollSize, Importance.Medium,
            "Number of documents per scroll request")
        .Define(ElasticsearchConnectorConfig.ScrollKeepAliveConfig, ConfigType.String, ElasticsearchConnectorConfig.DefaultScrollKeepAlive, Importance.Medium,
            "Scroll context keep-alive duration (e.g., '5m')")
        .Define(ElasticsearchConnectorConfig.SortFieldConfig, ConfigType.String, ElasticsearchConnectorConfig.DefaultSortField, Importance.Medium,
            "Sort field for deterministic ordering")
        // Polling
        .Define(ElasticsearchConnectorConfig.PollIntervalMsConfig, ConfigType.Long, ElasticsearchConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Poll interval in milliseconds")
        // Incremental mode
        .Define(ElasticsearchConnectorConfig.IncrementalModeConfig, ConfigType.String, ElasticsearchConnectorConfig.IncrementalModeNone, Importance.Medium,
            "Incremental mode: 'none' or 'timestamp'")
        .Define(ElasticsearchConnectorConfig.IncrementalFieldConfig, ConfigType.String, ElasticsearchConnectorConfig.DefaultIncrementalField, Importance.Medium,
            "Timestamp field for incremental polling")
        // Retry
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

        // Validate topic
        if (!config.TryGetValue(ElasticsearchConnectorConfig.TopicConfig, out _))
            throw new ArgumentException($"Missing required config: {ElasticsearchConnectorConfig.TopicConfig}");

        // Validate index
        if (!config.TryGetValue(ElasticsearchConnectorConfig.IndexConfig, out _))
            throw new ArgumentException($"Missing required config: {ElasticsearchConnectorConfig.IndexConfig}");

        // Validate scroll mode
        var scrollMode = config.TryGetValue(ElasticsearchConnectorConfig.ScrollModeConfig, out var mode)
            ? mode
            : ElasticsearchConnectorConfig.ScrollModeSearchAfter;

        if (scrollMode is not (ElasticsearchConnectorConfig.ScrollModeScroll or ElasticsearchConnectorConfig.ScrollModeSearchAfter))
        {
            throw new ArgumentException($"Invalid scroll mode '{scrollMode}'. Must be 'scroll' or 'search_after'");
        }

        // Validate incremental mode
        var incrementalMode = config.TryGetValue(ElasticsearchConnectorConfig.IncrementalModeConfig, out var incMode)
            ? incMode
            : ElasticsearchConnectorConfig.IncrementalModeNone;

        if (incrementalMode is not (ElasticsearchConnectorConfig.IncrementalModeNone or ElasticsearchConnectorConfig.IncrementalModeTimestamp))
        {
            throw new ArgumentException($"Invalid incremental mode '{incrementalMode}'. Must be 'none' or 'timestamp'");
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
        // Single task for simplicity; could partition by index shards for parallelism
        return [new Dictionary<string, string>(_config)];
    }
}
