namespace Kuestenlogik.Surgewave.Connector.Elasticsearch;

/// <summary>
/// Configuration constants shared between Elasticsearch source and sink connectors.
/// </summary>
internal static class ElasticsearchConnectorConfig
{
    // Connection configs
    public const string UrlConfig = "elasticsearch.url";
    public const string ApiKeyConfig = "elasticsearch.api.key";
    public const string UsernameConfig = "elasticsearch.username";
    public const string PasswordConfig = "elasticsearch.password";
    public const string CloudIdConfig = "elasticsearch.cloud.id";

    // Retry configs
    public const string RetryMaxConfig = "retry.max";
    public const string RetryBackoffMsConfig = "retry.backoff.ms";

    // Common sink/source configs
    public const string TopicsConfig = "topics";
    public const string TopicConfig = "topic";
    public const string IndexConfig = "elasticsearch.index";
    public const string BatchSizeConfig = "batch.size";

    // Sink-specific configs
    public const string IndexStrategyConfig = "elasticsearch.index.strategy";
    public const string IndexTimeFormatConfig = "elasticsearch.index.time.format";
    public const string IndexFieldConfig = "elasticsearch.index.field";
    public const string DocumentIdStrategyConfig = "elasticsearch.document.id.strategy";
    public const string DocumentIdFieldConfig = "elasticsearch.document.id.field";
    public const string DocumentIdCompositeFieldsConfig = "elasticsearch.document.id.composite.fields";
    public const string DocumentIdCompositeDelimiterConfig = "elasticsearch.document.id.composite.delimiter";
    public const string WriteMethodConfig = "elasticsearch.write.method";
    public const string BehaviorOnMalformedConfig = "behavior.on.malformed.documents";

    // Source-specific configs
    public const string QueryConfig = "elasticsearch.query";
    public const string ScrollModeConfig = "elasticsearch.scroll.mode";
    public const string ScrollSizeConfig = "elasticsearch.scroll.size";
    public const string ScrollKeepAliveConfig = "elasticsearch.scroll.keep.alive";
    public const string SortFieldConfig = "elasticsearch.sort.field";
    public const string PollIntervalMsConfig = "poll.interval.ms";
    public const string IncrementalModeConfig = "elasticsearch.incremental.mode";
    public const string IncrementalFieldConfig = "elasticsearch.incremental.field";

    // Index strategy values
    public const string IndexStrategyStatic = "static";
    public const string IndexStrategyTopic = "topic";
    public const string IndexStrategyTime = "time";
    public const string IndexStrategyField = "field";

    // Document ID strategy values
    public const string DocIdStrategyAuto = "auto";
    public const string DocIdStrategyKey = "key";
    public const string DocIdStrategyField = "field";
    public const string DocIdStrategyComposite = "composite";

    // Write method values
    public const string WriteMethodIndex = "index";
    public const string WriteMethodCreate = "create";
    public const string WriteMethodUpsert = "upsert";

    // Scroll mode values
    public const string ScrollModeScroll = "scroll";
    public const string ScrollModeSearchAfter = "search_after";

    // Incremental mode values
    public const string IncrementalModeNone = "none";
    public const string IncrementalModeTimestamp = "timestamp";

    // Behavior values
    public const string BehaviorIgnore = "ignore";
    public const string BehaviorWarn = "warn";
    public const string BehaviorFail = "fail";

    // Defaults
    public const int DefaultBatchSize = 100;
    public const int DefaultRetryMax = 3;
    public const long DefaultRetryBackoffMs = 1000;
    public const int DefaultScrollSize = 500;
    public const long DefaultPollIntervalMs = 5000;
    public const string DefaultIndexPattern = "${topic}";
    public const string DefaultTimeFormat = "yyyy.MM.dd";
    public const string DefaultCompositeDelimiter = "_";
    public const string DefaultScrollKeepAlive = "5m";
    public const string DefaultSortField = "_doc";
    public const string DefaultIncrementalField = "@timestamp";
}
