namespace Kuestenlogik.Surgewave.Connector.InfluxDB;

/// <summary>
/// Configuration constants for InfluxDB connectors.
/// </summary>
public static class InfluxDBConnectorConfig
{
    // Connection configs
    public const string UrlConfig = "influxdb.url";
    public const string TokenConfig = "influxdb.token";
    public const string OrgConfig = "influxdb.org";
    public const string BucketConfig = "influxdb.bucket";

    // Source configs
    public const string TopicPatternConfig = "influxdb.topic.pattern";
    public const string PollIntervalMsConfig = "influxdb.poll.interval.ms";
    public const string MaxRowsPerPollConfig = "influxdb.max.rows.per.poll";
    public const string IncludeMetadataConfig = "influxdb.include.metadata";
    public const string QueryConfig = "influxdb.query";
    public const string MeasurementConfig = "influxdb.measurement";
    public const string StartTimeConfig = "influxdb.start.time";
    public const string StopTimeConfig = "influxdb.stop.time";
    public const string TimeRangeConfig = "influxdb.time.range"; // e.g., "-1h", "-24h", "-7d"

    // Sink configs
    public const string TopicsConfig = "topics";
    public const string BatchSizeConfig = "influxdb.batch.size";
    public const string MaxRetryCountConfig = "influxdb.max.retry.count";
    public const string RetryDelayMsConfig = "influxdb.retry.delay.ms";
    public const string MeasurementFieldConfig = "influxdb.measurement.field";
    public const string TimestampFieldConfig = "influxdb.timestamp.field";
    public const string TagFieldsConfig = "influxdb.tag.fields";
    public const string FieldFieldsConfig = "influxdb.field.fields";
    public const string PrecisionConfig = "influxdb.precision";

    // Default values
    public const string DefaultTopicPattern = "influxdb.${org}.${bucket}.${measurement}";
    public const long DefaultPollIntervalMs = 10000;
    public const int DefaultMaxRowsPerPoll = 10000;
    public const int DefaultBatchSize = 5000;
    public const int DefaultMaxRetryCount = 3;
    public const long DefaultRetryDelayMs = 1000;
    public const string DefaultTimeRange = "-1h";
    public const string DefaultPrecision = "ns";

    // Header names
    public const string HeaderOrg = "influxdb.org";
    public const string HeaderBucket = "influxdb.bucket";
    public const string HeaderMeasurement = "influxdb.measurement";
    public const string HeaderTimestamp = "influxdb.timestamp";
    public const string HeaderTags = "influxdb.tags";

    // Offset tracking
    public const string OffsetTimestamp = "timestamp";
    public const string OffsetMeasurement = "measurement";
    public const string OffsetBucket = "bucket";
}
