using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.InfluxDB;

/// <summary>
/// Sink connector that writes time-series data to InfluxDB using line protocol.
/// Supports batch writes with configurable precision.
/// </summary>
public sealed class InfluxDBSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(InfluxDBSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(InfluxDBConnectorConfig.UrlConfig, ConfigType.String, Importance.High, "InfluxDB server URL (e.g., http://localhost:8086)")
        .Define(InfluxDBConnectorConfig.TokenConfig, ConfigType.Password, Importance.High, "InfluxDB API token")
        .Define(InfluxDBConnectorConfig.OrgConfig, ConfigType.String, Importance.High, "InfluxDB organization")
        .Define(InfluxDBConnectorConfig.BucketConfig, ConfigType.String, Importance.High, "InfluxDB bucket")
        .Define(InfluxDBConnectorConfig.MeasurementConfig, ConfigType.String, "", Importance.High, "Default measurement name (can be overridden by field)")
        .Define(InfluxDBConnectorConfig.TopicsConfig, ConfigType.String, Importance.High, "Comma-separated list of topics to consume", EditorHint.Topic)
        .Define(InfluxDBConnectorConfig.BatchSizeConfig, ConfigType.Int, InfluxDBConnectorConfig.DefaultBatchSize, Importance.Low, "Batch size for writes")
        .Define(InfluxDBConnectorConfig.MaxRetryCountConfig, ConfigType.Int, InfluxDBConnectorConfig.DefaultMaxRetryCount, Importance.Low, "Maximum retry attempts")
        .Define(InfluxDBConnectorConfig.RetryDelayMsConfig, ConfigType.Int, (int)InfluxDBConnectorConfig.DefaultRetryDelayMs, Importance.Low, "Delay between retries in milliseconds")
        .Define(InfluxDBConnectorConfig.MeasurementFieldConfig, ConfigType.String, "", Importance.Low, "Field to use as measurement name")
        .Define(InfluxDBConnectorConfig.TimestampFieldConfig, ConfigType.String, "", Importance.Low, "Field to use as timestamp")
        .Define(InfluxDBConnectorConfig.TagFieldsConfig, ConfigType.String, "", Importance.Low, "Comma-separated fields to use as tags")
        .Define(InfluxDBConnectorConfig.FieldFieldsConfig, ConfigType.String, "", Importance.Low, "Comma-separated fields to use as values (empty = all non-tag fields)")
        .Define(InfluxDBConnectorConfig.PrecisionConfig, ConfigType.String, InfluxDBConnectorConfig.DefaultPrecision, Importance.Low, "Write precision: ns, us, ms, s", EditorHint.Select, options: ["ns", "us", "ms", "s"]);

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(InfluxDBConnectorConfig.UrlConfig, out var url) || string.IsNullOrEmpty(url))
            throw new ArgumentException($"Required configuration '{InfluxDBConnectorConfig.UrlConfig}' is missing");

        if (!config.TryGetValue(InfluxDBConnectorConfig.TokenConfig, out var token) || string.IsNullOrEmpty(token))
            throw new ArgumentException($"Required configuration '{InfluxDBConnectorConfig.TokenConfig}' is missing");

        if (!config.TryGetValue(InfluxDBConnectorConfig.OrgConfig, out var org) || string.IsNullOrEmpty(org))
            throw new ArgumentException($"Required configuration '{InfluxDBConnectorConfig.OrgConfig}' is missing");

        if (!config.TryGetValue(InfluxDBConnectorConfig.BucketConfig, out var bucket) || string.IsNullOrEmpty(bucket))
            throw new ArgumentException($"Required configuration '{InfluxDBConnectorConfig.BucketConfig}' is missing");

        if (!config.TryGetValue(InfluxDBConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{InfluxDBConnectorConfig.TopicsConfig}' is missing");

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
        // Single task - InfluxDB handles batching internally
        return [new Dictionary<string, string>(_config)];
    }
}
