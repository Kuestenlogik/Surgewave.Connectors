using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.InfluxDB;

/// <summary>
/// Source connector that reads time-series data from InfluxDB using Flux queries.
/// Supports measurement polling and custom Flux query modes.
/// </summary>
public sealed class InfluxDBSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(InfluxDBSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(InfluxDBConnectorConfig.UrlConfig, ConfigType.String, Importance.High, "InfluxDB server URL (e.g., http://localhost:8086)")
        .Define(InfluxDBConnectorConfig.TokenConfig, ConfigType.Password, Importance.High, "InfluxDB API token")
        .Define(InfluxDBConnectorConfig.OrgConfig, ConfigType.String, Importance.High, "InfluxDB organization")
        .Define(InfluxDBConnectorConfig.BucketConfig, ConfigType.String, Importance.High, "InfluxDB bucket")
        .Define(InfluxDBConnectorConfig.MeasurementConfig, ConfigType.String, "", Importance.Medium, "Measurement name (optional if using custom query)")
        .Define(InfluxDBConnectorConfig.QueryConfig, ConfigType.String, "", Importance.Medium, "Custom Flux query (overrides measurement)", EditorHint.Code, "sql")
        .Define(InfluxDBConnectorConfig.TopicPatternConfig, ConfigType.String, InfluxDBConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern")
        .Define(InfluxDBConnectorConfig.PollIntervalMsConfig, ConfigType.Int, (int)InfluxDBConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in milliseconds")
        .Define(InfluxDBConnectorConfig.MaxRowsPerPollConfig, ConfigType.Int, InfluxDBConnectorConfig.DefaultMaxRowsPerPoll, Importance.Low, "Max rows per poll")
        .Define(InfluxDBConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, true, Importance.Low, "Include InfluxDB metadata in output")
        .Define(InfluxDBConnectorConfig.TimeRangeConfig, ConfigType.String, InfluxDBConnectorConfig.DefaultTimeRange, Importance.Low, "Time range for queries (e.g., -1h, -24h, -7d)")
        .Define(InfluxDBConnectorConfig.StartTimeConfig, ConfigType.String, "", Importance.Low, "Start time for queries (ISO 8601 format)")
        .Define(InfluxDBConnectorConfig.StopTimeConfig, ConfigType.String, "", Importance.Low, "Stop time for queries (ISO 8601 format)");

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

        // Either measurement or custom query must be provided
        var measurement = GetConfigValue(config, InfluxDBConnectorConfig.MeasurementConfig, "");
        var query = GetConfigValue(config, InfluxDBConnectorConfig.QueryConfig, "");

        if (string.IsNullOrEmpty(measurement) && string.IsNullOrEmpty(query))
            throw new ArgumentException($"Either '{InfluxDBConnectorConfig.MeasurementConfig}' or '{InfluxDBConnectorConfig.QueryConfig}' must be provided");

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
        // Single task - InfluxDB handles distributed queries internally
        return [new Dictionary<string, string>(_config)];
    }
}
