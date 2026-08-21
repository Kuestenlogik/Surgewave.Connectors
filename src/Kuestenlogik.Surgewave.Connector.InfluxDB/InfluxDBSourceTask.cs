using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.Json;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.InfluxDB;

/// <summary>
/// Task that reads time-series data from InfluxDB using Flux queries.
/// Supports incremental polling with timestamp-based tracking.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Client disposed in Stop()")]
public sealed class InfluxDBSourceTask : SourceTask
{
    /// <summary>RFC3339 with tick precision - accepted by Flux and round-trips through offset storage.</summary>
    private const string TimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'";

    public override string Version => "1.0.0";

    private InfluxDBClient? _client;
    private string _org = "";
    private string _bucket = "";
    private string _measurement = "";
    private string _customQuery = "";
    private string _topicPattern = InfluxDBConnectorConfig.DefaultTopicPattern;
    private long _pollIntervalMs = InfluxDBConnectorConfig.DefaultPollIntervalMs;
    private int _maxRowsPerPoll = InfluxDBConnectorConfig.DefaultMaxRowsPerPoll;
    private bool _includeMetadata = true;
    private string _timeRange = InfluxDBConnectorConfig.DefaultTimeRange;
    private string _startTime = "";
    private string _stopTime = "";

    private DateTimeOffset? _lastTimestamp;
    private DateTime _lastPollTime = DateTime.MinValue;
    private IDictionary<string, object> _sourcePartition = new Dictionary<string, object>();

    public override void Start(IDictionary<string, string> config)
    {
        var url = config[InfluxDBConnectorConfig.UrlConfig];
        var token = config[InfluxDBConnectorConfig.TokenConfig];
        _org = config[InfluxDBConnectorConfig.OrgConfig];
        _bucket = config[InfluxDBConnectorConfig.BucketConfig];
        _measurement = GetConfigValue(config, InfluxDBConnectorConfig.MeasurementConfig, "");
        _customQuery = GetConfigValue(config, InfluxDBConnectorConfig.QueryConfig, "");
        _topicPattern = GetConfigValue(config, InfluxDBConnectorConfig.TopicPatternConfig, InfluxDBConnectorConfig.DefaultTopicPattern);
        _pollIntervalMs = long.Parse(GetConfigValue(config, InfluxDBConnectorConfig.PollIntervalMsConfig, InfluxDBConnectorConfig.DefaultPollIntervalMs.ToString()));
        _maxRowsPerPoll = int.Parse(GetConfigValue(config, InfluxDBConnectorConfig.MaxRowsPerPollConfig, InfluxDBConnectorConfig.DefaultMaxRowsPerPoll.ToString()));
        _includeMetadata = bool.Parse(GetConfigValue(config, InfluxDBConnectorConfig.IncludeMetadataConfig, "true"));
        _timeRange = GetConfigValue(config, InfluxDBConnectorConfig.TimeRangeConfig, InfluxDBConnectorConfig.DefaultTimeRange);
        _startTime = GetConfigValue(config, InfluxDBConnectorConfig.StartTimeConfig, "");
        _stopTime = GetConfigValue(config, InfluxDBConnectorConfig.StopTimeConfig, "");

        _sourcePartition = new Dictionary<string, object>
        {
            [InfluxDBConnectorConfig.OffsetBucket] = _bucket,
            [InfluxDBConnectorConfig.OffsetMeasurement] = _measurement
        };

        _client = new InfluxDBClient(url, token);

        RestoreOffset();
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private void RestoreOffset()
    {
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return;

        // Resume from the last emitted point instead of replaying the whole configured range
        if (storedOffset.TryGetValue(InfluxDBConnectorConfig.OffsetTimestamp, out var timestamp) && timestamp != null &&
            DateTimeOffset.TryParse(timestamp.ToString(), CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var parsed))
        {
            _lastTimestamp = parsed;
        }
    }

    private static string FormatTimestamp(DateTimeOffset timestamp)
        => timestamp.ToUniversalTime().ToString(TimestampFormat, CultureInfo.InvariantCulture);

    public override void Stop()
    {
        _client?.Dispose();
        _client = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_client == null)
            return [];

        // Respect poll interval
        var elapsed = DateTime.UtcNow - _lastPollTime;
        if (elapsed.TotalMilliseconds < _pollIntervalMs)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs - elapsed.TotalMilliseconds), cancellationToken);
        }

        _lastPollTime = DateTime.UtcNow;

        var records = new List<SourceRecord>();

        try
        {
            var query = BuildQuery();
            var queryApi = _client.GetQueryApi();
            var tables = await queryApi.QueryAsync(query, _org, cancellationToken);

            foreach (var table in tables)
            {
                foreach (var record in table.Records)
                {
                    var sourceRecord = CreateSourceRecord(record);
                    records.Add(sourceRecord);

                    // Track timestamp for incremental polling
                    if (record.GetTime() != null)
                    {
                        var timestamp = record.GetTimeInDateTime();
                        if (timestamp.HasValue)
                        {
                            var offset = new DateTimeOffset(timestamp.Value, TimeSpan.Zero);
                            if (!_lastTimestamp.HasValue || offset > _lastTimestamp.Value)
                                _lastTimestamp = offset;
                        }
                    }

                    if (records.Count >= _maxRowsPerPoll)
                        break;
                }

                if (records.Count >= _maxRowsPerPoll)
                    break;
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Surface query failures (bad token, unreachable server, invalid Flux) instead of
            // silently returning an empty batch forever; the next poll retries.
            Context?.RaiseError?.Invoke(ex);
        }

        return records;
    }

    internal string BuildQuery()
    {
        if (!string.IsNullOrEmpty(_customQuery))
        {
            return _customQuery;
        }

        var sb = new StringBuilder();

        // Build Flux query
        sb.AppendLine($"from(bucket: \"{_bucket}\")");

        // Time range
        if (!string.IsNullOrEmpty(_startTime) && !string.IsNullOrEmpty(_stopTime))
        {
            sb.AppendLine($"  |> range(start: {_startTime}, stop: {_stopTime})");
        }
        else if (_lastTimestamp.HasValue)
        {
            // Incremental polling from last timestamp. Flux 'range' start is inclusive, so start
            // one tick past the newest emitted point - otherwise every poll re-emits it as a duplicate.
            sb.AppendLine($"  |> range(start: {FormatTimestamp(_lastTimestamp.Value.AddTicks(1))})");
        }
        else
        {
            sb.AppendLine($"  |> range(start: {_timeRange})");
        }

        // Measurement filter
        if (!string.IsNullOrEmpty(_measurement))
        {
            sb.AppendLine($"  |> filter(fn: (r) => r._measurement == \"{_measurement}\")");
        }

        // Limit results
        sb.AppendLine($"  |> limit(n: {_maxRowsPerPoll})");

        return sb.ToString();
    }

    private SourceRecord CreateSourceRecord(global::InfluxDB.Client.Core.Flux.Domain.FluxRecord record)
    {
        var rowDict = new Dictionary<string, object?>();

        // Add all values
        foreach (var kvp in record.Values)
        {
            rowDict[kvp.Key] = kvp.Value;
        }

        // Extract measurement for topic
        var measurement = record.GetMeasurement() ?? _measurement;
        var topic = BuildTopic(measurement);
        var key = BuildKey(record);
        var recordValue = JsonSerializer.SerializeToUtf8Bytes(rowDict, JsonSerializerOptions);
        var headers = BuildHeaders(record, measurement);

        var sourceOffset = new Dictionary<string, object>();
        var recordTime = record.GetTimeInDateTime();
        if (recordTime.HasValue)
        {
            // Tick precision so the restored cursor is exactly this record's timestamp
            sourceOffset[InfluxDBConnectorConfig.OffsetTimestamp] =
                FormatTimestamp(new DateTimeOffset(recordTime.Value, TimeSpan.Zero));
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = sourceOffset,
            Topic = topic,
            Key = key,
            Value = recordValue,
            Timestamp = DateTimeOffset.UtcNow,
            Headers = headers
        };
    }

    private string BuildTopic(string measurement)
    {
        return _topicPattern
            .Replace("${org}", _org)
            .Replace("${bucket}", _bucket)
            .Replace("${measurement}", measurement);
    }

    private static byte[]? BuildKey(global::InfluxDB.Client.Core.Flux.Domain.FluxRecord record)
    {
        // Use measurement + tags as key
        var sb = new StringBuilder();
        sb.Append(record.GetMeasurement() ?? "");

        // Add tag values to key
        foreach (var kv in record.Values)
        {
            // Tags typically don't start with underscore
            if (!kv.Key.StartsWith('_') && kv.Value != null)
            {
                sb.Append(':');
                sb.Append(kv.Value.ToString());
            }
        }

        return sb.Length > 0 ? Encoding.UTF8.GetBytes(sb.ToString()) : null;
    }

    private Dictionary<string, byte[]> BuildHeaders(global::InfluxDB.Client.Core.Flux.Domain.FluxRecord record, string measurement)
    {
        var headers = new Dictionary<string, byte[]>();

        if (_includeMetadata)
        {
            headers[InfluxDBConnectorConfig.HeaderOrg] = Encoding.UTF8.GetBytes(_org);
            headers[InfluxDBConnectorConfig.HeaderBucket] = Encoding.UTF8.GetBytes(_bucket);
            headers[InfluxDBConnectorConfig.HeaderMeasurement] = Encoding.UTF8.GetBytes(measurement);

            if (record.GetTime() != null)
            {
                headers[InfluxDBConnectorConfig.HeaderTimestamp] = Encoding.UTF8.GetBytes(record.GetTime()!.ToString()!);
            }

            // Extract tags
            var tags = new Dictionary<string, string>();
            foreach (var kv in record.Values)
            {
                if (!kv.Key.StartsWith('_') && kv.Value != null)
                {
                    tags[kv.Key] = kv.Value.ToString()!;
                }
            }
            if (tags.Count > 0)
            {
                headers[InfluxDBConnectorConfig.HeaderTags] = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(tags));
            }
        }

        return headers;
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Nothing to acknowledge upstream: every record carries its own cursor in SourceOffset
        // and Start() restores it from offset storage, so a restart resumes where this task stopped.
        return Task.CompletedTask;
    }

    public override void CommitRecord(SourceRecord record, RecordMetadata metadata)
    {
        // Individual record commit - nothing to do
    }

    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };
}
