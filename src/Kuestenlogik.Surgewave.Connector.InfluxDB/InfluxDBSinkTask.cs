using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.InfluxDB;

/// <summary>
/// Task that writes time-series data to InfluxDB using line protocol.
/// Supports batch writes with configurable precision.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Client disposed in Stop()")]
public sealed class InfluxDBSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private InfluxDBClient? _client;
    private WriteApiAsync? _writeApi;
    private string _org = "";
    private string _bucket = "";
    private string _measurement = "";
    private int _batchSize = InfluxDBConnectorConfig.DefaultBatchSize;
    private int _maxRetryCount = InfluxDBConnectorConfig.DefaultMaxRetryCount;
    private long _retryDelayMs = InfluxDBConnectorConfig.DefaultRetryDelayMs;
    private string _measurementField = "";
    private string _timestampField = "";
    private string[] _tagFields = [];
    private string[] _fieldFields = [];
    private WritePrecision _precision = WritePrecision.Ns;

    private readonly List<PointData> _batch = [];

    public override void Start(IDictionary<string, string> config)
    {
        var url = config[InfluxDBConnectorConfig.UrlConfig];
        var token = config[InfluxDBConnectorConfig.TokenConfig];
        _org = config[InfluxDBConnectorConfig.OrgConfig];
        _bucket = config[InfluxDBConnectorConfig.BucketConfig];
        _measurement = GetConfigValue(config, InfluxDBConnectorConfig.MeasurementConfig, "");
        _batchSize = int.Parse(GetConfigValue(config, InfluxDBConnectorConfig.BatchSizeConfig, InfluxDBConnectorConfig.DefaultBatchSize.ToString()));
        _maxRetryCount = int.Parse(GetConfigValue(config, InfluxDBConnectorConfig.MaxRetryCountConfig, InfluxDBConnectorConfig.DefaultMaxRetryCount.ToString()));
        _retryDelayMs = long.Parse(GetConfigValue(config, InfluxDBConnectorConfig.RetryDelayMsConfig, InfluxDBConnectorConfig.DefaultRetryDelayMs.ToString()));
        _measurementField = GetConfigValue(config, InfluxDBConnectorConfig.MeasurementFieldConfig, "");
        _timestampField = GetConfigValue(config, InfluxDBConnectorConfig.TimestampFieldConfig, "");

        var tagFieldsStr = GetConfigValue(config, InfluxDBConnectorConfig.TagFieldsConfig, "");
        _tagFields = string.IsNullOrEmpty(tagFieldsStr) ? [] : tagFieldsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var fieldFieldsStr = GetConfigValue(config, InfluxDBConnectorConfig.FieldFieldsConfig, "");
        _fieldFields = string.IsNullOrEmpty(fieldFieldsStr) ? [] : fieldFieldsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        _precision = ParsePrecision(GetConfigValue(config, InfluxDBConnectorConfig.PrecisionConfig, InfluxDBConnectorConfig.DefaultPrecision));

        _client = new InfluxDBClient(url, token);
        _writeApi = _client.GetWriteApiAsync();
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static WritePrecision ParsePrecision(string precision) => precision.ToLowerInvariant() switch
    {
        "s" => WritePrecision.S,
        "ms" => WritePrecision.Ms,
        "us" => WritePrecision.Us,
        "ns" => WritePrecision.Ns,
        _ => WritePrecision.Ns
    };

    public override void Stop()
    {
        FlushBatch().GetAwaiter().GetResult();
        _client?.Dispose();
        _client = null;
        _writeApi = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            // Skip tombstones (null value)
            if (record.Value == null)
                continue;

            var point = CreatePoint(record);
            if (point != null)
            {
                _batch.Add(point);
            }

            if (_batch.Count >= _batchSize)
            {
                await FlushBatch();
            }
        }
    }

    private async Task FlushBatch()
    {
        if (_batch.Count == 0 || _writeApi == null)
            return;

        var retryCount = 0;
        while (retryCount < _maxRetryCount)
        {
            try
            {
                await _writeApi.WritePointsAsync(_batch, _bucket, _org);
                _batch.Clear();
                return;
            }
            catch (Exception ex)
            {
                retryCount++;
                if (retryCount >= _maxRetryCount)
                {
                    Console.Error.WriteLine($"InfluxDB batch write failed after {_maxRetryCount} retries: {ex.Message}");
                    throw;
                }
                await Task.Delay(TimeSpan.FromMilliseconds(_retryDelayMs * retryCount));
            }
        }
    }

    private PointData? CreatePoint(SinkRecord record)
    {
        if (record.Value == null)
            return null;

        var data = ParseRecordValue(record);
        if (data == null || data.Count == 0)
            return null;

        // Determine measurement name
        var measurement = _measurement;
        if (!string.IsNullOrEmpty(_measurementField) && data.TryGetValue(_measurementField, out var mValue) && mValue != null)
        {
            measurement = mValue.ToString()!;
        }

        if (string.IsNullOrEmpty(measurement))
        {
            measurement = record.Topic ?? "default";
        }

        var point = PointData.Measurement(measurement);

        // Add timestamp
        if (!string.IsNullOrEmpty(_timestampField) && data.TryGetValue(_timestampField, out var tsValue) && tsValue != null)
        {
            var timestamp = ParseTimestamp(tsValue);
            if (timestamp.HasValue)
            {
                point = point.Timestamp(timestamp.Value, _precision);
            }
        }
        else if (record.Timestamp != default)
        {
            point = point.Timestamp(record.Timestamp.UtcDateTime, _precision);
        }
        else
        {
            point = point.Timestamp(DateTime.UtcNow, _precision);
        }

        // Add tags and fields
        foreach (var kvp in data)
        {
            if (kvp.Key == _measurementField || kvp.Key == _timestampField)
                continue;

            if (kvp.Value == null)
                continue;

            if (_tagFields.Contains(kvp.Key, StringComparer.OrdinalIgnoreCase))
            {
                // Add as tag
                point = point.Tag(kvp.Key, kvp.Value.ToString()!);
            }
            else if (_fieldFields.Length == 0 || _fieldFields.Contains(kvp.Key, StringComparer.OrdinalIgnoreCase))
            {
                // Add as field
                point = AddField(point, kvp.Key, kvp.Value);
            }
        }

        return point;
    }

    private static PointData AddField(PointData point, string key, object value)
    {
        // Handle JsonElement
        if (value is JsonElement je)
        {
            return je.ValueKind switch
            {
                JsonValueKind.Number when je.TryGetInt64(out var l) => point.Field(key, l),
                JsonValueKind.Number when je.TryGetDouble(out var d) => point.Field(key, d),
                JsonValueKind.True => point.Field(key, true),
                JsonValueKind.False => point.Field(key, false),
                JsonValueKind.String => point.Field(key, je.GetString()!),
                _ => point.Field(key, je.ToString())
            };
        }

        return value switch
        {
            long l => point.Field(key, l),
            int i => point.Field(key, i),
            double d => point.Field(key, d),
            float f => point.Field(key, f),
            decimal dec => point.Field(key, (double)dec),
            bool b => point.Field(key, b),
            string s => point.Field(key, s),
            _ => point.Field(key, value.ToString()!)
        };
    }

    private static DateTime? ParseTimestamp(object value)
    {
        if (value is DateTime dt)
            return dt;

        if (value is DateTimeOffset dto)
            return dto.UtcDateTime;

        if (value is long ticks)
            return DateTime.FromFileTimeUtc(ticks);

        if (value is string s && DateTime.TryParse(s, out var parsed))
            return parsed.ToUniversalTime();

        if (value is JsonElement je)
        {
            if (je.ValueKind == JsonValueKind.String && DateTime.TryParse(je.GetString(), out var jeParsed))
                return jeParsed.ToUniversalTime();
            if (je.ValueKind == JsonValueKind.Number && je.TryGetInt64(out var jeTicks))
                return DateTime.FromFileTimeUtc(jeTicks);
        }

        return null;
    }

    private Dictionary<string, object?>? ParseRecordValue(SinkRecord record)
    {
        if (record.Value == null)
            return null;

        try
        {
            var json = Encoding.UTF8.GetString(record.Value);
            return JsonSerializer.Deserialize<Dictionary<string, object?>>(json, JsonSerializerOptions);
        }
        catch
        {
            return null;
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return FlushBatch();
    }

    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };
}
