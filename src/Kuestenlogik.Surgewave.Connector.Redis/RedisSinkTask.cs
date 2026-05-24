namespace Kuestenlogik.Surgewave.Connector.Redis;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using StackExchange.Redis;

/// <summary>
/// Task that writes records to Redis using pipelining for performance.
/// </summary>
public sealed class RedisSinkTask : SinkTask
{
    private ConnectionMultiplexer? _redis;
    private IDatabase? _db;
    private string _mode = RedisConnectorConfig.ModeString;
    private string _keyPrefix = "";
    private int _ttlSeconds;
    private string _hashKeyField = "";
    private string _streamNamePattern = "${topic}";
    private int _batchSize = RedisConnectorConfig.DefaultBatchSize;
    private int _retryMax = RedisConnectorConfig.DefaultRetryMax;
    private long _retryBackoffMs = RedisConnectorConfig.DefaultRetryBackoffMs;
    private readonly List<SinkRecord> _buffer = [];

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var connectionString = config[RedisConnectorConfig.ConnectionConfig];
        _mode = GetConfigValue(config, RedisConnectorConfig.ModeConfig, RedisConnectorConfig.ModeString);
        _keyPrefix = GetConfigValue(config, RedisConnectorConfig.KeyPrefixConfig, "");
        _ttlSeconds = int.Parse(GetConfigValue(config, RedisConnectorConfig.TtlSecondsConfig, "0"));
        _hashKeyField = GetConfigValue(config, RedisConnectorConfig.HashKeyFieldConfig, "");
        _streamNamePattern = GetConfigValue(config, RedisConnectorConfig.StreamNameConfig, "${topic}");
        _batchSize = int.Parse(GetConfigValue(config, RedisConnectorConfig.BatchSizeConfig, RedisConnectorConfig.DefaultBatchSize.ToString()));
        _retryMax = int.Parse(GetConfigValue(config, RedisConnectorConfig.RetryMaxConfig, RedisConnectorConfig.DefaultRetryMax.ToString()));
        _retryBackoffMs = long.Parse(GetConfigValue(config, RedisConnectorConfig.RetryBackoffMsConfig, RedisConnectorConfig.DefaultRetryBackoffMs.ToString()));

        // Connect to Redis
        _redis = ConnectionMultiplexer.Connect(connectionString);
        _db = _redis.GetDatabase();
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        FlushBuffer();
        _redis?.Dispose();
        _redis = null;
        _db = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _redis?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        _buffer.AddRange(records);
        if (_buffer.Count >= _batchSize)
        {
            await FlushBufferAsync(cancellationToken);
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        await FlushBufferAsync(cancellationToken);
    }

    private void FlushBuffer() => FlushBufferAsync(CancellationToken.None).GetAwaiter().GetResult();

    private async Task FlushBufferAsync(CancellationToken cancellationToken)
    {
        if (_buffer.Count == 0 || _db == null)
            return;

        var attempt = 0;
        while (true)
        {
            try
            {
                var batch = _db.CreateBatch();
                var tasks = new List<Task>();
                var ttl = _ttlSeconds > 0 ? TimeSpan.FromSeconds(_ttlSeconds) : (TimeSpan?)null;

                foreach (var record in _buffer)
                {
                    switch (_mode)
                    {
                        case RedisConnectorConfig.ModeString:
                            tasks.Add(WriteStringAsync(batch, record, ttl));
                            break;
                        case RedisConnectorConfig.ModeHash:
                            tasks.Add(WriteHashAsync(batch, record, ttl));
                            break;
                        case RedisConnectorConfig.ModeStream:
                            tasks.Add(WriteStreamAsync(batch, record));
                            break;
                    }
                }

                batch.Execute();
                await Task.WhenAll(tasks);
                _buffer.Clear();
                return;
            }
            catch (RedisException) when (attempt < _retryMax)
            {
                attempt++;
                await Task.Delay((int)_retryBackoffMs, cancellationToken);
            }
        }
    }

    private Task<bool> WriteStringAsync(IBatch batch, SinkRecord record, TimeSpan? ttl)
    {
        var key = GetRedisKey(record);
        var value = Encoding.UTF8.GetString(record.Value);
        return batch.StringSetAsync(key, value, ttl, When.Always);
    }

    private Task WriteHashAsync(IBatch batch, SinkRecord record, TimeSpan? ttl)
    {
        var hashKey = GetHashKey(record);
        var entries = ParseHashEntries(record);

        var setTask = batch.HashSetAsync(hashKey, entries);
        if (ttl.HasValue)
        {
            batch.KeyExpireAsync(hashKey, ttl);
        }

        return setTask;
    }

    private Task<RedisValue> WriteStreamAsync(IBatch batch, SinkRecord record)
    {
        var streamName = GetStreamName(record.Topic);
        var entries = ParseStreamEntries(record);
        return batch.StreamAddAsync(streamName, entries);
    }

    private RedisKey GetRedisKey(SinkRecord record)
    {
        var key = record.Key != null
            ? Encoding.UTF8.GetString(record.Key)
            : $"{record.Topic}:{record.Partition}:{record.Offset}";
        return $"{_keyPrefix}{key}";
    }

    private RedisKey GetHashKey(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_hashKeyField))
            return GetRedisKey(record);

        // Extract hash key from record value JSON
        try
        {
            using var doc = JsonDocument.Parse(record.Value);
            if (doc.RootElement.TryGetProperty(_hashKeyField, out var fieldValue))
            {
                var fieldStr = fieldValue.ValueKind == JsonValueKind.String
                    ? fieldValue.GetString()
                    : fieldValue.GetRawText();
                return $"{_keyPrefix}{fieldStr}";
            }
        }
        catch (JsonException)
        {
            // Fall through to default
        }

        return GetRedisKey(record);
    }

    private string GetStreamName(string topic)
    {
        return $"{_keyPrefix}{_streamNamePattern.Replace("${topic}", topic)}";
    }

    private HashEntry[] ParseHashEntries(SinkRecord record)
    {
        try
        {
            using var doc = JsonDocument.Parse(record.Value);
            return doc.RootElement.EnumerateObject()
                .Select(p => new HashEntry(p.Name, GetJsonElementValue(p.Value)))
                .ToArray();
        }
        catch (JsonException)
        {
            // Fallback: store entire value under "data" field
            return [new HashEntry("data", Encoding.UTF8.GetString(record.Value))];
        }
    }

    private NameValueEntry[] ParseStreamEntries(SinkRecord record)
    {
        var entries = new List<NameValueEntry>();

        try
        {
            using var doc = JsonDocument.Parse(record.Value);
            foreach (var prop in doc.RootElement.EnumerateObject())
            {
                entries.Add(new NameValueEntry(prop.Name, GetJsonElementValue(prop.Value)));
            }
        }
        catch (JsonException)
        {
            entries.Add(new NameValueEntry("data", Encoding.UTF8.GetString(record.Value)));
        }

        // Add metadata
        entries.Add(new NameValueEntry("_topic", record.Topic));
        entries.Add(new NameValueEntry("_partition", record.Partition.ToString()));
        entries.Add(new NameValueEntry("_offset", record.Offset.ToString()));

        return entries.ToArray();
    }

    private static string GetJsonElementValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString() ?? "",
            JsonValueKind.Number => element.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => "",
            _ => element.GetRawText()
        };
    }
}
