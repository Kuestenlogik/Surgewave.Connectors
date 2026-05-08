using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using StackExchange.Redis;

namespace Kuestenlogik.Surgewave.Connector.Redis.Scan;

/// <summary>
/// Source task that scans Redis keys using SCAN with pattern matching and TYPE filtering.
/// </summary>
#pragma warning disable CA2213 // Disposable fields should be disposed - disposed in Stop()
public sealed class RedisScanSourceTask : SourceTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    private ConnectionMultiplexer? _redis;
    private IDatabase? _db;
    private IServer? _server;
    private string _pattern = string.Empty;
    private string _topic = string.Empty;
    private string _keyType = string.Empty;
    private bool _includeValue;
    private int _batchSize;
    private int _pollIntervalMs;
    private DateTime _lastPoll = DateTime.MinValue;
    private HashSet<string> _seenKeys = new();
    private long _offset;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var connectionString = config.TryGetValue(RedisScanConnectorConfig.ConnectionString, out var cs) ? cs : RedisScanConnectorConfig.DefaultConnectionString;
        var database = config.TryGetValue(RedisScanConnectorConfig.Database, out var db) ? int.Parse(db) : RedisScanConnectorConfig.DefaultDatabase;

        _pattern = config.TryGetValue(RedisScanConnectorConfig.Pattern, out var p) ? p : RedisScanConnectorConfig.DefaultPattern;
        _topic = config[RedisScanConnectorConfig.Topic];
        _keyType = config.TryGetValue(RedisScanConnectorConfig.KeyType, out var kt) ? kt : RedisScanConnectorConfig.DefaultKeyType;
        _includeValue = config.TryGetValue(RedisScanConnectorConfig.IncludeValue, out var iv) && iv == "true";
        _batchSize = config.TryGetValue(RedisScanConnectorConfig.BatchSize, out var bs) ? int.Parse(bs) : RedisScanConnectorConfig.DefaultBatchSize;
        _pollIntervalMs = config.TryGetValue(RedisScanConnectorConfig.PollIntervalMs, out var pi) ? int.Parse(pi) : RedisScanConnectorConfig.DefaultPollIntervalMs;

        _redis = ConnectionMultiplexer.Connect(connectionString);
        _db = _redis.GetDatabase(database);
        _server = _redis.GetServer(_redis.GetEndPoints().First());
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        // Check poll interval
        if ((DateTime.UtcNow - _lastPoll).TotalMilliseconds < _pollIntervalMs)
        {
            await Task.Delay(Math.Max(100, _pollIntervalMs - (int)(DateTime.UtcNow - _lastPoll).TotalMilliseconds), cancellationToken);
        }

        _lastPoll = DateTime.UtcNow;

        try
        {
            var keys = _server!.Keys(pattern: _pattern, pageSize: _batchSize);
            var count = 0;

            foreach (var key in keys)
            {
                if (count >= _batchSize) break;

                var keyStr = key.ToString();

                // Skip already seen keys
                if (_seenKeys.Contains(keyStr))
                    continue;

                // Filter by type if specified
                if (!string.IsNullOrEmpty(_keyType))
                {
                    var type = await _db!.KeyTypeAsync(key);
                    if (!type.ToString().Equals(_keyType, StringComparison.OrdinalIgnoreCase))
                        continue;
                }

                var record = await CreateSourceRecordAsync(key);
                if (record != null)
                {
                    records.Add(record);
                    _seenKeys.Add(keyStr);
                    count++;
                }
            }

            // Clear seen keys periodically to allow re-scanning
            if (_seenKeys.Count > 100000)
            {
                _seenKeys.Clear();
            }
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation
        }

        return records;
    }

    private async Task<SourceRecord?> CreateSourceRecordAsync(RedisKey key)
    {
        var keyStr = key.ToString();
        var type = await _db!.KeyTypeAsync(key);
        var currentOffset = Interlocked.Increment(ref _offset);

        var data = new Dictionary<string, object?>
        {
            ["key"] = keyStr,
            ["type"] = type.ToString()
        };

        if (_includeValue)
        {
            data["value"] = type switch
            {
                RedisType.String => (string?)await _db.StringGetAsync(key),
                RedisType.List => (await _db.ListRangeAsync(key)).Select(v => (string?)v).ToList(),
                RedisType.Set => (await _db.SetMembersAsync(key)).Select(v => (string?)v).ToList(),
                RedisType.SortedSet => (await _db.SortedSetRangeByRankWithScoresAsync(key)).Select(e => new { member = (string?)e.Element, score = e.Score }).ToList(),
                RedisType.Hash => (await _db.HashGetAllAsync(key)).ToDictionary(e => (string)e.Name!, e => (string?)e.Value),
                _ => null
            };

            var ttl = await _db.KeyTimeToLiveAsync(key);
            if (ttl.HasValue)
            {
                data["ttl_seconds"] = (long)ttl.Value.TotalSeconds;
            }
        }

        var json = JsonSerializer.Serialize(data, JsonOptions);

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["pattern"] = _pattern
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["offset"] = currentOffset
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(keyStr),
            Value = Encoding.UTF8.GetBytes(json),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["redis.key"] = Encoding.UTF8.GetBytes(keyStr),
                ["redis.type"] = Encoding.UTF8.GetBytes(type.ToString())
            }
        };
    }

    public override void Stop()
    {
        _redis?.Dispose();
        _redis = null;
        _db = null;
        _server = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }
}
#pragma warning restore CA2213
