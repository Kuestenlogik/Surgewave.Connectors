using System.Diagnostics.CodeAnalysis;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using StackExchange.Redis;

namespace Kuestenlogik.Surgewave.Connector.Redis.List;

/// <summary>
/// Source task that pops items from Redis lists using BLPOP/BRPOP.
/// </summary>
#pragma warning disable CA2213 // Disposable fields should be disposed - disposed in Stop()
public sealed class RedisListSourceTask : SourceTask
{
    private ConnectionMultiplexer? _redis;
    private IDatabase? _db;
    private string _key = string.Empty;
    private string _topic = string.Empty;
    private string _popDirection = string.Empty;
    private int _blockingTimeoutMs;
    private int _batchSize;
    private long _offset;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var connectionString = config.TryGetValue(RedisListConnectorConfig.ConnectionString, out var cs) ? cs : RedisListConnectorConfig.DefaultConnectionString;
        _key = config[RedisListConnectorConfig.Key];
        _topic = config[RedisListConnectorConfig.Topic];
        _popDirection = config.TryGetValue(RedisListConnectorConfig.PopDirection, out var pd) ? pd : RedisListConnectorConfig.DefaultPopDirection;
        _blockingTimeoutMs = config.TryGetValue(RedisListConnectorConfig.BlockingTimeoutMs, out var bt) ? int.Parse(bt) : RedisListConnectorConfig.DefaultBlockingTimeoutMs;
        _batchSize = config.TryGetValue(RedisListConnectorConfig.BatchSize, out var bs) ? int.Parse(bs) : RedisListConnectorConfig.DefaultBatchSize;

        _redis = ConnectionMultiplexer.Connect(connectionString);
        _db = _redis.GetDatabase();
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        try
        {
            for (var i = 0; i < _batchSize; i++)
            {
                RedisValue value;

                if (_popDirection.Equals("right", StringComparison.OrdinalIgnoreCase))
                {
                    value = await _db!.ListRightPopAsync(_key);
                }
                else
                {
                    value = await _db!.ListLeftPopAsync(_key);
                }

                if (value.IsNullOrEmpty)
                {
                    // No more items, wait briefly before returning
                    if (records.Count == 0)
                    {
                        await Task.Delay(Math.Min(_blockingTimeoutMs, 1000), cancellationToken);
                    }
                    break;
                }

                var currentOffset = Interlocked.Increment(ref _offset);
                var data = (byte[])value!;

                records.Add(new SourceRecord
                {
                    SourcePartition = new Dictionary<string, object>
                    {
                        ["key"] = _key
                    },
                    SourceOffset = new Dictionary<string, object>
                    {
                        ["offset"] = currentOffset
                    },
                    Topic = _topic,
                    Key = Encoding.UTF8.GetBytes($"{_key}:{currentOffset}"),
                    Value = data,
                    Timestamp = DateTimeOffset.UtcNow,
                    Headers = new Dictionary<string, byte[]>
                    {
                        ["redis.key"] = Encoding.UTF8.GetBytes(_key),
                        ["redis.direction"] = Encoding.UTF8.GetBytes(_popDirection)
                    }
                });
            }
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation
        }

        return records;
    }

    public override void Stop()
    {
        _redis?.Dispose();
        _redis = null;
        _db = null;
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
