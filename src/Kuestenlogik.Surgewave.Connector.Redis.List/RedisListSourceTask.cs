using System.Diagnostics.CodeAnalysis;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using StackExchange.Redis;

namespace Kuestenlogik.Surgewave.Connector.Redis.List;

/// <summary>
/// Source task that reads items from Redis lists via LMOVE into a processing list,
/// removing them only after the records are committed to the broker.
/// </summary>
#pragma warning disable CA2213 // Disposable fields should be disposed - disposed in Stop()
public sealed class RedisListSourceTask : SourceTask
{
    private ConnectionMultiplexer? _redis;
    private IDatabase? _db;
    private string _key = string.Empty;
    private string _processingKey = string.Empty;
    private string _topic = string.Empty;
    private string _popDirection = string.Empty;
    private int _blockingTimeoutMs;
    private int _batchSize;
    private long _offset;

    // Values committed to the broker but not yet removed from the processing list
    private readonly List<RedisValue> _committed = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var connectionString = config.TryGetValue(RedisListConnectorConfig.ConnectionString, out var cs) ? cs : RedisListConnectorConfig.DefaultConnectionString;
        _key = config[RedisListConnectorConfig.Key];
        _topic = config[RedisListConnectorConfig.Topic];
        _popDirection = config.TryGetValue(RedisListConnectorConfig.PopDirection, out var pd) ? pd : RedisListConnectorConfig.DefaultPopDirection;
        _blockingTimeoutMs = config.TryGetValue(RedisListConnectorConfig.BlockingTimeoutMs, out var bt) ? int.Parse(bt) : RedisListConnectorConfig.DefaultBlockingTimeoutMs;
        _batchSize = config.TryGetValue(RedisListConnectorConfig.BatchSize, out var bs) ? int.Parse(bs) : RedisListConnectorConfig.DefaultBatchSize;

        _processingKey = $"{_key}:processing";

        _redis = ConnectionMultiplexer.Connect(connectionString);
        _db = _redis.GetDatabase();

        RecoverInFlightItems();
    }

    private ListSide ConsumeSide()
        => _popDirection.Equals("right", StringComparison.OrdinalIgnoreCase) ? ListSide.Right : ListSide.Left;

    private void RecoverInFlightItems()
    {
        // Items left in the processing list belong to a previous run that stopped between
        // reading and committing - move them back so they are delivered again.
        var refillSide = ConsumeSide() == ListSide.Left ? ListSide.Right : ListSide.Left;
        while (!_db!.ListMove(_processingKey, _key, ListSide.Left, refillSide).IsNull)
        {
        }
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        try
        {
            for (var i = 0; i < _batchSize; i++)
            {
                // LMOVE to a processing list instead of a destructive pop; the item is
                // removed from there only after the record is committed to the broker
                var value = await _db!.ListMoveAsync(_key, _processingKey, ConsumeSide(), ListSide.Right);

                if (value.IsNull)
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

    public override void CommitRecord(SourceRecord record, RecordMetadata metadata)
    {
        _committed.Add(record.Value);
    }

    public override async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_db == null || _committed.Count == 0)
            return;

        try
        {
            foreach (var value in _committed)
            {
                await _db.ListRemoveAsync(_processingKey, value, 1);
            }
            _committed.Clear();
        }
        catch (RedisException ex)
        {
            // Keep the values so the next commit retries the removal (LREM of an
            // already-removed value is a no-op)
            Context.RaiseError?.Invoke(ex);
        }
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
