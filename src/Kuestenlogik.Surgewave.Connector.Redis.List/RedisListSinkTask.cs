using Kuestenlogik.Surgewave.Connect;
using StackExchange.Redis;

namespace Kuestenlogik.Surgewave.Connector.Redis.List;

/// <summary>
/// Sink task that pushes items to Redis lists using LPUSH/RPUSH.
/// </summary>
#pragma warning disable CA2213 // Disposable fields should be disposed - disposed in Stop()
public sealed class RedisListSinkTask : SinkTask
{
    private ConnectionMultiplexer? _redis;
    private IDatabase? _db;
    private string _key = string.Empty;
    private string _pushDirection = string.Empty;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var connectionString = config.TryGetValue(RedisListConnectorConfig.ConnectionString, out var cs) ? cs : RedisListConnectorConfig.DefaultConnectionString;
        _key = config[RedisListConnectorConfig.Key];
        _pushDirection = config.TryGetValue(RedisListConnectorConfig.PushDirection, out var pd) ? pd : RedisListConnectorConfig.DefaultPushDirection;

        _redis = ConnectionMultiplexer.Connect(connectionString);
        _db = _redis.GetDatabase();
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                if (_pushDirection.Equals("left", StringComparison.OrdinalIgnoreCase))
                {
                    await _db!.ListLeftPushAsync(_key, record.Value);
                }
                else
                {
                    await _db!.ListRightPushAsync(_key, record.Value);
                }
            }
            catch (Exception)
            {
                // Log and continue
            }
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
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
