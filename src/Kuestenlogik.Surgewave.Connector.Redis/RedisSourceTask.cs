namespace Kuestenlogik.Surgewave.Connector.Redis;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using StackExchange.Redis;

/// <summary>
/// Task that reads from Redis Streams or Pub/Sub.
/// </summary>
public sealed class RedisSourceTask : SourceTask
{
    private ConnectionMultiplexer? _redis;
    private IDatabase? _db;
    private ISubscriber? _subscriber;
    private string _mode = RedisConnectorConfig.ModeStream;
    private string _topic = "";
    private string[] _streams = [];
    private string _consumerGroup = "surgewave-connect";
    private string _consumerName = "consumer-1";
    private string[] _pubsubChannels = [];
    private long _pollIntervalMs = RedisConnectorConfig.DefaultPollIntervalMs;
    private int _batchMaxRecords = RedisConnectorConfig.DefaultBatchMaxRecords;
    private DateTimeOffset _lastPollTime = DateTimeOffset.MinValue;

    // Track stream offsets per stream
    private readonly Dictionary<string, string> _streamOffsets = new();
    private readonly Dictionary<string, object> _sourcePartition = new();

    // Pub/Sub message buffer
    private readonly Queue<ChannelMessage> _pubsubBuffer = new();
    private readonly object _bufferLock = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var connectionString = config[RedisConnectorConfig.ConnectionConfig];
        _mode = GetConfigValue(config, RedisConnectorConfig.ModeConfig, RedisConnectorConfig.ModeStream);
        _topic = config[RedisConnectorConfig.TopicConfig];
        _pollIntervalMs = long.Parse(GetConfigValue(config, RedisConnectorConfig.PollIntervalMsConfig, RedisConnectorConfig.DefaultPollIntervalMs.ToString()));
        _batchMaxRecords = int.Parse(GetConfigValue(config, RedisConnectorConfig.BatchMaxRecordsConfig, RedisConnectorConfig.DefaultBatchMaxRecords.ToString()));

        _redis = ConnectionMultiplexer.Connect(connectionString);
        _db = _redis.GetDatabase();

        if (_mode == RedisConnectorConfig.ModeStream)
        {
            _streams = GetConfigValue(config, RedisConnectorConfig.StreamsConfig, "")
                .Split(',', StringSplitOptions.RemoveEmptyEntries)
                .Select(s => s.Trim())
                .ToArray();
            _consumerGroup = GetConfigValue(config, RedisConnectorConfig.ConsumerGroupConfig, "surgewave-connect");
            _consumerName = GetConfigValue(config, RedisConnectorConfig.ConsumerNameConfig, "consumer-1");

            _sourcePartition["streams"] = string.Join(",", _streams);
            _sourcePartition["consumer_group"] = _consumerGroup;

            // Initialize consumer groups and restore offsets
            InitializeConsumerGroups();
            RestoreOffsets();
        }
        else // pubsub
        {
            _pubsubChannels = GetConfigValue(config, RedisConnectorConfig.PubSubChannelsConfig, "")
                .Split(',', StringSplitOptions.RemoveEmptyEntries)
                .Select(s => s.Trim())
                .ToArray();

            _sourcePartition["channels"] = string.Join(",", _pubsubChannels);

            // Subscribe to channels
            _subscriber = _redis.GetSubscriber();
            foreach (var channel in _pubsubChannels)
            {
                _subscriber.Subscribe(RedisChannel.Pattern(channel), OnPubSubMessage);
            }
        }
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    private void InitializeConsumerGroups()
    {
        foreach (var stream in _streams)
        {
            try
            {
                // Create consumer group if it doesn't exist
                // Use $ to start from the latest message for new groups
                _db!.StreamCreateConsumerGroup(stream, _consumerGroup, "$", createStream: true);
            }
            catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
            {
                // Consumer group already exists, which is fine
            }
        }
    }

    private void RestoreOffsets()
    {
        var storedOffset = Context.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null)
        {
            foreach (var stream in _streams)
            {
                if (storedOffset.TryGetValue($"stream:{stream}", out var offset))
                {
                    _streamOffsets[stream] = offset?.ToString() ?? ">";
                }
                else
                {
                    _streamOffsets[stream] = ">"; // Read pending + new messages
                }
            }
        }
        else
        {
            foreach (var stream in _streams)
            {
                _streamOffsets[stream] = ">"; // Start from pending messages
            }
        }
    }

    public override void Stop()
    {
        _subscriber?.UnsubscribeAll();
        _redis?.Dispose();
        _redis = null;
        _db = null;
        _subscriber = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _subscriber?.UnsubscribeAll();
            _redis?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        // Handle poll interval
        var now = DateTimeOffset.UtcNow;
        var elapsed = (now - _lastPollTime).TotalMilliseconds;
        if (elapsed < _pollIntervalMs)
        {
            var waitTime = (int)(_pollIntervalMs - elapsed);
            await Task.Delay(waitTime, cancellationToken);
        }
        _lastPollTime = DateTimeOffset.UtcNow;

        return _mode == RedisConnectorConfig.ModeStream
            ? await PollStreamsAsync(cancellationToken)
            : PollPubSub();
    }

    private async Task<IReadOnlyList<SourceRecord>> PollStreamsAsync(CancellationToken cancellationToken)
    {
        if (_db == null || _streams.Length == 0)
            return [];

        var records = new List<SourceRecord>();

        // Read from each stream using consumer group
        foreach (var stream in _streams)
        {
            var position = _streamOffsets.TryGetValue(stream, out var pos) ? pos : ">";

            var entries = await _db.StreamReadGroupAsync(
                stream,
                _consumerGroup,
                _consumerName,
                position,
                _batchMaxRecords - records.Count);

            if (entries.Length > 0)
            {
                foreach (var entry in entries)
                {
                    var record = CreateSourceRecord(stream, entry);
                    records.Add(record);

                    // Update offset
                    _streamOffsets[stream] = entry.Id.ToString();
                }
            }

            if (records.Count >= _batchMaxRecords)
                break;
        }

        return records;
    }

    private SourceRecord CreateSourceRecord(string streamName, StreamEntry entry)
    {
        // Convert entry values to JSON
        var data = new Dictionary<string, string>();
        foreach (var value in entry.Values)
        {
            data[value.Name.ToString()] = value.Value.ToString();
        }

        var sourceOffset = new Dictionary<string, object>();
        foreach (var (stream, offset) in _streamOffsets)
        {
            sourceOffset[$"stream:{stream}"] = offset;
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = sourceOffset,
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes($"{streamName}:{entry.Id}"),
            Value = JsonSerializer.SerializeToUtf8Bytes(data),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["redis.stream"] = Encoding.UTF8.GetBytes(streamName),
                ["redis.id"] = Encoding.UTF8.GetBytes(entry.Id.ToString())
            }
        };
    }

    private List<SourceRecord> PollPubSub()
    {
        var records = new List<SourceRecord>();

        lock (_bufferLock)
        {
            var count = Math.Min(_pubsubBuffer.Count, _batchMaxRecords);
            for (var i = 0; i < count; i++)
            {
                var msg = _pubsubBuffer.Dequeue();
                records.Add(new SourceRecord
                {
                    SourcePartition = _sourcePartition,
                    SourceOffset = new Dictionary<string, object>
                    {
                        ["timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    },
                    Topic = _topic,
                    Key = Encoding.UTF8.GetBytes(msg.Channel.ToString()),
                    Value = Encoding.UTF8.GetBytes(msg.Message.ToString()),
                    Timestamp = DateTimeOffset.UtcNow,
                    Headers = new Dictionary<string, byte[]>
                    {
                        ["redis.channel"] = Encoding.UTF8.GetBytes(msg.Channel.ToString())
                    }
                });
            }
        }

        return records;
    }

    private void OnPubSubMessage(RedisChannel channel, RedisValue message)
    {
        lock (_bufferLock)
        {
            _pubsubBuffer.Enqueue(new ChannelMessage(channel, message));
        }
    }

    public override async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_mode != RedisConnectorConfig.ModeStream || _db == null)
            return;

        // Acknowledge processed messages
        foreach (var (stream, lastId) in _streamOffsets)
        {
            if (lastId != ">")
            {
                try
                {
                    await _db.StreamAcknowledgeAsync(stream, _consumerGroup, lastId);
                }
                catch (RedisException)
                {
                    // Log but don't fail - might be already acknowledged
                }
            }
        }
    }

    private readonly record struct ChannelMessage(RedisChannel Channel, RedisValue Message);
}
