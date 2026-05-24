namespace Kuestenlogik.Surgewave.Connector.Redis;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A source connector that reads data from Redis Streams or Pub/Sub.
/// </summary>
public sealed class RedisSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(RedisSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(RedisConnectorConfig.ConnectionConfig, ConfigType.Password, Importance.High,
            "Redis connection string")
        .Define(RedisConnectorConfig.TopicConfig, ConfigType.String, Importance.High,
            "Destination Surgewave topic", EditorHint.Topic)
        .Define(RedisConnectorConfig.ModeConfig, ConfigType.String, RedisConnectorConfig.ModeStream, Importance.High,
            "Source mode: 'stream' (Redis Streams) or 'pubsub' (Pub/Sub)", EditorHint.Select, options: ["string", "hash", "stream", "pubsub"])
        .Define(RedisConnectorConfig.StreamsConfig, ConfigType.String, "", Importance.Medium,
            "Comma-separated Redis stream names (stream mode)")
        .Define(RedisConnectorConfig.ConsumerGroupConfig, ConfigType.String, "surgewave-connect", Importance.Medium,
            "Consumer group name (stream mode)")
        .Define(RedisConnectorConfig.ConsumerNameConfig, ConfigType.String, "consumer-1", Importance.Medium,
            "Consumer name within group (stream mode)")
        .Define(RedisConnectorConfig.PubSubChannelsConfig, ConfigType.String, "", Importance.Medium,
            "Comma-separated Pub/Sub channel patterns (pubsub mode)")
        .Define(RedisConnectorConfig.PollIntervalMsConfig, ConfigType.Long, (long)RedisConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Polling interval in milliseconds")
        .Define(RedisConnectorConfig.BatchMaxRecordsConfig, ConfigType.Int, (long)RedisConnectorConfig.DefaultBatchMaxRecords, Importance.Medium,
            "Maximum records per poll");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(RedisConnectorConfig.ConnectionConfig, out _))
            throw new ArgumentException($"Missing required config: {RedisConnectorConfig.ConnectionConfig}");

        if (!config.TryGetValue(RedisConnectorConfig.TopicConfig, out _))
            throw new ArgumentException($"Missing required config: {RedisConnectorConfig.TopicConfig}");

        var mode = config.TryGetValue(RedisConnectorConfig.ModeConfig, out var modeValue) ? modeValue : RedisConnectorConfig.ModeStream;
        if (mode is not (RedisConnectorConfig.ModeStream or RedisConnectorConfig.ModePubSub))
            throw new ArgumentException($"Invalid mode '{mode}'. Must be '{RedisConnectorConfig.ModeStream}' or '{RedisConnectorConfig.ModePubSub}'");

        // Validate mode-specific requirements
        if (mode == RedisConnectorConfig.ModeStream && !config.ContainsKey(RedisConnectorConfig.StreamsConfig))
            throw new ArgumentException($"Missing required config for stream mode: {RedisConnectorConfig.StreamsConfig}");

        if (mode == RedisConnectorConfig.ModePubSub && !config.ContainsKey(RedisConnectorConfig.PubSubChannelsConfig))
            throw new ArgumentException($"Missing required config for pubsub mode: {RedisConnectorConfig.PubSubChannelsConfig}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        var mode = _config.TryGetValue(RedisConnectorConfig.ModeConfig, out var modeValue) ? modeValue : RedisConnectorConfig.ModeStream;

        if (mode == RedisConnectorConfig.ModeStream)
        {
            // Partition by stream for parallelism
            var streams = (_config.TryGetValue(RedisConnectorConfig.StreamsConfig, out var streamsValue) ? streamsValue : "")
                .Split(',', StringSplitOptions.RemoveEmptyEntries)
                .Select(s => s.Trim())
                .ToList();

            if (streams.Count == 0)
                return [new Dictionary<string, string>(_config)];

            var tasksToCreate = Math.Min(maxTasks, streams.Count);
            if (tasksToCreate == 0) tasksToCreate = 1;

            var configs = new List<IDictionary<string, string>>();
            var streamsPerTask = streams.Count / tasksToCreate;
            var remainder = streams.Count % tasksToCreate;

            var streamIndex = 0;
            for (var i = 0; i < tasksToCreate; i++)
            {
                var taskConfig = new Dictionary<string, string>(_config);
                var count = streamsPerTask + (i < remainder ? 1 : 0);
                var taskStreams = streams.Skip(streamIndex).Take(count).ToList();
                streamIndex += count;

                taskConfig[RedisConnectorConfig.StreamsConfig] = string.Join(",", taskStreams);
                taskConfig[RedisConnectorConfig.ConsumerNameConfig] = $"consumer-{i}";
                configs.Add(taskConfig);
            }

            return configs;
        }

        // Pub/Sub: single task (subscriptions are handled differently)
        return [new Dictionary<string, string>(_config)];
    }
}
