using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Akka.Actor;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Akka;

/// <summary>
/// Task that receives messages from Akka.NET actors.
/// Creates an actor that receives messages and queues them for polling.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "ActorSystem disposed via Terminate() in Stop()")]
public sealed class AkkaSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private ActorSystem? _actorSystem;
    private IActorRef? _receiverActor;
    private string _actorPath = "/user/surgewave-receiver";
    private string _topicPattern = AkkaConnectorConfig.DefaultTopicPattern;
    private long _pollTimeoutMs = AkkaConnectorConfig.DefaultPollTimeoutMs;
    private int _maxMessagesPerPoll = AkkaConnectorConfig.DefaultMaxMessagesPerPoll;
    private bool _includeMetadata = true;
    private string _messageTypeFilter = "";

    private readonly ConcurrentQueue<ReceivedMessage> _messageQueue = new();
    private readonly Dictionary<string, object> _sourcePartition = new();
    private long _sequenceNumber;

    public override void Start(IDictionary<string, string> config)
    {
        var systemName = GetConfigValue(config, AkkaConnectorConfig.ActorSystemNameConfig, AkkaConnectorConfig.DefaultActorSystemName);
        var hoconConfig = GetConfigValue(config, AkkaConnectorConfig.ActorSystemConfigConfig, "");
        _actorPath = GetConfigValue(config, AkkaConnectorConfig.ActorPathConfig, "/user/surgewave-receiver");
        _topicPattern = GetConfigValue(config, AkkaConnectorConfig.TopicPatternConfig, AkkaConnectorConfig.DefaultTopicPattern);
        _pollTimeoutMs = long.Parse(GetConfigValue(config, AkkaConnectorConfig.PollTimeoutMsConfig, AkkaConnectorConfig.DefaultPollTimeoutMs.ToString()));
        _maxMessagesPerPoll = int.Parse(GetConfigValue(config, AkkaConnectorConfig.MaxMessagesPerPollConfig, AkkaConnectorConfig.DefaultMaxMessagesPerPoll.ToString()));
        _includeMetadata = bool.Parse(GetConfigValue(config, AkkaConnectorConfig.IncludeMetadataConfig, "true"));
        _messageTypeFilter = GetConfigValue(config, AkkaConnectorConfig.MessageTypeConfig, "");

        _sourcePartition["actor_path"] = _actorPath;

        // Create actor system
        if (!string.IsNullOrEmpty(hoconConfig))
        {
            var akkaConfig = global::Akka.Configuration.ConfigurationFactory.ParseString(hoconConfig);
            _actorSystem = ActorSystem.Create(systemName, akkaConfig);
        }
        else
        {
            _actorSystem = ActorSystem.Create(systemName);
        }

        // Create receiver actor
        var props = Props.Create(() => new MessageReceiverActor(_messageQueue, _messageTypeFilter));
        _receiverActor = _actorSystem.ActorOf(props, _actorPath.Split('/').Last());
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        if (_actorSystem != null)
        {
            _actorSystem.Terminate().Wait(TimeSpan.FromSeconds(5));
            _actorSystem.Dispose();
            _actorSystem = null;
        }
        _receiverActor = null;
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
        if (_actorSystem == null)
            return [];

        var records = new List<SourceRecord>();
        var count = 0;

        // Poll with timeout
        var deadline = DateTime.UtcNow.AddMilliseconds(_pollTimeoutMs);

        while (count < _maxMessagesPerPoll && DateTime.UtcNow < deadline && !cancellationToken.IsCancellationRequested)
        {
            if (_messageQueue.TryDequeue(out var message))
            {
                var record = ConvertToSourceRecord(message);
                records.Add(record);
                count++;
            }
            else if (count == 0)
            {
                // Wait a bit before checking again
                await Task.Delay(50, cancellationToken);
            }
            else
            {
                // We have some messages, don't wait
                break;
            }
        }

        return records;
    }

    private SourceRecord ConvertToSourceRecord(ReceivedMessage message)
    {
        _sequenceNumber++;

        // Build key
        var key = new Dictionary<string, object>
        {
            ["sequence"] = _sequenceNumber,
            ["actor_path"] = _actorPath
        };

        // Build payload
        Dictionary<string, object?> payload;
        if (_includeMetadata)
        {
            payload = new Dictionary<string, object?>
            {
                ["source"] = new Dictionary<string, object>
                {
                    ["actor_path"] = _actorPath,
                    ["sender_path"] = message.SenderPath,
                    ["message_type"] = message.MessageType,
                    ["timestamp"] = message.Timestamp.ToString("O")
                },
                ["data"] = message.Content,
                ["ts_ms"] = message.Timestamp.ToUnixTimeMilliseconds()
            };
        }
        else
        {
            payload = new Dictionary<string, object?>
            {
                ["data"] = message.Content
            };
        }

        // Build offset
        var offset = new Dictionary<string, object>
        {
            [AkkaConnectorConfig.OffsetSequence] = _sequenceNumber,
            [AkkaConnectorConfig.OffsetActorPath] = _actorPath
        };

        // Build headers
        var headers = new Dictionary<string, byte[]>
        {
            [AkkaConnectorConfig.HeaderActorPath] = Encoding.UTF8.GetBytes(_actorPath),
            [AkkaConnectorConfig.HeaderSenderPath] = Encoding.UTF8.GetBytes(message.SenderPath),
            [AkkaConnectorConfig.HeaderMessageType] = Encoding.UTF8.GetBytes(message.MessageType),
            [AkkaConnectorConfig.HeaderTimestamp] = Encoding.UTF8.GetBytes(message.Timestamp.ToString("O"))
        };

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = offset,
            Topic = GetTopicName(),
            Key = JsonSerializer.SerializeToUtf8Bytes(key),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = message.Timestamp,
            Headers = headers
        };
    }

    private string GetTopicName()
    {
        return _topicPattern.Replace("${path}", _actorPath.Replace("/", ".").TrimStart('.'));
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // No commit needed - messages are consumed from queue
        return Task.CompletedTask;
    }

    /// <summary>
    /// Gets the receiver actor reference for sending messages.
    /// </summary>
    public IActorRef? ReceiverActor => _receiverActor;

    /// <summary>
    /// Gets the actor system.
    /// </summary>
    public ActorSystem? ActorSystem => _actorSystem;

    private sealed class ReceivedMessage
    {
        public required object Content { get; init; }
        public required string SenderPath { get; init; }
        public required string MessageType { get; init; }
        public required DateTimeOffset Timestamp { get; init; }
    }

    private sealed class MessageReceiverActor : ReceiveActor
    {
        private readonly ConcurrentQueue<ReceivedMessage> _queue;
        private readonly string _messageTypeFilter;

        public MessageReceiverActor(ConcurrentQueue<ReceivedMessage> queue, string messageTypeFilter)
        {
            _queue = queue;
            _messageTypeFilter = messageTypeFilter;

            ReceiveAny(HandleMessage);
        }

        private void HandleMessage(object message)
        {
            var messageType = message.GetType().Name;

            // Apply filter if specified
            if (!string.IsNullOrEmpty(_messageTypeFilter) && messageType != _messageTypeFilter)
                return;

            // Serialize message to JSON-compatible format
            object content;
            try
            {
                // Try to serialize and deserialize to ensure JSON compatibility
                var json = JsonSerializer.Serialize(message);
                content = JsonSerializer.Deserialize<Dictionary<string, object?>>(json) ?? message;
            }
            catch
            {
                // Fallback to string representation
                content = message.ToString() ?? "";
            }

            _queue.Enqueue(new ReceivedMessage
            {
                Content = content,
                SenderPath = Sender.Path.ToString(),
                MessageType = messageType,
                Timestamp = DateTimeOffset.UtcNow
            });
        }
    }
}
