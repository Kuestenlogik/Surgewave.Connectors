using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Akka;

/// <summary>
/// Task that receives messages from an Akka Streams source.
/// Creates an actor-backed sink that queues messages for Surgewave consumption.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "ActorSystem disposed via Terminate() in Stop()")]
public sealed class AkkaStreamsSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private ActorSystem? _actorSystem;
    private ActorMaterializer? _materializer;
    private IActorRef? _sourceActor;
    private string _topicPattern = AkkaConnectorConfig.DefaultTopicPattern;
    private long _pollTimeoutMs = AkkaConnectorConfig.DefaultPollTimeoutMs;
    private int _maxMessagesPerPoll = AkkaConnectorConfig.DefaultMaxMessagesPerPoll;
    private bool _includeMetadata = true;
    private int _bufferSize = AkkaConnectorConfig.DefaultStreamBufferSize;

    private readonly ConcurrentQueue<StreamMessage> _messageQueue = new();
    private readonly Dictionary<string, object> _sourcePartition = new();
    private long _sequenceNumber;

    public override void Start(IDictionary<string, string> config)
    {
        var systemName = GetConfigValue(config, AkkaConnectorConfig.ActorSystemNameConfig, AkkaConnectorConfig.DefaultActorSystemName);
        var hoconConfig = GetConfigValue(config, AkkaConnectorConfig.ActorSystemConfigConfig, "");
        _topicPattern = GetConfigValue(config, AkkaConnectorConfig.TopicPatternConfig, AkkaConnectorConfig.DefaultTopicPattern);
        _pollTimeoutMs = long.Parse(GetConfigValue(config, AkkaConnectorConfig.PollTimeoutMsConfig, AkkaConnectorConfig.DefaultPollTimeoutMs.ToString()));
        _maxMessagesPerPoll = int.Parse(GetConfigValue(config, AkkaConnectorConfig.MaxMessagesPerPollConfig, AkkaConnectorConfig.DefaultMaxMessagesPerPoll.ToString()));
        _includeMetadata = bool.Parse(GetConfigValue(config, AkkaConnectorConfig.IncludeMetadataConfig, "true"));
        _bufferSize = int.Parse(GetConfigValue(config, AkkaConnectorConfig.StreamBufferSizeConfig, AkkaConnectorConfig.DefaultStreamBufferSize.ToString()));

        _sourcePartition["stream"] = "akka-streams";

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

        // Create materializer
        _materializer = _actorSystem.Materializer();

        // Create a Source.actorRef that allows pushing messages into the stream
        var sourceProps = Props.Create(() => new StreamSourceActor(_messageQueue, _bufferSize));
        _sourceActor = _actorSystem.ActorOf(sourceProps, "stream-source");
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        _materializer?.Dispose();
        _materializer = null;

        if (_actorSystem != null)
        {
            _actorSystem.Terminate().Wait(TimeSpan.FromSeconds(5));
            _actorSystem.Dispose();
            _actorSystem = null;
        }
        _sourceActor = null;
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
                await Task.Delay(50, cancellationToken);
            }
            else
            {
                break;
            }
        }

        return records;
    }

    private SourceRecord ConvertToSourceRecord(StreamMessage message)
    {
        _sequenceNumber++;

        var key = new Dictionary<string, object>
        {
            ["sequence"] = _sequenceNumber
        };

        Dictionary<string, object?> payload;
        if (_includeMetadata)
        {
            payload = new Dictionary<string, object?>
            {
                ["source"] = new Dictionary<string, object>
                {
                    ["stream"] = "akka-streams",
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

        var offset = new Dictionary<string, object>
        {
            [AkkaConnectorConfig.OffsetSequence] = _sequenceNumber
        };

        var headers = new Dictionary<string, byte[]>
        {
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
        return _topicPattern.Replace("${path}", "streams");
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Gets the source actor for pushing messages into the stream.
    /// </summary>
    public IActorRef? SourceActor => _sourceActor;

    /// <summary>
    /// Gets the actor system.
    /// </summary>
    public ActorSystem? ActorSystem => _actorSystem;

    /// <summary>
    /// Gets the materializer for running streams.
    /// </summary>
    public ActorMaterializer? Materializer => _materializer;

    private sealed class StreamMessage
    {
        public required object Content { get; init; }
        public required string MessageType { get; init; }
        public required DateTimeOffset Timestamp { get; init; }
    }

    private sealed class StreamSourceActor : ReceiveActor
    {
        private readonly ConcurrentQueue<StreamMessage> _queue;
        private readonly int _bufferSize;

        public StreamSourceActor(ConcurrentQueue<StreamMessage> queue, int bufferSize)
        {
            _queue = queue;
            _bufferSize = bufferSize;

            ReceiveAny(HandleMessage);
        }

        private void HandleMessage(object message)
        {
            // Check buffer size
            if (_queue.Count >= _bufferSize)
            {
                // Drop oldest (backpressure simulation)
                _queue.TryDequeue(out _);
            }

            object content;
            try
            {
                var json = JsonSerializer.Serialize(message);
                content = JsonSerializer.Deserialize<Dictionary<string, object?>>(json) ?? message;
            }
            catch
            {
                content = message.ToString() ?? "";
            }

            _queue.Enqueue(new StreamMessage
            {
                Content = content,
                MessageType = message.GetType().Name,
                Timestamp = DateTimeOffset.UtcNow
            });
        }
    }
}
