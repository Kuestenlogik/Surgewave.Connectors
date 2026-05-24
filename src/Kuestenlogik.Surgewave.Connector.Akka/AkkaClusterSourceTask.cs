using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Akka.Actor;
using Akka.Cluster.Tools.Client;
using Akka.Cluster.Tools.PublishSubscribe;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Akka;

/// <summary>
/// Task that receives messages from Akka.NET Cluster Pub/Sub.
/// Uses ClusterClient to connect to a remote cluster and subscribe to topics.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "ActorSystem disposed via Terminate() in Stop()")]
public sealed class AkkaClusterSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private ActorSystem? _actorSystem;
    private IActorRef? _clusterClient;
    private IActorRef? _subscriberActor;
    private string _clusterTopic = "";
    private string _topicPattern = AkkaConnectorConfig.DefaultTopicPattern;
    private long _pollTimeoutMs = AkkaConnectorConfig.DefaultPollTimeoutMs;
    private int _maxMessagesPerPoll = AkkaConnectorConfig.DefaultMaxMessagesPerPoll;
    private bool _includeMetadata = true;

    private readonly ConcurrentQueue<ClusterMessage> _messageQueue = new();
    private readonly Dictionary<string, object> _sourcePartition = new();
    private long _sequenceNumber;

    public override void Start(IDictionary<string, string> config)
    {
        var systemName = GetConfigValue(config, AkkaConnectorConfig.ActorSystemNameConfig, AkkaConnectorConfig.DefaultActorSystemName);
        var hoconConfig = GetConfigValue(config, AkkaConnectorConfig.ActorSystemConfigConfig, "");
        var seedNodes = GetConfigValue(config, AkkaConnectorConfig.ClusterSeedNodesConfig, "");
        _clusterTopic = config[AkkaConnectorConfig.ClusterSubscribeTopicConfig];
        _topicPattern = GetConfigValue(config, AkkaConnectorConfig.TopicPatternConfig, AkkaConnectorConfig.DefaultTopicPattern);
        _pollTimeoutMs = long.Parse(GetConfigValue(config, AkkaConnectorConfig.PollTimeoutMsConfig, AkkaConnectorConfig.DefaultPollTimeoutMs.ToString()));
        _maxMessagesPerPoll = int.Parse(GetConfigValue(config, AkkaConnectorConfig.MaxMessagesPerPollConfig, AkkaConnectorConfig.DefaultMaxMessagesPerPoll.ToString()));
        _includeMetadata = bool.Parse(GetConfigValue(config, AkkaConnectorConfig.IncludeMetadataConfig, "true"));

        _sourcePartition["cluster_topic"] = _clusterTopic;

        // Build HOCON config for cluster client
        var clusterClientConfig = BuildClusterClientConfig(hoconConfig, seedNodes);

        // Create actor system
        var akkaConfig = global::Akka.Configuration.ConfigurationFactory.ParseString(clusterClientConfig);
        _actorSystem = ActorSystem.Create(systemName, akkaConfig);

        // Create cluster client
        _clusterClient = _actorSystem.ActorOf(ClusterClient.Props(
            ClusterClientSettings.Create(_actorSystem)), "cluster-client");

        // Create subscriber actor
        var subscriberProps = Props.Create(() => new ClusterSubscriberActor(_messageQueue, _clusterTopic));
        _subscriberActor = _actorSystem.ActorOf(subscriberProps, "cluster-subscriber");

        // Subscribe to distributed pub/sub via cluster client
        var mediator = DistributedPubSub.Get(_actorSystem).Mediator;
        mediator.Tell(new Subscribe(_clusterTopic, _subscriberActor));
    }

    private static string BuildClusterClientConfig(string userConfig, string seedNodes)
    {
        var seedNodeList = seedNodes.Split(',', StringSplitOptions.RemoveEmptyEntries);
        var seedNodeConfig = string.Join(",", seedNodeList.Select(n => $"\"{n.Trim()}\""));

        var baseConfig = $@"
akka {{
    actor {{
        provider = cluster
    }}
    remote {{
        dot-netty.tcp {{
            hostname = ""127.0.0.1""
            port = 0
        }}
    }}
    cluster {{
        client {{
            initial-contacts = [{seedNodeConfig}]
        }}
    }}
    extensions = [""Akka.Cluster.Tools.PublishSubscribe.DistributedPubSubExtensionProvider, Akka.Cluster.Tools""]
}}
";
        return string.IsNullOrEmpty(userConfig)
            ? baseConfig
            : userConfig + "\n" + baseConfig;
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        if (_actorSystem != null)
        {
            // Unsubscribe
            if (_subscriberActor != null)
            {
                var mediator = DistributedPubSub.Get(_actorSystem).Mediator;
                mediator.Tell(new Unsubscribe(_clusterTopic, _subscriberActor));
            }

            _actorSystem.Terminate().Wait(TimeSpan.FromSeconds(5));
            _actorSystem.Dispose();
            _actorSystem = null;
        }
        _clusterClient = null;
        _subscriberActor = null;
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

    private SourceRecord ConvertToSourceRecord(ClusterMessage message)
    {
        _sequenceNumber++;

        var key = new Dictionary<string, object>
        {
            ["sequence"] = _sequenceNumber,
            ["cluster_topic"] = _clusterTopic
        };

        Dictionary<string, object?> payload;
        if (_includeMetadata)
        {
            payload = new Dictionary<string, object?>
            {
                ["source"] = new Dictionary<string, object>
                {
                    ["cluster_topic"] = _clusterTopic,
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

        var offset = new Dictionary<string, object>
        {
            [AkkaConnectorConfig.OffsetSequence] = _sequenceNumber,
            [AkkaConnectorConfig.OffsetClusterTopic] = _clusterTopic
        };

        var headers = new Dictionary<string, byte[]>
        {
            [AkkaConnectorConfig.HeaderClusterTopic] = Encoding.UTF8.GetBytes(_clusterTopic),
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
        return _topicPattern
            .Replace("${path}", _clusterTopic)
            .Replace("${topic}", _clusterTopic);
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    private sealed class ClusterMessage
    {
        public required object Content { get; init; }
        public required string SenderPath { get; init; }
        public required string MessageType { get; init; }
        public required DateTimeOffset Timestamp { get; init; }
    }

    private sealed class ClusterSubscriberActor : ReceiveActor
    {
        private readonly ConcurrentQueue<ClusterMessage> _queue;
        private readonly string _topic;

        public ClusterSubscriberActor(ConcurrentQueue<ClusterMessage> queue, string topic)
        {
            _queue = queue;
            _topic = topic;

            ReceiveAny(HandleMessage);
        }

        private void HandleMessage(object message)
        {
            // Skip internal messages
            if (message is SubscribeAck or UnsubscribeAck)
                return;

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

            _queue.Enqueue(new ClusterMessage
            {
                Content = content,
                SenderPath = Sender.Path.ToString(),
                MessageType = message.GetType().Name,
                Timestamp = DateTimeOffset.UtcNow
            });
        }
    }
}
