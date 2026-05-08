using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Akka.Actor;
using Akka.Cluster.Tools.Client;
using Akka.Cluster.Tools.PublishSubscribe;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Akka;

/// <summary>
/// Task that publishes messages to Akka.NET Cluster Pub/Sub.
/// Uses ClusterClient to connect to a remote cluster and publish to topics.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "ActorSystem disposed via Terminate() in Stop()")]
public sealed class AkkaClusterSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private ActorSystem? _actorSystem;
    private IActorRef? _clusterClient;
    private IActorRef? _mediator;
    private string _clusterTopic = "";
    private int _batchSize = AkkaConnectorConfig.DefaultBatchSize;
    private int _maxRetryCount = AkkaConnectorConfig.DefaultMaxRetryCount;
    private long _retryDelayMs = AkkaConnectorConfig.DefaultRetryDelayMs;

    public override void Start(IDictionary<string, string> config)
    {
        var systemName = GetConfigValue(config, AkkaConnectorConfig.ActorSystemNameConfig, AkkaConnectorConfig.DefaultActorSystemName);
        var hoconConfig = GetConfigValue(config, AkkaConnectorConfig.ActorSystemConfigConfig, "");
        var seedNodes = GetConfigValue(config, AkkaConnectorConfig.ClusterSeedNodesConfig, "");
        _clusterTopic = config[AkkaConnectorConfig.ClusterPublishTopicConfig];
        _batchSize = int.Parse(GetConfigValue(config, AkkaConnectorConfig.BatchSizeConfig, AkkaConnectorConfig.DefaultBatchSize.ToString()));
        _maxRetryCount = int.Parse(GetConfigValue(config, AkkaConnectorConfig.MaxRetryCountConfig, AkkaConnectorConfig.DefaultMaxRetryCount.ToString()));
        _retryDelayMs = long.Parse(GetConfigValue(config, AkkaConnectorConfig.RetryDelayMsConfig, AkkaConnectorConfig.DefaultRetryDelayMs.ToString()));

        // Build HOCON config for cluster client
        var clusterClientConfig = BuildClusterClientConfig(hoconConfig, seedNodes);

        // Create actor system
        var akkaConfig = global::Akka.Configuration.ConfigurationFactory.ParseString(clusterClientConfig);
        _actorSystem = ActorSystem.Create(systemName, akkaConfig);

        // Create cluster client
        _clusterClient = _actorSystem.ActorOf(ClusterClient.Props(
            ClusterClientSettings.Create(_actorSystem)), "cluster-client");

        // Get distributed pub/sub mediator
        _mediator = DistributedPubSub.Get(_actorSystem).Mediator;
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
            _actorSystem.Terminate().Wait(TimeSpan.FromSeconds(5));
            _actorSystem.Dispose();
            _actorSystem = null;
        }
        _clusterClient = null;
        _mediator = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_actorSystem == null || _mediator == null || records.Count == 0)
            return;

        foreach (var batch in records.Chunk(_batchSize))
        {
            var tasks = new List<Task>();

            foreach (var record in batch)
            {
                if (record.Value == null || record.Value.Length == 0)
                    continue;

                var task = PublishWithRetryAsync(record, cancellationToken);
                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
        }
    }

    private async Task PublishWithRetryAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var message = CreateMessage(record);
        var retries = 0;

        while (retries <= _maxRetryCount)
        {
            try
            {
                // Publish to cluster topic
                _mediator!.Tell(new Publish(_clusterTopic, message));
                return;
            }
            catch (Exception) when (retries < _maxRetryCount)
            {
                retries++;
                await Task.Delay(TimeSpan.FromMilliseconds(_retryDelayMs * retries), cancellationToken);
            }
        }
    }

    private SurgewaveMessage CreateMessage(SinkRecord record)
    {
        try
        {
            var content = Encoding.UTF8.GetString(record.Value);

            if (content.StartsWith('{') || content.StartsWith('['))
            {
                var parsed = JsonSerializer.Deserialize<Dictionary<string, object?>>(content);
                if (parsed != null)
                {
                    return new SurgewaveMessage
                    {
                        Topic = record.Topic,
                        Partition = record.Partition,
                        Offset = record.Offset,
                        Timestamp = record.Timestamp,
                        Key = record.Key != null ? Encoding.UTF8.GetString(record.Key) : null,
                        Data = parsed,
                        Headers = record.Headers?.ToDictionary(
                            h => h.Key,
                            h => Encoding.UTF8.GetString(h.Value))
                    };
                }
            }

            return new SurgewaveMessage
            {
                Topic = record.Topic,
                Partition = record.Partition,
                Offset = record.Offset,
                Timestamp = record.Timestamp,
                Key = record.Key != null ? Encoding.UTF8.GetString(record.Key) : null,
                Data = content,
                Headers = record.Headers?.ToDictionary(
                    h => h.Key,
                    h => Encoding.UTF8.GetString(h.Value))
            };
        }
        catch
        {
            return new SurgewaveMessage
            {
                Topic = record.Topic,
                Partition = record.Partition,
                Offset = record.Offset,
                Timestamp = record.Timestamp,
                Key = record.Key != null ? Encoding.UTF8.GetString(record.Key) : null,
                Data = Convert.ToBase64String(record.Value),
                Headers = record.Headers?.ToDictionary(
                    h => h.Key,
                    h => Encoding.UTF8.GetString(h.Value))
            };
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
