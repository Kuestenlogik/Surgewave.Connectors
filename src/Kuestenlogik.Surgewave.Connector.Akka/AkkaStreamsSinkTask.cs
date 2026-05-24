using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Akka;

/// <summary>
/// Task that sends messages to an Akka Streams sink.
/// Uses Akka Streams for backpressure-aware message processing.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "ActorSystem disposed via Terminate() in Stop()")]
public sealed class AkkaStreamsSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private ActorSystem? _actorSystem;
    private ActorMaterializer? _materializer;
    private IActorRef? _targetActor;
    private string _actorPath = "";
    private int _batchSize = AkkaConnectorConfig.DefaultBatchSize;
    private int _bufferSize = AkkaConnectorConfig.DefaultStreamBufferSize;
    private int _parallelism = AkkaConnectorConfig.DefaultStreamParallelism;
    private string _overflowStrategy = AkkaConnectorConfig.DefaultOverflowStrategy;

    public override void Start(IDictionary<string, string> config)
    {
        var systemName = GetConfigValue(config, AkkaConnectorConfig.ActorSystemNameConfig, AkkaConnectorConfig.DefaultActorSystemName);
        var hoconConfig = GetConfigValue(config, AkkaConnectorConfig.ActorSystemConfigConfig, "");
        _actorPath = GetConfigValue(config, AkkaConnectorConfig.ActorPathConfig, "/user/stream-sink");
        _batchSize = int.Parse(GetConfigValue(config, AkkaConnectorConfig.BatchSizeConfig, AkkaConnectorConfig.DefaultBatchSize.ToString()));
        _bufferSize = int.Parse(GetConfigValue(config, AkkaConnectorConfig.StreamBufferSizeConfig, AkkaConnectorConfig.DefaultStreamBufferSize.ToString()));
        _parallelism = int.Parse(GetConfigValue(config, AkkaConnectorConfig.StreamParallelismConfig, AkkaConnectorConfig.DefaultStreamParallelism.ToString()));
        _overflowStrategy = GetConfigValue(config, AkkaConnectorConfig.StreamOverflowStrategyConfig, AkkaConnectorConfig.DefaultOverflowStrategy);

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

        // Create target actor that will receive the stream output
        var sinkProps = Props.Create(() => new StreamSinkActor());
        _targetActor = _actorSystem.ActorOf(sinkProps, _actorPath.Split('/').Last());
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
        _targetActor = null;
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
        if (_actorSystem == null || _materializer == null || _targetActor == null || records.Count == 0)
            return;

        // Filter tombstones
        var validRecords = records.Where(r => r.Value != null && r.Value.Length > 0).ToList();
        if (validRecords.Count == 0)
            return;

        // Create messages
        var messages = validRecords.Select(CreateMessage).ToList();

        // Build and run the stream
        var source = Source.From(messages);

        // Apply buffer with overflow strategy
        var buffered = ApplyBuffer(source);

        // Process with parallelism
        var processed = buffered.SelectAsync(_parallelism, async msg =>
        {
            _targetActor.Tell(msg);
            return msg;
        });

        // Run to completion
        await processed.RunWith(Sink.Ignore<SurgewaveMessage>(), _materializer);
    }

    private Source<SurgewaveMessage, global::Akka.NotUsed> ApplyBuffer(Source<SurgewaveMessage, global::Akka.NotUsed> source)
    {
        return _overflowStrategy.ToLowerInvariant() switch
        {
            "drophead" => source.Buffer(_bufferSize, OverflowStrategy.DropHead),
            "droptail" => source.Buffer(_bufferSize, OverflowStrategy.DropTail),
            "dropbuffer" => source.Buffer(_bufferSize, OverflowStrategy.DropBuffer),
            "fail" => source.Buffer(_bufferSize, OverflowStrategy.Fail),
            _ => source.Buffer(_bufferSize, OverflowStrategy.Backpressure)
        };
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

    /// <summary>
    /// Gets the target actor for the stream sink.
    /// </summary>
    public IActorRef? TargetActor => _targetActor;

    /// <summary>
    /// Gets the actor system.
    /// </summary>
    public ActorSystem? ActorSystem => _actorSystem;

    /// <summary>
    /// Gets the materializer for running streams.
    /// </summary>
    public ActorMaterializer? Materializer => _materializer;

    private sealed class StreamSinkActor : ReceiveActor
    {
        public StreamSinkActor()
        {
            Receive<SurgewaveMessage>(HandleMessage);
            ReceiveAny(_ => { }); // Ignore other messages
        }

        private void HandleMessage(SurgewaveMessage message)
        {
            // This actor receives processed messages
            // Override this in a derived class to handle messages
            // Or use a custom actor path to route to your own actor
        }
    }
}
