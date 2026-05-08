using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Pulsar;

/// <summary>
/// Task that consumes from Surgewave and produces to Pulsar.
/// </summary>
#pragma warning disable CA2213 // Disposable fields should be disposed - disposed in Stop()
public sealed class PulsarSinkTask : SinkTask
{
    private IPulsarClient? _client;
#pragma warning restore CA2213
    private readonly ConcurrentDictionary<string, IProducer<ReadOnlySequence<byte>>> _producers = new();
    private string _pulsarTopicTemplate = null!;
    private bool _topicMappingEnabled;
    private string _topicMappingPrefix = "";
    private string _serviceUrl = null!;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _serviceUrl = config.TryGetValue(PulsarConnectorConfig.ServiceUrl, out var serviceUrl) ? serviceUrl : PulsarConnectorConfig.DefaultServiceUrl;
        _pulsarTopicTemplate = config[PulsarConnectorConfig.Topic];
        _topicMappingEnabled = (config.TryGetValue(PulsarConnectorConfig.TopicMappingEnabled, out var topicMappingEnabled) ? topicMappingEnabled : "false") == "true";
        _topicMappingPrefix = config.TryGetValue(PulsarConnectorConfig.TopicMappingPrefix, out var topicMappingPrefix) ? topicMappingPrefix : "";

        var clientBuilder = PulsarClient.Builder()
            .ServiceUrl(new Uri(_serviceUrl));

        _client = clientBuilder.Build();
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Value == null) continue;

            var pulsarTopic = GetPulsarTopic(record.Topic);
            var producer = GetOrCreateProducer(pulsarTopic);

            var metadata = new MessageMetadata();

            if (record.Key != null)
            {
                metadata.KeyBytes = record.Key;
            }

            // Add Surgewave metadata as properties
            metadata["surgewave.source.topic"] = record.Topic;
            metadata["surgewave.source.partition"] = record.Partition.ToString();
            metadata["surgewave.source.offset"] = record.Offset.ToString();

            // Copy headers as properties
            if (record.Headers != null)
            {
                foreach (var (key, value) in record.Headers)
                {
                    metadata[$"surgewave.header.{key}"] = Encoding.UTF8.GetString(value);
                }
            }

            await producer.Send(metadata, new ReadOnlySequence<byte>(record.Value), cancellationToken);
        }
    }

    private string GetPulsarTopic(string surgewaveTopic)
    {
        var result = _pulsarTopicTemplate.Replace("${surgewave.topic}", surgewaveTopic);

        if (_topicMappingEnabled && !string.IsNullOrEmpty(_topicMappingPrefix))
        {
            result = _topicMappingPrefix + result;
        }

        // Ensure it's a valid Pulsar topic format
        if (!result.Contains("://"))
        {
            result = $"persistent://public/default/{result}";
        }

        return result;
    }

    private IProducer<ReadOnlySequence<byte>> GetOrCreateProducer(string topic)
    {
        return _producers.GetOrAdd(topic, t =>
            _client!.NewProducer()
                .Topic(t)
                .Create());
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // Producers auto-flush, but we can wait for completion
        await Task.CompletedTask;
    }

    public override void Stop()
    {
        foreach (var producer in _producers.Values)
        {
            producer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
        _producers.Clear();
        _client?.DisposeAsync().AsTask().GetAwaiter().GetResult();
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
