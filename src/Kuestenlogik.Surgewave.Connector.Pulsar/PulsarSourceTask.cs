using System.Buffers;
using System.Text;
using System.Text.RegularExpressions;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Pulsar;

/// <summary>
/// Task that consumes from Pulsar and produces to Surgewave.
/// </summary>
#pragma warning disable CA2213 // Disposable fields should be disposed - disposed in Stop()
public sealed class PulsarSourceTask : SourceTask
{
    private IPulsarClient? _client;
    private IConsumer<ReadOnlySequence<byte>>? _consumer;
#pragma warning restore CA2213
    private string _surgewaveTopicTemplate = null!;
    private bool _topicMappingEnabled;
    private string _topicMappingPrefix = "";
    private long _messageId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var serviceUrl = config.TryGetValue(PulsarConnectorConfig.ServiceUrl, out var svcUrl) ? svcUrl : PulsarConnectorConfig.DefaultServiceUrl;
        _surgewaveTopicTemplate = config[PulsarConnectorConfig.Topic];
        _topicMappingEnabled = (config.TryGetValue(PulsarConnectorConfig.TopicMappingEnabled, out var topicMappingEnabled) ? topicMappingEnabled : "false") == "true";
        _topicMappingPrefix = config.TryGetValue(PulsarConnectorConfig.TopicMappingPrefix, out var topicMappingPrefix) ? topicMappingPrefix : "";

        var clientBuilder = PulsarClient.Builder()
            .ServiceUrl(new Uri(serviceUrl));

        _client = clientBuilder.Build();

        var subscription = config.TryGetValue(PulsarConnectorConfig.Subscription, out var sub) ? sub : PulsarConnectorConfig.DefaultSubscription;
        var subscriptionType = config.TryGetValue(PulsarConnectorConfig.SubscriptionType, out var subType) ? subType : PulsarConnectorConfig.DefaultSubscriptionType;
        var initialPosition = config.TryGetValue(PulsarConnectorConfig.InitialPosition, out var initPos) ? initPos : PulsarConnectorConfig.DefaultInitialPosition;

        var consumerBuilder = _client.NewConsumer()
            .SubscriptionName(subscription)
            .SubscriptionType(subscriptionType?.ToLowerInvariant() switch
            {
                "exclusive" => SubscriptionType.Exclusive,
                "failover" => SubscriptionType.Failover,
                "keyshared" => SubscriptionType.KeyShared,
                _ => SubscriptionType.Shared
            })
            .InitialPosition(initialPosition?.ToLowerInvariant() == "latest"
                ? SubscriptionInitialPosition.Latest
                : SubscriptionInitialPosition.Earliest);

        // Subscribe to topic (DotPulsar supports single topic per consumer)
        if (config.TryGetValue(PulsarConnectorConfig.Topics, out var topicsStr) && !string.IsNullOrWhiteSpace(topicsStr))
        {
            var topics = topicsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            // Use first topic - DotPulsar consumer builder uses Topic() for single topic
            if (topics.Length > 0)
            {
                consumerBuilder.Topic(topics[0]);
            }
        }
        else if (config.TryGetValue(PulsarConnectorConfig.TopicsPattern, out var pattern) && !string.IsNullOrWhiteSpace(pattern))
        {
            // Topic pattern not supported in current DotPulsar - use direct topic
            consumerBuilder.Topic(pattern);
        }

        if (config.TryGetValue(PulsarConnectorConfig.ConsumerName, out var consumerName) && !string.IsNullOrWhiteSpace(consumerName))
        {
            consumerBuilder.ConsumerName(consumerName);
        }

        _consumer = consumerBuilder.Create();
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        try
        {
            var message = await _consumer!.Receive(cancellationToken);
            if (message != null)
            {
                var pulsarTopic = message.MessageId.Topic ?? "unknown";
                var surgewaveTopic = GetSurgewaveTopic(pulsarTopic);

                var record = new SourceRecord
                {
                    SourcePartition = new Dictionary<string, object>
                    {
                        ["pulsar.topic"] = pulsarTopic
                    },
                    SourceOffset = new Dictionary<string, object>
                    {
                        ["pulsar.message.id"] = message.MessageId.ToString(),
                        ["message_id"] = Interlocked.Increment(ref _messageId)
                    },
                    Topic = surgewaveTopic,
                    Key = message.KeyBytes is { Length: > 0 } keyBytes ? keyBytes.ToArray() : null,
                    Value = message.Data.ToArray(),
                    Timestamp = DateTimeOffset.FromUnixTimeMilliseconds((long)(message.PublishTime / 1000)),
                    Headers = ConvertProperties(message, pulsarTopic)
                };

                records.Add(record);

                // Acknowledge the message
                await _consumer.Acknowledge(message, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation
        }

        return records;
    }

    private string GetSurgewaveTopic(string pulsarTopic)
    {
        // Extract topic name from full Pulsar topic (persistent://tenant/namespace/topic)
        var topicName = pulsarTopic;
        var lastSlash = pulsarTopic.LastIndexOf('/');
        if (lastSlash >= 0)
        {
            topicName = pulsarTopic[(lastSlash + 1)..];
        }

        var result = _surgewaveTopicTemplate.Replace("${pulsar.topic}", topicName);

        if (_topicMappingEnabled && !string.IsNullOrEmpty(_topicMappingPrefix))
        {
            result = _topicMappingPrefix + result;
        }

        return result;
    }

    private static Dictionary<string, byte[]> ConvertProperties(IMessage<ReadOnlySequence<byte>> message, string topic)
    {
        var headers = new Dictionary<string, byte[]>
        {
            ["pulsar.source.topic"] = Encoding.UTF8.GetBytes(topic),
            ["pulsar.message.id"] = Encoding.UTF8.GetBytes(message.MessageId.ToString()),
            ["pulsar.publish.time"] = Encoding.UTF8.GetBytes(message.PublishTime.ToString("O"))
        };

        if (!string.IsNullOrEmpty(message.ProducerName))
        {
            headers["pulsar.producer.name"] = Encoding.UTF8.GetBytes(message.ProducerName);
        }

        if (message.SequenceId > 0)
        {
            headers["pulsar.sequence.id"] = Encoding.UTF8.GetBytes(message.SequenceId.ToString());
        }

        foreach (var (key, value) in message.Properties)
        {
            headers[$"pulsar.property.{key}"] = Encoding.UTF8.GetBytes(value);
        }

        return headers;
    }

    public override void Stop()
    {
        _consumer?.DisposeAsync().AsTask().GetAwaiter().GetResult();
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
