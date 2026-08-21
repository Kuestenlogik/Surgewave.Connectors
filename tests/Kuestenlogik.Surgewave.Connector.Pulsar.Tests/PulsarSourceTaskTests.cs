using System.Buffers;
using System.Globalization;
using System.Text;
using DotPulsar;
using DotPulsar.Abstractions;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Pulsar.Tests;

/// <summary>
/// The source used to format the raw epoch-millisecond <c>PublishTime</c> with "O" (a
/// <see cref="FormatException"/> on every message), to divide those milliseconds by 1000 before
/// feeding them back to <c>FromUnixTimeMilliseconds</c> (every record stamped January 1970), and to
/// acknowledge inside <c>PollAsync</c> before the record had ever reached Surgewave. These tests
/// drive the task through a fake consumer and pin all three.
/// </summary>
public class PulsarSourceTaskTests
{
    private static readonly DateTimeOffset PublishedAt = new(2026, 8, 20, 12, 34, 56, TimeSpan.Zero);

    private const string PulsarTopic = "persistent://public/default/orders";

    [Fact]
    public async Task PollAsync_StampsTheRecordWithTheRealPublishTime()
    {
        var consumer = new FakeConsumer(Message("hello"));
        using var task = new PulsarSourceTask(consumer);
        task.Initialize(new TaskContext());
        task.Configure(SourceConfig());

        var record = Assert.Single(await task.PollAsync(CancellationToken.None));

        Assert.Equal(PublishedAt, record.Timestamp);

        // The header has to round-trip as an ISO-8601 instant, not blow up and not land in 1970.
        var header = Encoding.UTF8.GetString(record.Headers!["pulsar.publish.time"]);
        Assert.Equal(
            PublishedAt,
            DateTimeOffset.Parse(header, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind));
    }

    [Fact]
    public async Task PollAsync_CarriesTheKeyValueAndPulsarProperties()
    {
        var consumer = new FakeConsumer(Message("hello", key: "customer-7"));
        using var task = new PulsarSourceTask(consumer);
        task.Initialize(new TaskContext());
        task.Configure(SourceConfig());

        var record = Assert.Single(await task.PollAsync(CancellationToken.None));

        Assert.Equal("orders", record.Topic);
        Assert.Equal("customer-7", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("hello", Encoding.UTF8.GetString(record.Value));
        Assert.Equal(PulsarTopic, (string)record.SourcePartition["pulsar.topic"]);
        Assert.Equal(1L, (long)record.SourceOffset["message_id"]);
        Assert.Equal(consumer.LastMessageId.ToString(), (string)record.SourceOffset["pulsar.message.id"]);
        Assert.Equal(PulsarTopic, Encoding.UTF8.GetString(record.Headers!["pulsar.source.topic"]));
        Assert.Equal("producer-1", Encoding.UTF8.GetString(record.Headers!["pulsar.producer.name"]));
        Assert.Equal("42", Encoding.UTF8.GetString(record.Headers!["pulsar.sequence.id"]));
        Assert.Equal("acme", Encoding.UTF8.GetString(record.Headers!["pulsar.property.tenant"]));
    }

    [Fact]
    public async Task CommitAsync_AcknowledgesOnlyRecordsThatReachedSurgewave()
    {
        var consumer = new FakeConsumer(Message("hello"));
        using var task = new PulsarSourceTask(consumer);
        task.Initialize(new TaskContext());
        task.Configure(SourceConfig());

        var record = Assert.Single(await task.PollAsync(CancellationToken.None));

        // Receiving is not delivering: acknowledging here would lose the message if the produce
        // failed or the worker crashed before the record was durable.
        await task.CommitAsync(CancellationToken.None);
        Assert.Empty(consumer.Acknowledged);

        task.CommitRecord(record, Metadata());
        await task.CommitAsync(CancellationToken.None);

        Assert.Equal(consumer.LastMessageId, Assert.Single(consumer.Acknowledged));
    }

    [Fact]
    public async Task CommitRecord_ForARecordThisTaskNeverPolled_AcknowledgesNothing()
    {
        var consumer = new FakeConsumer(Message("hello"));
        using var task = new PulsarSourceTask(consumer);
        task.Initialize(new TaskContext());
        task.Configure(SourceConfig());

        var record = Assert.Single(await task.PollAsync(CancellationToken.None));

        // A copy carries the same values but is a different message as far as Pulsar is concerned.
        task.CommitRecord(record with { Partition = 7 }, Metadata());
        await task.CommitAsync(CancellationToken.None);

        Assert.Empty(consumer.Acknowledged);
    }

    [Fact]
    public async Task PollAsync_ReturnsNothing_WhenTheConsumerHasNoMessage()
    {
        var consumer = new FakeConsumer();
        using var task = new PulsarSourceTask(consumer);
        task.Initialize(new TaskContext());
        task.Configure(SourceConfig());

        Assert.Empty(await task.PollAsync(CancellationToken.None));
        Assert.Empty(consumer.Acknowledged);
    }

    [Theory]
    [InlineData("${pulsar.topic}", "persistent://public/default/orders", "orders")]
    [InlineData("${pulsar.topic}", "persistent://acme/eu/payments", "payments")]
    [InlineData("pulsar-${pulsar.topic}", "persistent://public/default/orders", "pulsar-orders")]
    [InlineData("fixed-topic", "persistent://public/default/orders", "fixed-topic")]
    public void GetSurgewaveTopic_ResolvesThePlaceholderFromTheLastTopicSegment(
        string template,
        string pulsarTopic,
        string expected)
    {
        using var task = new PulsarSourceTask();

        var config = SourceConfig();
        config[PulsarConnectorConfig.Topic] = template;
        task.Configure(config);

        Assert.Equal(expected, task.GetSurgewaveTopic(pulsarTopic));
    }

    [Fact]
    public void GetSurgewaveTopic_AppliesTheMappingPrefix_OnlyWhenMappingIsEnabled()
    {
        using var disabled = new PulsarSourceTask();
        var config = SourceConfig();
        config[PulsarConnectorConfig.TopicMappingPrefix] = "eu-";
        disabled.Configure(config);

        Assert.Equal("orders", disabled.GetSurgewaveTopic(PulsarTopic));

        using var enabled = new PulsarSourceTask();
        config[PulsarConnectorConfig.TopicMappingEnabled] = "true";
        enabled.Configure(config);

        Assert.Equal("eu-orders", enabled.GetSurgewaveTopic(PulsarTopic));
    }

    private static Dictionary<string, string> SourceConfig() => new(StringComparer.Ordinal)
    {
        [PulsarConnectorConfig.ServiceUrl] = "pulsar://localhost:6650",
        [PulsarConnectorConfig.Topic] = "${pulsar.topic}",
        [PulsarConnectorConfig.Topics] = "persistent://public/default/orders",
        [PulsarConnectorConfig.Subscription] = "surgewave-tests"
    };

    private static RecordMetadata Metadata() => new()
    {
        Topic = "orders",
        Partition = 0,
        Offset = 17,
        Timestamp = PublishedAt
    };

    private static FakeMessage Message(string body, string? key = null) => new()
    {
        MessageId = new MessageId(1, 2, 0, 0, PulsarTopic),
        Data = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(body)),
        KeyBytes = key is null ? null : Encoding.UTF8.GetBytes(key),
        ProducerName = "producer-1",
        SequenceId = 42,
        PublishTime = (ulong)PublishedAt.ToUnixTimeMilliseconds(),
        Properties = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "acme" }
    };

    private sealed class FakeMessage : IMessage<ReadOnlySequence<byte>>
    {
        public required MessageId MessageId { get; init; }

        public required ReadOnlySequence<byte> Data { get; init; }

        public required ulong PublishTime { get; init; }

        public string ProducerName { get; init; } = string.Empty;

        public ulong SequenceId { get; init; }

        public byte[]? KeyBytes { get; init; }

        public IReadOnlyDictionary<string, string> Properties { get; init; }
            = new Dictionary<string, string>(StringComparer.Ordinal);

        public byte[]? SchemaVersion => null;

        public uint RedeliveryCount => 0;

        public bool HasEventTime => false;

        public ulong EventTime => 0;

        public DateTime EventTimeAsDateTime => DateTime.UnixEpoch;

        public DateTimeOffset EventTimeAsDateTimeOffset => DateTimeOffset.UnixEpoch;

        public bool HasBase64EncodedKey => false;

        public bool HasKey => KeyBytes is { Length: > 0 };

        public string? Key => KeyBytes is { } bytes ? Encoding.UTF8.GetString(bytes) : null;

        public bool HasOrderingKey => false;

        public byte[]? OrderingKey => null;

        public DateTime PublishTimeAsDateTime => PublishTimeAsDateTimeOffset.UtcDateTime;

        public DateTimeOffset PublishTimeAsDateTimeOffset
            => DateTimeOffset.FromUnixTimeMilliseconds((long)PublishTime);

        public ReadOnlySequence<byte> Value() => Data;
    }

    private sealed class FakeConsumer : IConsumer<ReadOnlySequence<byte>>
    {
        private readonly Queue<FakeMessage> _messages;

        public FakeConsumer(params FakeMessage[] messages)
        {
            _messages = new Queue<FakeMessage>(messages);
            LastMessageId = messages.Length > 0
                ? messages[^1].MessageId
                : new MessageId(0, 0, 0, 0, PulsarTopic);
        }

        public MessageId LastMessageId { get; }

        public List<MessageId> Acknowledged { get; } = [];

        public Uri ServiceUrl { get; } = new("pulsar://localhost:6650");

        public string SubscriptionName => "surgewave-tests";

        public SubscriptionType SubscriptionType => DotPulsar.SubscriptionType.Shared;

        public string Topic => PulsarTopic;

        public IState<ConsumerState> State => throw new NotSupportedException();

        public ValueTask<IMessage<ReadOnlySequence<byte>>> Receive(CancellationToken cancellationToken = default)
        {
            if (_messages.TryDequeue(out var message))
            {
                return ValueTask.FromResult<IMessage<ReadOnlySequence<byte>>>(message);
            }

            throw new OperationCanceledException("no message available");
        }

        public ValueTask Acknowledge(MessageId messageId, CancellationToken cancellationToken = default)
        {
            Acknowledged.Add(messageId);
            return ValueTask.CompletedTask;
        }

        public ValueTask Acknowledge(IEnumerable<MessageId> messageIds, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask AcknowledgeCumulative(MessageId messageId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask Unsubscribe(CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask RedeliverUnacknowledgedMessages(IEnumerable<MessageId> messageIds, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask RedeliverUnacknowledgedMessages(CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<IEnumerable<MessageId>> GetLastMessageIds(CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask Seek(MessageId messageId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask Seek(ulong publishTime, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
