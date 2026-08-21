using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Telegram.Bot;
using Telegram.Bot.Args;
using Telegram.Bot.Exceptions;
using Telegram.Bot.Requests;
using Telegram.Bot.Requests.Abstractions;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

namespace Kuestenlogik.Surgewave.Connector.Telegram.Tests;

/// <summary>
/// Tests for the getUpdates cursor of <see cref="TelegramSourceTask"/>. Telegram acknowledges
/// every update below the offset a poll asks for, so the task may only advance that offset once
/// Surgewave has durably taken the records - otherwise a crash between poll and commit loses
/// messages that Telegram will never hand out again.
/// </summary>
public class TelegramSourceTaskTests
{
    private const string BotToken = "123456:AAHfake-token";
    private const string BotId = "123456";
    private const string Topic = "telegram-events";
    private const long WatchedChat = 4242L;

    private static readonly DateTime MessageDate = new(2026, 3, 1, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public async Task PollAsync_WithNothingAcknowledgedYet_AsksForUpdatesWithoutAnOffset()
    {
        var client = new FakeBotClient();
        using var task = StartTask(client);

        await task.PollAsync(CancellationToken.None);

        Assert.Null(Assert.Single(client.RequestedOffsets));
    }

    [Fact]
    public async Task PollAsync_ResumesAfterTheUpdateIdTheLastRunCommitted()
    {
        var client = new FakeBotClient();
        client.Returns(PrivateMessage(101));
        using var task = StartTask(client, new FakeOffsetStorageReader(lastUpdateId: 100));

        await task.PollAsync(CancellationToken.None);

        Assert.Equal(101, Assert.Single(client.RequestedOffsets));
    }

    [Fact]
    public async Task PollAsync_DoesNotAcknowledgeUpdatesThatWereOnlyPolled()
    {
        // The failure this guards: acknowledging on poll means a crash before the records are
        // durable drops them, because Telegram never redelivers an acknowledged update.
        var client = new FakeBotClient();
        client.Returns(PrivateMessage(101), PrivateMessage(102));
        client.Returns(PrivateMessage(101), PrivateMessage(102));
        using var task = StartTask(client, new FakeOffsetStorageReader(lastUpdateId: 100));

        await task.PollAsync(CancellationToken.None);
        await task.PollAsync(CancellationToken.None);

        Assert.Equal(2, client.RequestedOffsets.Count);
        Assert.All(client.RequestedOffsets, offset => Assert.Equal(101, offset));
    }

    [Fact]
    public async Task CommitRecord_AcknowledgesUpToTheCommittedUpdate()
    {
        var client = new FakeBotClient();
        client.Returns(PrivateMessage(101), PrivateMessage(102));
        using var task = StartTask(client, new FakeOffsetStorageReader(lastUpdateId: 100));

        var records = await task.PollAsync(CancellationToken.None);
        task.CommitRecord(records[0], Metadata());
        await task.PollAsync(CancellationToken.None);

        // Update 102 is still uncommitted, so only 101 may be acknowledged.
        Assert.Equal(102, client.RequestedOffsets[1]);
    }

    [Fact]
    public async Task CommitAsync_AcknowledgesTheWholePolledBatch()
    {
        var client = new FakeBotClient();
        client.Returns(PrivateMessage(101), PrivateMessage(102));
        using var task = StartTask(client, new FakeOffsetStorageReader(lastUpdateId: 100));

        await task.PollAsync(CancellationToken.None);
        await task.CommitAsync(CancellationToken.None);
        await task.PollAsync(CancellationToken.None);

        Assert.Equal(103, client.RequestedOffsets[1]);
    }

    [Fact]
    public async Task PollAsync_WhenEveryUpdateIsFilteredOut_StillMovesPastThem()
    {
        // Filtered updates produce no record and would otherwise be redelivered forever.
        var client = new FakeBotClient();
        client.Returns(PrivateMessage(101, chatId: 999));
        using var task = StartTask(
            client,
            new FakeOffsetStorageReader(lastUpdateId: 100),
            raiseError: null,
            (TelegramConnectorConfig.ChatIds, WatchedChat.ToString(CultureInfo.InvariantCulture)));

        var records = await task.PollAsync(CancellationToken.None);
        await task.PollAsync(CancellationToken.None);

        Assert.Empty(records);
        Assert.Equal(102, client.RequestedOffsets[1]);
    }

    [Fact]
    public async Task PollAsync_WhenTheBotApiFails_SurfacesTheErrorInsteadOfProducingNothing()
    {
        // A rejected bot token used to be swallowed: the connector stayed "running" forever
        // while never emitting a single message.
        using var cancellation = new CancellationTokenSource();
        var errors = new List<Exception>();
        var client = new FakeBotClient();
        client.Fails(new InvalidOperationException("Unauthorized: bot token is invalid"));

        using var task = StartTask(client, reader: null, raiseError: ex =>
        {
            errors.Add(ex);

            // Cut the task's error backoff short so the test stays fast and deterministic.
            cancellation.Cancel();
        });

        var records = await task.PollAsync(cancellation.Token);

        Assert.Empty(records);
        Assert.Contains("Unauthorized", Assert.Single(errors).Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_SkipsGroupMessagesWhenGroupsAreExcluded()
    {
        var client = new FakeBotClient();
        client.Returns(
            NewUpdate(101, NewMessage(WatchedChat, messageId: 1, text: "group", chatType: ChatType.Supergroup)),
            PrivateMessage(102, messageId: 2, text: "direct"));
        using var task = StartTask(client, reader: null, raiseError: null,
            (TelegramConnectorConfig.IncludeGroups, "false"));

        var records = await task.PollAsync(CancellationToken.None);

        var record = Assert.Single(records);
        Assert.Equal("direct", TextOf(record));
    }

    [Fact]
    public async Task PollAsync_OnlyKeepsTheConfiguredMessageTypes()
    {
        var client = new FakeBotClient();
        var withoutText = NewMessage(WatchedChat, messageId: 5, text: null, chatType: ChatType.Private);
        client.Returns(
            NewUpdate(101, withoutText),
            PrivateMessage(102, messageId: 6, text: "keep me"));
        using var task = StartTask(client, reader: null, raiseError: null,
            (TelegramConnectorConfig.MessageTypes, "text"));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal("keep me", TextOf(Assert.Single(records)));
    }

    [Fact]
    public async Task PollAsync_CarriesTheUpdateIdOfEachRecordInItsOwnOffset()
    {
        var client = new FakeBotClient();
        client.Returns(PrivateMessage(101, messageId: 1), PrivateMessage(102, messageId: 2));
        using var task = StartTask(client);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(2, records.Count);
        Assert.Equal(101, UpdateIdOf(records[0]));
        Assert.Equal(102, UpdateIdOf(records[1]));
    }

    [Fact]
    public async Task PollAsync_MapsTheMessageOntoTheRecord()
    {
        var client = new FakeBotClient();
        client.Returns(PrivateMessage(101, messageId: 99, text: "hello"));
        using var task = StartTask(client);

        var record = Assert.Single(await task.PollAsync(CancellationToken.None));

        Assert.Equal(Topic, record.Topic);
        Assert.Equal($"{WatchedChat}:99", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("telegram", record.SourcePartition[TelegramConnectorConfig.PartitionSource]);
        Assert.Equal(BotId, record.SourcePartition[TelegramConnectorConfig.PartitionBotId]);
        Assert.Equal(new DateTimeOffset(MessageDate), record.Timestamp);
        Assert.Equal("message", Encoding.UTF8.GetString(record.Headers!["telegram.event.type"]));

        using var document = JsonDocument.Parse(record.Value);
        var payload = document.RootElement;
        Assert.Equal("message", payload.GetProperty("event_type").GetString());
        Assert.Equal("hello", payload.GetProperty("text").GetString());
        Assert.Equal(WatchedChat, payload.GetProperty("chat_id").GetInt64());
        Assert.Equal(new DateTimeOffset(MessageDate).ToUnixTimeSeconds(), payload.GetProperty("date").GetInt64());
    }

    [Fact]
    public async Task PollAsync_LabelsAnEditedMessageAsAnEdit()
    {
        var client = new FakeBotClient();
        client.Returns(new Update
        {
            Id = 101,
            EditedMessage = NewMessage(WatchedChat, messageId: 3, text: "fixed typo", chatType: ChatType.Private)
        });
        using var task = StartTask(client);

        var record = Assert.Single(await task.PollAsync(CancellationToken.None));

        using var document = JsonDocument.Parse(record.Value);
        Assert.Equal("message_edit", document.RootElement.GetProperty("event_type").GetString());
    }

    private static TelegramSourceTask StartTask(
        FakeBotClient client,
        IOffsetStorageReader? reader = null,
        Action<Exception>? raiseError = null,
        params (string Key, string Value)[] settings)
    {
        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TelegramConnectorConfig.BotToken] = BotToken,
            [TelegramConnectorConfig.Topic] = Topic
        };

        foreach (var (key, value) in settings)
        {
            config[key] = value;
        }

        var task = new TelegramSourceTask(client);
        task.Initialize(new TaskContext
        {
            OffsetStorageReader = reader,
            RaiseError = raiseError ?? (_ => { })
        });
        task.Start(config);
        return task;
    }

    private static Update PrivateMessage(
        int updateId,
        long chatId = WatchedChat,
        int messageId = 1,
        string? text = "hello") =>
        NewUpdate(updateId, NewMessage(chatId, messageId, text, ChatType.Private));

    private static Update NewUpdate(int updateId, Message message) => new() { Id = updateId, Message = message };

    private static Message NewMessage(long chatId, int messageId, string? text, ChatType chatType) => new()
    {
        Id = messageId,
        Date = MessageDate,
        Text = text,
        Chat = new Chat { Id = chatId, Type = chatType, Title = "Ops" },
        From = new User { Id = 77, IsBot = false, FirstName = "Ada", Username = "ada" }
    };

    private static RecordMetadata Metadata() => new()
    {
        Topic = Topic,
        Partition = 0,
        Offset = 12
    };

    private static string? TextOf(SourceRecord record)
    {
        using var document = JsonDocument.Parse(record.Value);
        return document.RootElement.GetProperty("text").GetString();
    }

    private static int UpdateIdOf(SourceRecord record) =>
        Convert.ToInt32(record.SourceOffset[TelegramConnectorConfig.OffsetUpdateId], CultureInfo.InvariantCulture);

    private sealed class FakeOffsetStorageReader(int lastUpdateId) : IOffsetStorageReader
    {
        public IDictionary<string, object>? Offset(IDictionary<string, object> partition)
        {
            Assert.Equal("telegram", partition[TelegramConnectorConfig.PartitionSource]);
            Assert.Equal(BotId, partition[TelegramConnectorConfig.PartitionBotId]);

            return new Dictionary<string, object>(StringComparer.Ordinal)
            {
                [TelegramConnectorConfig.OffsetUpdateId] = lastUpdateId
            };
        }

        public IDictionary<IDictionary<string, object>, IDictionary<string, object>> Offsets(
            IReadOnlyCollection<IDictionary<string, object>> partitions) => throw new NotSupportedException();
    }

    /// <summary>
    /// A Bot API client that answers <c>getUpdates</c> from a scripted queue and records the
    /// offset every call asked for.
    /// </summary>
    private sealed class FakeBotClient : ITelegramBotClient
    {
        private readonly Queue<Func<Update[]>> _responses = new();

        public List<int?> RequestedOffsets { get; } = [];

        public bool LocalBotServer => false;

        public long BotId => 123456L;

        public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);

        public IExceptionParser ExceptionsParser { get; set; } = new DefaultExceptionParser();

        public event AsyncEventHandler<ApiRequestEventArgs>? OnMakingApiRequest { add { } remove { } }

        public event AsyncEventHandler<ApiResponseEventArgs>? OnApiResponseReceived { add { } remove { } }

        public void Returns(params Update[] updates) => _responses.Enqueue(() => updates);

        public void Fails(Exception error) => _responses.Enqueue(() => throw error);

        public Task<TResponse> SendRequest<TResponse>(IRequest<TResponse> request, CancellationToken cancellationToken = default)
        {
            if (request is GetUpdatesRequest getUpdates)
            {
                RequestedOffsets.Add(getUpdates.Offset);
            }

            var updates = _responses.Count > 0 ? _responses.Dequeue()() : Array.Empty<Update>();
            return Task.FromResult((TResponse)(object)updates);
        }

        public Task<bool> TestApi(CancellationToken cancellationToken = default) => Task.FromResult(true);

        public Task DownloadFile(string filePath, Stream destination, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task DownloadFile(TGFile file, Stream destination, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }
}
