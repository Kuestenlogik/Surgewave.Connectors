using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.RocketChat.Tests;

/// <summary>
/// The source used to start every run at "five minutes ago" and never asked offset storage for
/// its cursor, so any downtime longer than that lost messages. These tests drive the task through
/// a stubbed transport and pin the cursor handling across polls.
/// </summary>
public class RocketChatSourceTaskTests
{
    [Fact]
    public async Task PollAsync_StartsFromTheStoredCursor()
    {
        var now = DateTimeOffset.UtcNow;
        var restored = Truncate(now.AddHours(-1));

        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, History(
            new Message("m-old", "before the cursor", now.AddHours(-2)),
            new Message("m-fresh", "after the cursor", now.AddMinutes(-30)))));
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://rocket.test") };
        using var task = new RocketChatSourceTask(http);
        task.Initialize(new TaskContext { OffsetStorageReader = ReaderAt(restored) });
        task.Start(SourceConfig());

        var records = await task.PollAsync(CancellationToken.None);

        // Without the restored cursor the task would ask for the last five minutes and drop the
        // half-hour-old message entirely.
        var record = Assert.Single(records);
        Assert.Equal("m-fresh", Encoding.UTF8.GetString(record.Key!));

        var request = Assert.Single(handler.Requests);
        Assert.Equal(restored, OldestOf(request.Uri));
        Assert.Equal("room-1", QueryValue(request.Uri, "roomId"));
        Assert.Equal("100", QueryValue(request.Uri, "count"));
        Assert.Equal("token-1", request.Headers["X-Auth-Token"]);
        Assert.Equal("user-1", request.Headers["X-User-Id"]);
    }

    [Fact]
    public async Task PollAsync_WithoutAStoredCursor_LooksBackFiveMinutes()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, History()));
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://rocket.test") };
        using var task = new RocketChatSourceTask(http);
        task.Initialize(new TaskContext());
        task.Start(SourceConfig());

        var before = DateTimeOffset.UtcNow;
        await task.PollAsync(CancellationToken.None);
        var after = DateTimeOffset.UtcNow;

        var oldest = OldestOf(Assert.Single(handler.Requests).Uri);
        Assert.InRange(oldest, before.AddMinutes(-5), after.AddMinutes(-5));
    }

    [Fact]
    public async Task PollAsync_EmitsOneRecordPerMessage_WithItsOwnOffset()
    {
        var now = DateTimeOffset.UtcNow;
        var restored = Truncate(now.AddHours(-1));
        var firstTimestamp = now.AddMinutes(-30);

        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, History(
            new Message("m-1", "hello", firstTimestamp),
            new Message("m-2", "world", now.AddMinutes(-10)))));
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://rocket.test") };
        using var task = new RocketChatSourceTask(http);
        task.Initialize(new TaskContext { OffsetStorageReader = ReaderAt(restored) });
        task.Start(SourceConfig());

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(2, records.Count);

        var first = records[0];
        Assert.Equal("rocketchat-messages", first.Topic);
        Assert.Equal("room-1", first.SourcePartition["room_id"]);
        Assert.Equal("m-1", first.SourceOffset["message_id"]);
        Assert.Equal(firstTimestamp.ToUnixTimeMilliseconds(), first.SourceOffset["ts"]);
        Assert.Equal(1L, first.SourceOffset["offset"]);
        Assert.Equal(2L, records[1].SourceOffset["offset"]);
        Assert.Equal("room-1", Encoding.UTF8.GetString(first.Headers!["rocketchat.room.id"]));
        Assert.Equal("u-1", Encoding.UTF8.GetString(first.Headers!["rocketchat.user.id"]));
        Assert.Contains("\"msg\":\"hello\"", Encoding.UTF8.GetString(first.Value), StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_SkipsBotMessages_ByDefault()
    {
        var now = DateTimeOffset.UtcNow;
        var restored = Truncate(now.AddHours(-1));

        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, History(
            new Message("m-bot", "deploy finished", now.AddMinutes(-20), FromBot: true),
            new Message("m-human", "nice", now.AddMinutes(-10)))));
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://rocket.test") };
        using var task = new RocketChatSourceTask(http);
        task.Initialize(new TaskContext { OffsetStorageReader = ReaderAt(restored) });
        task.Start(SourceConfig());

        var records = await task.PollAsync(CancellationToken.None);

        var record = Assert.Single(records);
        Assert.Equal("m-human", Encoding.UTF8.GetString(record.Key!));
    }

    [Fact]
    public async Task PollAsync_KeepsBotMessages_WhenTheyAreRequested()
    {
        var now = DateTimeOffset.UtcNow;
        var restored = Truncate(now.AddHours(-1));

        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, History(
            new Message("m-bot", "deploy finished", now.AddMinutes(-20), FromBot: true),
            new Message("m-human", "nice", now.AddMinutes(-10)))));
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://rocket.test") };
        using var task = new RocketChatSourceTask(http);
        task.Initialize(new TaskContext { OffsetStorageReader = ReaderAt(restored) });

        var config = SourceConfig();
        config[RocketChatConnectorConfig.IncludeBotMessages] = "true";
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(2, records.Count);
    }

    [Fact]
    public async Task PollAsync_AdvancesTheCursorToTheNewestMessage()
    {
        var now = DateTimeOffset.UtcNow;
        var restored = Truncate(now.AddHours(-1));
        var newest = now.AddMinutes(-10);

        var call = 0;
        using var handler = new StubHandler(_ => ++call == 1
            ? Json(HttpStatusCode.OK, History(
                new Message("m-1", "one", now.AddMinutes(-30)),
                new Message("m-2", "two", newest)))
            : Json(HttpStatusCode.OK, History()));
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://rocket.test") };
        using var task = new RocketChatSourceTask(http);
        task.Initialize(new TaskContext { OffsetStorageReader = ReaderAt(restored) });
        task.Start(SourceConfig());

        Assert.Equal(2, (await task.PollAsync(CancellationToken.None)).Count);
        Assert.Empty(await task.PollAsync(CancellationToken.None));

        // The second poll must not re-request the window it already consumed.
        Assert.Equal(2, handler.Requests.Count);
        Assert.Equal(newest, OldestOf(handler.Requests[1].Uri));
    }

    [Fact]
    public async Task PollAsync_KeepsTheCursor_WhenTheHistoryCallFails()
    {
        var now = DateTimeOffset.UtcNow;
        var restored = Truncate(now.AddHours(-1));

        var call = 0;
        using var handler = new StubHandler(_ => ++call == 1
            ? new HttpResponseMessage(HttpStatusCode.InternalServerError)
            : Json(HttpStatusCode.OK, History(new Message("m-1", "retry me", now.AddMinutes(-30)))));
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://rocket.test") };
        using var task = new RocketChatSourceTask(http);
        task.Initialize(new TaskContext { OffsetStorageReader = ReaderAt(restored) });
        task.Start(SourceConfig());

        Assert.Empty(await task.PollAsync(CancellationToken.None));
        var records = await task.PollAsync(CancellationToken.None);

        // A failed room must leave its cursor untouched so the same window is fetched again.
        var record = Assert.Single(records);
        Assert.Equal("m-1", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal(2, handler.Requests.Count);
        Assert.Equal(OldestOf(handler.Requests[0].Uri), OldestOf(handler.Requests[1].Uri));
    }

    [Fact]
    public void Start_AsksOffsetStorageForEveryRoom()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, History()));
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://rocket.test") };
        using var task = new RocketChatSourceTask(http);

        var reader = new StubOffsetStorageReader(null);
        task.Initialize(new TaskContext { OffsetStorageReader = reader });

        var config = SourceConfig();
        config[RocketChatConnectorConfig.RoomIds] = "room-1, room-2";
        task.Start(config);

        Assert.Equal(2, reader.RequestedPartitions.Count);
        Assert.Equal(
            new[] { "room-1", "room-2" },
            reader.RequestedPartitions
                .Select(p => (string)p["room_id"])
                .OrderBy(r => r, StringComparer.Ordinal));
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [RocketChatConnectorConfig.ServerUrl] = "http://rocket.test",
        [RocketChatConnectorConfig.UserId] = "user-1",
        [RocketChatConnectorConfig.AuthToken] = "token-1",
        [RocketChatConnectorConfig.Topic] = "rocketchat-messages",
        [RocketChatConnectorConfig.RoomIds] = "room-1",
        [RocketChatConnectorConfig.PollIntervalMs] = "0"
    };

    private static StubOffsetStorageReader ReaderAt(DateTimeOffset cursor)
        => new(new Dictionary<string, object> { ["ts"] = cursor.ToUnixTimeMilliseconds() });

    /// <summary>Stored cursors only carry millisecond precision.</summary>
    private static DateTimeOffset Truncate(DateTimeOffset value)
        => DateTimeOffset.FromUnixTimeMilliseconds(value.ToUnixTimeMilliseconds());

    private static string History(params Message[] messages)
    {
        var payload = new
        {
            messages = messages.Select(m => new
            {
                id = m.Id,
                rid = "room-1",
                msg = m.Text,
                ts = m.Timestamp,
                user = new { id = "u-1", username = "bob", name = "Bob" },
                bot = m.FromBot ? new { i = "js" } : null
            }).ToList(),
            success = true
        };

        return JsonSerializer.Serialize(payload);
    }

    private static HttpResponseMessage Json(HttpStatusCode status, string json)
        => new(status) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    private static DateTimeOffset OldestOf(string uri)
        => DateTimeOffset.Parse(QueryValue(uri, "oldest"), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    private static string QueryValue(string uri, string name)
    {
        var query = new Uri(uri).Query.TrimStart('?');

        foreach (var part in query.Split('&'))
        {
            var separator = part.IndexOf('=', StringComparison.Ordinal);
            if (separator > 0 && part[..separator] == name)
            {
                return Uri.UnescapeDataString(part[(separator + 1)..]);
            }
        }

        return string.Empty;
    }

    private sealed record Message(string Id, string Text, DateTimeOffset Timestamp, bool FromBot = false);

    private sealed class StubOffsetStorageReader(IDictionary<string, object>? storedOffset) : IOffsetStorageReader
    {
        public List<IDictionary<string, object>> RequestedPartitions { get; } = [];

        public IDictionary<string, object>? Offset(IDictionary<string, object> partition)
        {
            RequestedPartitions.Add(partition);
            return storedOffset;
        }

        public IDictionary<IDictionary<string, object>, IDictionary<string, object>> Offsets(
            IReadOnlyCollection<IDictionary<string, object>> partitions)
        {
            var result = new Dictionary<IDictionary<string, object>, IDictionary<string, object>>();

            foreach (var partition in partitions)
            {
                var offset = Offset(partition);
                if (offset != null)
                {
                    result[partition] = offset;
                }
            }

            return result;
        }
    }

    private sealed class StubHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        public List<CapturedRequest> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(new CapturedRequest
            {
                Uri = request.RequestUri?.ToString() ?? string.Empty,
                Headers = request.Headers.ToDictionary(h => h.Key, h => string.Join(",", h.Value), StringComparer.OrdinalIgnoreCase)
            });

            return Task.FromResult(responder(request));
        }
    }

    private sealed class CapturedRequest
    {
        public required string Uri { get; init; }

        public required IReadOnlyDictionary<string, string> Headers { get; init; }
    }
}
