using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Mattermost.Tests;

/// <summary>
/// The source used to keep its per-channel cursor only in memory, so a restart longer than the
/// five-minute look-back skipped every post written while the task was down, and an empty
/// 'mattermost.channel.ids' polled nothing at all instead of every channel the token can see.
/// These tests drive the task through a stubbed transport and pin both behaviours.
/// </summary>
public class MattermostSourceTaskTests
{
    private const string TeamsPath = "/api/v4/users/me/teams";

    /// <summary>2023-11-14T22:13:20Z, in the Mattermost epoch-millisecond format.</summary>
    private const long Cursor = 1_700_000_000_000;

    [Fact]
    public async Task PollAsync_StartsFromTheStoredCursor()
    {
        using var handler = new StubHandler(_ => Json(Posts(
            new Post("p-old", "written before the cursor", Cursor - 60_000),
            new Post("p-fresh", "written after the cursor", Cursor + 60_000))));
        using var http = Client(handler);
        using var task = new MattermostSourceTask(http);

        var reader = new StubOffsetStorageReader(new Dictionary<string, object> { ["create_at"] = Cursor });
        task.Initialize(new TaskContext { OffsetStorageReader = reader });
        task.Start(SourceConfig());

        var records = await task.PollAsync(CancellationToken.None);

        // Without the restored cursor the task would ask for the last five minutes and never see
        // a post that is more than a year old.
        var record = Assert.Single(records);
        Assert.Equal("p-fresh", Encoding.UTF8.GetString(record.Key!));

        var request = Assert.Single(handler.Requests);
        Assert.Equal("/api/v4/channels/channel-1/posts", request.Path);
        Assert.Equal(Cursor, SinceOf(request.Uri));
        Assert.Equal("Bearer token-1", request.Authorization);

        var partition = Assert.Single(reader.RequestedPartitions);
        Assert.Equal("channel-1", (string)partition["channel_id"]);
    }

    [Fact]
    public async Task PollAsync_WithoutAStoredCursor_LooksBackFiveMinutes()
    {
        using var handler = new StubHandler(_ => Json(Posts()));
        using var http = Client(handler);
        using var task = new MattermostSourceTask(http);
        task.Initialize(new TaskContext());
        task.Start(SourceConfig());

        var before = DateTimeOffset.UtcNow;
        await task.PollAsync(CancellationToken.None);
        var after = DateTimeOffset.UtcNow;

        var since = SinceOf(Assert.Single(handler.Requests).Uri);
        Assert.InRange(
            since,
            before.AddMinutes(-5).ToUnixTimeMilliseconds(),
            after.AddMinutes(-5).ToUnixTimeMilliseconds());
    }

    [Fact]
    public async Task PollAsync_EmitsOneRecordPerPost_WithItsOwnOffset()
    {
        using var handler = new StubHandler(_ => Json(Posts(
            new Post("p-1", "hello", Cursor + 1_000),
            new Post("p-2", "world", Cursor + 2_000))));
        using var http = Client(handler);
        using var task = new MattermostSourceTask(http);
        task.Initialize(new TaskContext { OffsetStorageReader = ReaderAt(Cursor) });
        task.Start(SourceConfig());

        var records = (await task.PollAsync(CancellationToken.None))
            .OrderBy(r => (long)r.SourceOffset["create_at"])
            .ToList();

        Assert.Equal(2, records.Count);

        var first = records[0];
        Assert.Equal("mattermost-messages", first.Topic);
        Assert.Equal("channel-1", (string)first.SourcePartition["channel_id"]);
        Assert.Equal("p-1", (string)first.SourceOffset["post_id"]);
        Assert.Equal(Cursor + 1_000, (long)first.SourceOffset["create_at"]);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(Cursor + 1_000), first.Timestamp);
        Assert.Equal("channel-1", Encoding.UTF8.GetString(first.Headers!["mattermost.channel.id"]));
        Assert.Equal("p-1", Encoding.UTF8.GetString(first.Headers!["mattermost.post.id"]));
        Assert.Equal("u-1", Encoding.UTF8.GetString(first.Headers!["mattermost.user.id"]));
        Assert.Contains("\"message\":\"hello\"", Encoding.UTF8.GetString(first.Value), StringComparison.Ordinal);

        // Each record carries its own cursor value, so a partial commit resumes in the right place.
        Assert.Equal(Cursor + 2_000, (long)records[1].SourceOffset["create_at"]);
        Assert.NotEqual((long)records[0].SourceOffset["message_id"], (long)records[1].SourceOffset["message_id"]);
    }

    [Theory]
    [InlineData("false", 1)]
    [InlineData("true", 2)]
    public async Task PollAsync_AppliesTheBotFilter(string includeBots, int expected)
    {
        using var handler = new StubHandler(_ => Json(Posts(
            new Post("p-bot", "deploy finished", Cursor + 1_000, FromBot: true),
            new Post("p-human", "nice", Cursor + 2_000))));
        using var http = Client(handler);
        using var task = new MattermostSourceTask(http);
        task.Initialize(new TaskContext { OffsetStorageReader = ReaderAt(Cursor) });

        var config = SourceConfig();
        config[MattermostConnectorConfig.IncludeBotMessages] = includeBots;
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(expected, records.Count);
        Assert.Contains(records, r => Encoding.UTF8.GetString(r.Key!) == "p-human");
    }

    [Fact]
    public async Task PollAsync_AdvancesTheCursorToTheNewestPost()
    {
        var call = 0;
        using var handler = new StubHandler(_ => ++call == 1
            ? Json(Posts(
                new Post("p-1", "one", Cursor + 1_000),
                new Post("p-2", "two", Cursor + 9_000)))
            : Json(Posts()));
        using var http = Client(handler);
        using var task = new MattermostSourceTask(http);
        task.Initialize(new TaskContext { OffsetStorageReader = ReaderAt(Cursor) });
        task.Start(SourceConfig());

        Assert.Equal(2, (await task.PollAsync(CancellationToken.None)).Count);
        Assert.Empty(await task.PollAsync(CancellationToken.None));

        // The second poll must not re-request the window it already consumed.
        Assert.Equal(2, handler.Requests.Count);
        Assert.Equal(Cursor, SinceOf(handler.Requests[0].Uri));
        Assert.Equal(Cursor + 9_000, SinceOf(handler.Requests[1].Uri));
    }

    [Fact]
    public async Task PollAsync_RaisesTheError_AndKeepsTheCursor_WhenTheFetchFails()
    {
        var call = 0;
        using var handler = new StubHandler(_ => ++call == 1
            ? new HttpResponseMessage(HttpStatusCode.InternalServerError)
            : Json(Posts(new Post("p-1", "retry me", Cursor + 1_000))));
        using var http = Client(handler);
        using var task = new MattermostSourceTask(http);

        Exception? raised = null;
        task.Initialize(new TaskContext
        {
            OffsetStorageReader = ReaderAt(Cursor),
            RaiseError = ex => raised = ex
        });
        task.Start(SourceConfig());

        Assert.Empty(await task.PollAsync(CancellationToken.None));

        // A failed fetch has to stay visible instead of degenerating into a silent empty poll.
        var error = Assert.IsType<HttpRequestException>(raised);
        Assert.Contains("channel-1", error.Message, StringComparison.Ordinal);

        var records = await task.PollAsync(CancellationToken.None);

        // ... and it must leave the cursor untouched so the same window is fetched again.
        Assert.Equal("p-1", Encoding.UTF8.GetString(Assert.Single(records).Key!));
        Assert.Equal(SinceOf(handler.Requests[0].Uri), SinceOf(handler.Requests[1].Uri));
    }

    [Fact]
    public async Task PollAsync_DiscoversChannels_WhenNoneAreConfigured()
    {
        using var handler = new StubHandler(uri => uri.AbsolutePath switch
        {
            TeamsPath => Json("""[{"id":"team-1"}]"""),
            "/api/v4/users/me/teams/team-1/channels" => Json("""[{"id":"c-1"},{"id":"c-2"}]"""),
            _ => Json(Posts())
        });
        using var http = Client(handler);
        using var task = new MattermostSourceTask(http);
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config[MattermostConnectorConfig.ChannelIds] = "";
        task.Start(config);

        await task.PollAsync(CancellationToken.None);
        await task.PollAsync(CancellationToken.None);

        // An empty filter means "every channel the token can see" - not "poll nothing".
        Assert.Equal(
            new[] { "c-1", "c-2" },
            handler.Requests
                .Where(r => r.Path.EndsWith("/posts", StringComparison.Ordinal))
                .Select(r => r.Path.Split('/')[4])
                .Distinct(StringComparer.Ordinal)
                .Order(StringComparer.Ordinal));

        // The discovered list is reused for five minutes instead of re-scanning on every poll.
        Assert.Equal(1, handler.Requests.Count(r => r.Path == TeamsPath));
    }

    [Fact]
    public async Task PollAsync_WithConfiguredChannels_NeverListsTheAccountTeams()
    {
        using var handler = new StubHandler(_ => Json(Posts()));
        using var http = Client(handler);
        using var task = new MattermostSourceTask(http);
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config[MattermostConnectorConfig.ChannelIds] = "channel-1, channel-2";
        task.Start(config);

        await task.PollAsync(CancellationToken.None);

        Assert.DoesNotContain(handler.Requests, r => r.Path == TeamsPath);
        Assert.Equal(2, handler.Requests.Count);
    }

    private static Dictionary<string, string> SourceConfig() => new(StringComparer.Ordinal)
    {
        [MattermostConnectorConfig.ServerUrl] = "http://mattermost.test",
        [MattermostConnectorConfig.AccessToken] = "token-1",
        [MattermostConnectorConfig.Topic] = "mattermost-messages",
        [MattermostConnectorConfig.ChannelIds] = "channel-1",
        [MattermostConnectorConfig.PollIntervalMs] = "0"
    };

    private static HttpClient Client(StubHandler handler)
        => new(handler) { BaseAddress = new Uri("http://mattermost.test") };

    private static StubOffsetStorageReader ReaderAt(long cursor)
        => new(new Dictionary<string, object> { ["create_at"] = cursor });

    private static long SinceOf(string uri)
    {
        var query = new Uri(uri).Query.TrimStart('?');

        foreach (var part in query.Split('&'))
        {
            var separator = part.IndexOf('=', StringComparison.Ordinal);
            if (separator > 0 && part[..separator] == "since")
            {
                return long.Parse(part[(separator + 1)..], CultureInfo.InvariantCulture);
            }
        }

        return -1;
    }

    private static string Posts(params Post[] posts)
    {
        var payload = new
        {
            order = posts.Select(p => p.Id).ToList(),
            posts = posts.ToDictionary(
                p => p.Id,
                p => new
                {
                    id = p.Id,
                    channel_id = "channel-1",
                    user_id = "u-1",
                    message = p.Message,
                    create_at = p.CreateAt,
                    type = "",
                    props = p.FromBot ? new Dictionary<string, string> { ["from_bot"] = "true" } : null
                },
                StringComparer.Ordinal)
        };

        return JsonSerializer.Serialize(payload);
    }

    private static HttpResponseMessage Json(string json)
        => new(HttpStatusCode.OK) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    private sealed record Post(string Id, string Message, long CreateAt, bool FromBot = false);

    private sealed class CapturedRequest
    {
        public required string Uri { get; init; }

        public required string Path { get; init; }

        public required string? Authorization { get; init; }
    }

    private sealed class StubHandler(Func<Uri, HttpResponseMessage> responder) : HttpMessageHandler
    {
        public List<CapturedRequest> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var uri = request.RequestUri!;

            Requests.Add(new CapturedRequest
            {
                Uri = uri.ToString(),
                Path = uri.AbsolutePath,
                Authorization = request.Headers.Authorization?.ToString()
            });

            return Task.FromResult(responder(uri));
        }
    }

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
}
