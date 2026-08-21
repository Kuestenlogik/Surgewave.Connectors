using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Wikipedia.Tests;

/// <summary>
/// Drives the task against a stubbed MediaWiki endpoint: the recent-changes cursor, the
/// request shapes and the error path, all without touching the network.
/// </summary>
public class WikipediaSourceTaskTests
{
    private const string ChangesJson = """
        {"query":{"recentchanges":[
        {"revid":11,"title":"Alpha","user":"ann","timestamp":"2026-08-20T12:00:00Z","comment":"c1","oldlen":10,"newlen":20},
        {"revid":10,"title":"Beta","user":"bob","timestamp":"2026-08-20T11:00:00Z","comment":"c2","oldlen":5,"newlen":7}]}}
        """;

    private const string EmptyChangesJson = """{"query":{"recentchanges":[]}}""";
    private const string CategoryMembersJson = """{"query":{"categorymembers":[{"title":"Quantum"},{"title":"Relativity"}]}}""";
    private const string SearchJson = """{"query":{"search":[{"title":"Apache"},{"title":"Kafka"}]}}""";
    private const string PageInfoJson = """{"query":{"pages":{"1":{"title":"Quantum"}}}}""";

    [Fact]
    public async Task Start_RestoresTheChangesCursorAndPollWalksForwardFromIt()
    {
        var reader = new FakeOffsetStorageReader(new Dictionary<string, object>
        {
            [WikipediaConnectorConfig.OffsetTimestamp] = "2026-08-19T09:30:00Z"
        });
        using var handler = new StubHttpHandler(_ => JsonResponse(EmptyChangesJson));
        using var task = new WikipediaSourceTask(handler);
        task.Initialize(new TaskContext { OffsetStorageReader = reader });
        task.Start(ChangesConfig());

        await task.PollAsync(TestContext.Current.CancellationToken);

        // rcdir=newer is what makes rcstart a resume point; with the API default (rcdir=older)
        // the very same parameter walks backwards into history and never returns a new edit.
        var url = Assert.Single(handler.Requests);
        Assert.Contains("rcdir=newer", url, StringComparison.Ordinal);
        Assert.Contains("rcstart=2026-08-19T09:30:00Z", url, StringComparison.Ordinal);
        Assert.Equal("wikipedia", reader.RequestedPartition!["source"]);
        Assert.Equal("changes", reader.RequestedPartition!["mode"]);
    }

    [Fact]
    public async Task PollAsync_ChangesMode_AdvancesTheCursorToTheNewestTimestampSeen()
    {
        var responses = new Queue<string>([ChangesJson, EmptyChangesJson]);
        using var handler = new StubHttpHandler(_ => JsonResponse(responses.Dequeue()));
        using var task = new WikipediaSourceTask(handler);
        task.Start(ChangesConfig());

        var first = await task.PollAsync(TestContext.Current.CancellationToken);
        await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, first.Count);
        Assert.DoesNotContain("rcstart", handler.Requests[0], StringComparison.Ordinal);

        // The batch ends on the OLDEST change (11:00); the cursor must still be the newest one.
        Assert.Contains("rcstart=2026-08-20T12:00:00Z", handler.Requests[1], StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_ChangesMode_GivesEveryRecordItsOwnRevisionOffset()
    {
        using var handler = new StubHttpHandler(_ => JsonResponse(ChangesJson));
        using var task = new WikipediaSourceTask(handler);
        task.Start(ChangesConfig());

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, records.Count);
        Assert.Equal(11, records[0].SourceOffset[WikipediaConnectorConfig.OffsetRevisionId]);
        Assert.Equal("2026-08-20T12:00:00Z", records[0].SourceOffset[WikipediaConnectorConfig.OffsetTimestamp]);
        Assert.Equal(10, records[1].SourceOffset[WikipediaConnectorConfig.OffsetRevisionId]);
        Assert.Equal("2026-08-20T11:00:00Z", records[1].SourceOffset[WikipediaConnectorConfig.OffsetTimestamp]);
        Assert.Equal("11", Encoding.UTF8.GetString(records[0].Key!));
        Assert.Equal("wiki", records[0].Topic);
        Assert.Equal("recent_change", HeaderValue(records[0], "wikipedia.type"));
        Assert.Contains("\"title\":\"Alpha\"", Encoding.UTF8.GetString(records[0].Value), StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_ChangesMode_DoesNotRedeliverRevisionsItAlreadyProduced()
    {
        using var handler = new StubHttpHandler(_ => JsonResponse(ChangesJson));
        using var task = new WikipediaSourceTask(handler);
        task.Start(ChangesConfig());

        var first = await task.PollAsync(TestContext.Current.CancellationToken);
        var second = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, first.Count);
        Assert.Empty(second);
    }

    [Fact]
    public async Task PollAsync_WhenTheApiFails_RaisesTheErrorInsteadOfProducingNothingForever()
    {
        var errors = new List<Exception>();
        using var handler = new StubHttpHandler(_ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
        using var task = new WikipediaSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(ChangesConfig());

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Empty(records);
        var error = Assert.Single(errors);
        Assert.IsType<HttpRequestException>(error);
    }

    [Fact]
    public async Task PollAsync_PageMode_TurnsConfiguredCategoriesIntoPageFetches()
    {
        using var handler = new StubHttpHandler(uri =>
            uri.Query.Contains("list=categorymembers", StringComparison.Ordinal)
                ? JsonResponse(CategoryMembersJson)
                : JsonResponse(PageInfoJson));
        using var task = new WikipediaSourceTask(handler);
        var config = ChangesConfig();
        config[WikipediaConnectorConfig.Mode] = "page";
        config[WikipediaConnectorConfig.Categories] = "Physics";
        task.Start(config);

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Equal(3, handler.Requests.Count);
        Assert.Contains("cmtitle=Category:Physics", handler.Requests[0], StringComparison.Ordinal);
        Assert.Contains("cmlimit=50", handler.Requests[0], StringComparison.Ordinal);
        Assert.Contains("titles=Quantum", handler.Requests[1], StringComparison.Ordinal);
        Assert.Contains("titles=Relativity", handler.Requests[2], StringComparison.Ordinal);
        Assert.Equal(2, records.Count);
        Assert.Equal("Quantum", Encoding.UTF8.GetString(records[0].Key!));
        Assert.Equal("page", HeaderValue(records[0], "wikipedia.mode"));
    }

    [Fact]
    public async Task PollAsync_PageMode_AsksForContentLinksAndImagesWhenConfigured()
    {
        using var handler = new StubHttpHandler(_ => JsonResponse(PageInfoJson));
        using var task = new WikipediaSourceTask(handler);
        var config = ChangesConfig();
        config[WikipediaConnectorConfig.Mode] = "page";
        config[WikipediaConnectorConfig.PageTitles] = "Quantum";
        config[WikipediaConnectorConfig.IncludeContent] = "true";
        config[WikipediaConnectorConfig.IncludeLinks] = "true";
        config[WikipediaConnectorConfig.IncludeImages] = "true";
        config[WikipediaConnectorConfig.IncludeExtract] = "false";
        config[WikipediaConnectorConfig.IncludeCategories] = "false";
        task.Start(config);

        await task.PollAsync(TestContext.Current.CancellationToken);

        var url = Assert.Single(handler.Requests);
        Assert.Contains("prop=info|revisions|links|images", url, StringComparison.Ordinal);
        Assert.Contains("rvprop=content&rvslots=main", url, StringComparison.Ordinal);
        Assert.DoesNotContain("exintro", url, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_SearchMode_FetchesPageInfoForEveryHit()
    {
        using var handler = new StubHttpHandler(uri =>
            uri.Query.Contains("list=search", StringComparison.Ordinal)
                ? JsonResponse(SearchJson)
                : JsonResponse(PageInfoJson));
        using var task = new WikipediaSourceTask(handler);
        var config = ChangesConfig();
        config[WikipediaConnectorConfig.Mode] = "search";
        config[WikipediaConnectorConfig.SearchQuery] = "kafka";
        config[WikipediaConnectorConfig.Language] = "de";
        task.Start(config);

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Contains("de.wikipedia.org", handler.Requests[0], StringComparison.Ordinal);
        Assert.Contains("srsearch=kafka", handler.Requests[0], StringComparison.Ordinal);
        Assert.Equal(3, handler.Requests.Count);
        Assert.Equal(2, records.Count);
        Assert.Equal("Apache", Encoding.UTF8.GetString(records[0].Key!));
        Assert.Equal("search", HeaderValue(records[0], "wikipedia.mode"));
        Assert.Equal("de", HeaderValue(records[0], "wikipedia.language"));
    }

    private static string HeaderValue(SourceRecord record, string name) =>
        Encoding.UTF8.GetString(record.Headers![name]);

    private static HttpResponseMessage JsonResponse(string body) =>
        new(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    private static Dictionary<string, string> ChangesConfig() => new()
    {
        [WikipediaConnectorConfig.Topic] = "wiki",
        [WikipediaConnectorConfig.Mode] = "changes",
        [WikipediaConnectorConfig.PollIntervalMs] = "0"
    };

    /// <summary>Answers every request from a canned responder and records the URLs it saw.</summary>
    private sealed class StubHttpHandler : HttpMessageHandler
    {
        private readonly Func<Uri, HttpResponseMessage> _respond;

        public StubHttpHandler(Func<Uri, HttpResponseMessage> respond) => _respond = respond;

        /// <summary>Requested URLs, unescaped so assertions can read the MediaWiki parameters.</summary>
        public List<string> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var uri = request.RequestUri!;
            Requests.Add(Uri.UnescapeDataString(uri.ToString()));
            return Task.FromResult(_respond(uri));
        }
    }

    /// <summary>Hands out one canned offset and remembers which partition was asked for.</summary>
    private sealed class FakeOffsetStorageReader : IOffsetStorageReader
    {
        private readonly IDictionary<string, object>? _offset;

        public FakeOffsetStorageReader(IDictionary<string, object>? offset) => _offset = offset;

        public IDictionary<string, object>? RequestedPartition { get; private set; }

        public IDictionary<string, object>? Offset(IDictionary<string, object> partition)
        {
            RequestedPartition = partition;
            return _offset;
        }

        public IDictionary<IDictionary<string, object>, IDictionary<string, object>> Offsets(
            IReadOnlyCollection<IDictionary<string, object>> partitions) =>
            new Dictionary<IDictionary<string, object>, IDictionary<string, object>>();
    }
}
