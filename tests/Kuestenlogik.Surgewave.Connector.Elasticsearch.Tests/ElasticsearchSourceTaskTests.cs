namespace Kuestenlogik.Surgewave.Connector.Elasticsearch.Tests;

using System.Reflection;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class ElasticsearchSourceTaskTests
{
    [Fact]
    public void BuildQuery_WithDslJson_KeepsTheConfiguredQuery()
    {
        using var task = new ElasticsearchSourceTask();
        task.Initialize(CreateTaskContext(null));

        var config = SourceConfig();
        config["elasticsearch.query"] = """{"term":{"status":{"value":"active"}}}""";
        task.Start(config);

        var query = BuildQuery(task);

        Assert.NotNull(query.Term);
        Assert.Equal("status", query.Term!.Field.Name);
    }

    [Fact]
    public void BuildQuery_WithFullSearchBody_UnwrapsTheQueryClause()
    {
        using var task = new ElasticsearchSourceTask();
        task.Initialize(CreateTaskContext(null));

        var config = SourceConfig();
        config["elasticsearch.query"] = """{"query":{"term":{"status":{"value":"active"}}}}""";
        task.Start(config);

        var query = BuildQuery(task);

        Assert.NotNull(query.Term);
        Assert.Equal("status", query.Term!.Field.Name);
    }

    [Fact]
    public void Start_WithMalformedDslJson_ThrowsInsteadOfExportingTheWholeIndex()
    {
        using var task = new ElasticsearchSourceTask();
        task.Initialize(CreateTaskContext(null));

        var config = SourceConfig();
        config["elasticsearch.query"] = "{ this is not a query";

        Assert.Throws<ArgumentException>(() => task.Start(config));
    }

    [Fact]
    public void BuildQuery_WithQueryString_UsesQueryStringQuery()
    {
        using var task = new ElasticsearchSourceTask();
        task.Initialize(CreateTaskContext(null));

        var config = SourceConfig();
        config["elasticsearch.query"] = "status:active";
        task.Start(config);

        var query = BuildQuery(task);

        Assert.NotNull(query.QueryString);
        Assert.Equal("status:active", query.QueryString!.Query);
    }

    [Fact]
    public void BuildQuery_WithTimestampCursor_FiltersGreaterThanInsteadOfEqual()
    {
        var stored = new Dictionary<string, object> { ["last_timestamp"] = "2026-08-20T10:00:00Z" };

        using var task = new ElasticsearchSourceTask();
        task.Initialize(CreateTaskContext(stored));

        var config = SourceConfig();
        config["elasticsearch.incremental.mode"] = "timestamp";
        config["elasticsearch.incremental.field"] = "updated_at";
        task.Start(config);

        var query = BuildQuery(task);

        Assert.NotNull(query.Bool);
        var filter = Assert.Single<Query>(query.Bool!.Filter!);
        var range = Assert.IsType<UntypedRangeQuery>(filter.Range);
        Assert.Equal("updated_at", range.Field.Name);
        Assert.Equal("2026-08-20T10:00:00Z", range.Gt);
    }

    private static Query BuildQuery(ElasticsearchSourceTask task)
        => (Query)typeof(ElasticsearchSourceTask)
            .GetMethod("BuildQuery", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(task, null)!;

    private static TaskContext CreateTaskContext(IDictionary<string, object>? storedOffset)
        => new()
        {
            RaiseError = _ => { },
            OffsetStorageReader = new StubOffsetStorageReader(storedOffset)
        };

    private static Dictionary<string, string> SourceConfig() => new()
    {
        ["elasticsearch.url"] = "http://localhost:9200",
        ["topic"] = "elasticsearch-source",
        ["elasticsearch.index"] = "documents"
    };

    private sealed class StubOffsetStorageReader(IDictionary<string, object>? storedOffset) : IOffsetStorageReader
    {
        public IDictionary<string, object>? Offset(IDictionary<string, object> partition) => storedOffset;

        public IDictionary<IDictionary<string, object>, IDictionary<string, object>> Offsets(
            IReadOnlyCollection<IDictionary<string, object>> partitions)
        {
            var result = new Dictionary<IDictionary<string, object>, IDictionary<string, object>>();

            foreach (var partition in partitions)
            {
                if (storedOffset != null)
                    result[partition] = storedOffset;
            }

            return result;
        }
    }
}
