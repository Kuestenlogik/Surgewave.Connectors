using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sap.OData.Tests;

/// <summary>
/// Tests for <see cref="ODataSinkTask"/>: what reaches the service, what never should, and
/// which failures have to leave <c>PutAsync</c>. A write that fails silently lets the worker
/// commit consumer offsets for entities SAP never stored.
/// </summary>
/// <remarks>
/// Shares a collection with the source tests because Simple.OData.Client keeps a process-wide
/// metadata cache keyed by service URL.
/// </remarks>
[Collection("SapODataClient")]
public class ODataSinkTaskTests
{
    private const string ServiceUrl = "http://sap.invalid/sap/opu/odata/sap/ZORDERS_SRV/";

    [Fact]
    public async Task PutAsync_WhenTheServiceRejectsTheWrite_ThrowsTheErrorItSurfaced()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.InternalServerError, """{"error":"locked"}"""));
        Exception? raised = null;
        using var task = new ODataSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = ex => raised = ex });
        task.Start(SinkConfig());

        var thrown = await Assert.ThrowsAnyAsync<Exception>(
            () => task.PutAsync([Record("""{"OrderId":"4711","Amount":42}""")], TestContext.Current.CancellationToken));

        // A swallowed failure would commit offsets over entities that never reached SAP.
        Assert.Same(thrown, raised);
    }

    [Fact]
    public async Task PutAsync_WithValuesThatCanNeverBecomeEntities_SurfacesThemAndWritesNothing()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, "{}"));
        var errors = new List<Exception>();
        using var task = new ODataSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SinkConfig());

        // Neither value can ever become an entity, so retrying is pointless: both are
        // reported and skipped, and nothing is left over to send.
        await task.PutAsync(
            [Record("this is not json"), Record("[1, 2, 3]")],
            TestContext.Current.CancellationToken);

        Assert.Equal(2, errors.Count);
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task PutAsync_WithNothingToWrite_NeverTalksToTheService()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, "{}"));
        var errors = new List<Exception>();
        using var task = new ODataSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SinkConfig());

        await task.PutAsync([], TestContext.Current.CancellationToken);
        await task.PutAsync([RecordWithoutValue()], TestContext.Current.CancellationToken);
        await task.PutAsync([Record("{}")], TestContext.Current.CancellationToken);

        // A tombstone and an empty object carry no fields, so there is no entity to write
        // and no reason to open a connection to SAP - and neither is an error.
        Assert.Empty(handler.Requests);
        Assert.Empty(errors);
    }

    [Fact]
    public async Task PutAsync_WithMoreThanOneEntity_PropagatesFailuresFromTheBatchPathToo()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.InternalServerError, "{}"));
        Exception? raised = null;
        using var task = new ODataSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = ex => raised = ex });
        task.Start(SinkConfig());

        // More than one entity takes the $batch route instead of single writes; that
        // second path used to swallow its failures just like the first one.
        var thrown = await Assert.ThrowsAnyAsync<Exception>(
            () => task.PutAsync(
                [
                    Record("""{"data":{"OrderId":"4711"}}"""),
                    Record("""{"data":{"OrderId":"4712"}}""")
                ],
                TestContext.Current.CancellationToken));

        Assert.Same(thrown, raised);
    }

    [Theory]
    [InlineData(ODataConnectorConfig.TargetEntitySet)]
    [InlineData(ODataConnectorConfig.ServiceUrl)]
    public void Start_WithoutARequiredKey_FailsBeforeAnyRequest(string missingKey)
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, "{}"));
        using var task = new ODataSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var config = SinkConfig();
        config.Remove(missingKey);

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
        Assert.Empty(handler.Requests);
    }

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [ODataConnectorConfig.Topics] = "orders",
        [ODataConnectorConfig.ServiceUrl] = ServiceUrl,
        [ODataConnectorConfig.TargetEntitySet] = "SalesOrderSet",
        [ODataConnectorConfig.Username] = "SAPUSER",
        [ODataConnectorConfig.Password] = "s3cret"
    };

    private static SinkRecord Record(string value) => new()
    {
        Topic = "orders",
        Partition = 0,
        Offset = 1,
        Value = Encoding.UTF8.GetBytes(value),
        Timestamp = DateTimeOffset.UnixEpoch
    };

    private static SinkRecord RecordWithoutValue() => new()
    {
        Topic = "orders",
        Partition = 0,
        Offset = 2,
        Value = null!,
        Timestamp = DateTimeOffset.UnixEpoch
    };

    private static HttpResponseMessage Json(HttpStatusCode status, string json)
        => new(status) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    private sealed class CapturedRequest
    {
        public required HttpMethod Method { get; init; }

        public required string Uri { get; init; }

        public required string Body { get; init; }
    }

    private sealed class StubHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        public List<CapturedRequest> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(new CapturedRequest
            {
                Method = request.Method,
                Uri = request.RequestUri?.OriginalString ?? string.Empty,
                Body = request.Content is null ? string.Empty : await request.Content.ReadAsStringAsync(cancellationToken)
            });

            return responder(request);
        }
    }
}
