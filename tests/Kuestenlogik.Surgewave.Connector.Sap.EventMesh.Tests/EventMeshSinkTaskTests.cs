using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sap.EventMesh.Tests;

/// <summary>
/// The sink used to run the whole publish - token exchange, POST and status check - inside an
/// empty catch, so a rejected batch was dropped while its offsets were committed. These tests
/// drive the task through a stubbed transport and keep both the CloudEvent envelope and the
/// failure path honest.
/// </summary>
public class EventMeshSinkTaskTests
{
    private const string ServiceUrl = "https://em.test/";
    private const string TokenUrl = "https://auth.test/oauth/token";
    private const string TargetTopic = "orders/created";
    private const string PublishUrl = "https://em.test/messagingrest/v1/topics/orders%2Fcreated/messages";

    [Fact]
    public async Task PutAsync_PublishesACloudEventToTheTargetTopic()
    {
        using var handler = new StubHandler(request => Route(request, HttpStatusCode.OK));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        await task.PutAsync([Record("""{"orderId":4711}""")], CancellationToken.None);

        Assert.Equal(2, handler.Requests.Count);

        var token = handler.Requests[0];
        Assert.Equal(TokenUrl, token.Uri);
        Assert.Contains("grant_type=client_credentials", token.Body, StringComparison.Ordinal);
        Assert.Contains("client_id=client-1", token.Body, StringComparison.Ordinal);

        var publish = handler.Requests[1];
        Assert.Equal(HttpMethod.Post, publish.Method);
        // The topic is a path segment, so a topic containing '/' has to be escaped or it would
        // address a different resource.
        Assert.Equal(PublishUrl, publish.Uri);
        Assert.Equal("Bearer token-1", publish.Authorization);
        Assert.Equal("1", publish.Headers["x-qos"]);
        Assert.Equal("application/cloudevents+json", publish.ContentType);

        using var document = JsonDocument.Parse(publish.Body);
        var root = document.RootElement;
        Assert.Equal("1.0", root.GetProperty("specversion").GetString());
        Assert.Equal("/surgewave/orders", root.GetProperty("source").GetString());
        Assert.Equal("surgewave.order", root.GetProperty("type").GetString());
        Assert.Equal("application/json", root.GetProperty("datacontenttype").GetString());
        Assert.Equal(4711, root.GetProperty("data").GetProperty("orderId").GetInt32());
        Assert.True(DateTimeOffset.TryParse(
            root.GetProperty("time").GetString(),
            CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind,
            out _));
    }

    [Fact]
    public async Task PutAsync_TakesTheCloudEventIdentityFromRecordHeaders()
    {
        using var handler = new StubHandler(request => Route(request, HttpStatusCode.OK));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        // A record that already travelled through Event Mesh keeps its own CloudEvent identity
        // instead of being re-labelled with the connector defaults.
        await task.PutAsync(
            [
                Record("""{"orderId":4711}""", new Dictionary<string, byte[]>
                {
                    ["ce_id"] = "ce-1"u8.ToArray(),
                    ["ce_source"] = "/sap/s4/orders"u8.ToArray(),
                    ["ce_type"] = "com.sap.order.created"u8.ToArray()
                })
            ],
            CancellationToken.None);

        using var document = JsonDocument.Parse(handler.Requests[1].Body);
        var root = document.RootElement;
        Assert.Equal("ce-1", root.GetProperty("id").GetString());
        Assert.Equal("/sap/s4/orders", root.GetProperty("source").GetString());
        Assert.Equal("com.sap.order.created", root.GetProperty("type").GetString());
    }

    [Fact]
    public async Task PutAsync_CarriesNonJsonValuesAsPlainText()
    {
        using var handler = new StubHandler(request => Route(request, HttpStatusCode.OK));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        await task.PutAsync([Record("not json at all")], CancellationToken.None);

        using var document = JsonDocument.Parse(handler.Requests[1].Body);
        Assert.Equal("not json at all", document.RootElement.GetProperty("data").GetString());
    }

    [Fact]
    public async Task PutAsync_PublishesOnceTheBatchSizeIsReached()
    {
        using var handler = new StubHandler(request => Route(request, HttpStatusCode.OK));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var config = SinkConfig();
        config[EventMeshConnectorConfig.BatchSize] = "2";
        task.Start(config);

        await task.PutAsync(
            [
                Record("""{"orderId":1}"""),
                Record("""{"orderId":2}"""),
                Record("""{"orderId":3}"""),
                Record("""{"orderId":4}"""),
                Record("""{"orderId":5}""")
            ],
            CancellationToken.None);

        // One token exchange plus 2 + 2 + 1 published messages.
        Assert.Equal(4, handler.Requests.Count);

        var firstBatch = handler.Requests[1];
        Assert.Equal("application/cloudevents-batch+json", firstBatch.ContentType);
        using var batchDocument = JsonDocument.Parse(firstBatch.Body);
        Assert.Equal(2, batchDocument.RootElement.GetArrayLength());
        Assert.Equal(1, batchDocument.RootElement[0].GetProperty("data").GetProperty("orderId").GetInt32());

        // The trailing partial batch is a single message, not a one-element batch document.
        var remainder = handler.Requests[3];
        Assert.Equal("application/cloudevents+json", remainder.ContentType);
        using var remainderDocument = JsonDocument.Parse(remainder.Body);
        Assert.Equal(JsonValueKind.Object, remainderDocument.RootElement.ValueKind);
        Assert.Equal(5, remainderDocument.RootElement.GetProperty("data").GetProperty("orderId").GetInt32());
    }

    [Fact]
    public async Task PutAsync_SurfacesRejectedPublishes_InsteadOfDroppingTheBatch()
    {
        using var handler = new StubHandler(request => Route(request, HttpStatusCode.BadRequest));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSinkTask(http);

        Exception? raised = null;
        task.Initialize(new TaskContext { RaiseError = ex => raised = ex });
        task.Start(SinkConfig());

        var thrown = await Assert.ThrowsAsync<HttpRequestException>(
            () => task.PutAsync([Record("""{"orderId":4711}""")], CancellationToken.None));

        // The worker has to see the failure so it retries or dead-letters the batch instead of
        // committing offsets for events Event Mesh never accepted.
        Assert.Same(thrown, raised);
    }

    [Fact]
    public async Task PutAsync_SurfacesTokenFailures()
    {
        using var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.Unauthorized));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSinkTask(http);

        Exception? raised = null;
        task.Initialize(new TaskContext { RaiseError = ex => raised = ex });
        task.Start(SinkConfig());

        await Assert.ThrowsAsync<HttpRequestException>(
            () => task.PutAsync([Record("""{"orderId":4711}""")], CancellationToken.None));

        // Bad credentials are a configuration error, not an event to be silently discarded.
        Assert.NotNull(raised);
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task PutAsync_ReusesTheAccessTokenAcrossBatches()
    {
        using var handler = new StubHandler(request => Route(request, HttpStatusCode.OK));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        await task.PutAsync([Record("""{"orderId":1}""")], CancellationToken.None);
        await task.PutAsync([Record("""{"orderId":2}""")], CancellationToken.None);

        Assert.Equal(3, handler.Requests.Count);
        Assert.Single(handler.Requests, r => r.Uri == TokenUrl);
    }

    [Fact]
    public async Task PutAsync_WithNothingToPublish_NeverTalksToEventMesh()
    {
        using var handler = new StubHandler(request => Route(request, HttpStatusCode.OK));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        await task.PutAsync([], CancellationToken.None);
        await task.PutAsync([RecordWithoutValue()], CancellationToken.None);

        // No batch means no token exchange either - a tombstone must not cost a round trip.
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public void Start_WithoutTheTargetTopic_FailsBeforeAnyRequest()
    {
        using var task = new EventMeshSinkTask();
        task.Initialize(new TaskContext());

        var config = SinkConfig();
        config.Remove(EventMeshConnectorConfig.TargetTopic);

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [EventMeshConnectorConfig.ServiceUrl] = ServiceUrl,
        [EventMeshConnectorConfig.TokenUrl] = TokenUrl,
        [EventMeshConnectorConfig.ClientId] = "client-1",
        [EventMeshConnectorConfig.ClientSecret] = "secret-1",
        [EventMeshConnectorConfig.TargetTopic] = TargetTopic,
        [EventMeshConnectorConfig.CloudEventSource] = "/surgewave/orders",
        [EventMeshConnectorConfig.CloudEventType] = "surgewave.order"
    };

    private static SinkRecord Record(string value, IReadOnlyDictionary<string, byte[]>? headers = null) => new()
    {
        Topic = "orders",
        Partition = 0,
        Offset = 1,
        Value = Encoding.UTF8.GetBytes(value),
        Timestamp = DateTimeOffset.UnixEpoch,
        Headers = headers
    };

    private static SinkRecord RecordWithoutValue() => new()
    {
        Topic = "orders",
        Partition = 0,
        Offset = 2,
        Value = null!,
        Timestamp = DateTimeOffset.UnixEpoch
    };

    private static HttpResponseMessage Route(HttpRequestMessage request, HttpStatusCode publishStatus)
        => request.RequestUri?.OriginalString == TokenUrl
            ? Json(HttpStatusCode.OK, """{"access_token":"token-1","expires_in":3600}""")
            : Json(publishStatus, """{"status":"accepted"}""");

    private static HttpResponseMessage Json(HttpStatusCode status, string json)
        => new(status) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    private sealed class CapturedRequest
    {
        public required HttpMethod Method { get; init; }

        public required string Uri { get; init; }

        public required string Body { get; init; }

        public required string? ContentType { get; init; }

        public required string? Authorization { get; init; }

        public required IReadOnlyDictionary<string, string> Headers { get; init; }
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
                Body = request.Content is null ? string.Empty : await request.Content.ReadAsStringAsync(cancellationToken),
                ContentType = request.Content?.Headers.ContentType?.MediaType,
                Authorization = request.Headers.Authorization?.ToString(),
                Headers = request.Headers.ToDictionary(h => h.Key, h => string.Join(",", h.Value), StringComparer.OrdinalIgnoreCase)
            });

            return responder(request);
        }
    }
}
