using System.Net;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sap.EventMesh.Tests;

/// <summary>
/// The source consumes an Event Mesh queue over REST: it exchanges client credentials for a
/// bearer token, pulls a bounded batch of messages and - in manual acknowledgement mode - leaves
/// them in the queue until <see cref="EventMeshSourceTask.CommitAsync"/> acknowledges them. These
/// tests drive that sequence through a stubbed transport.
/// </summary>
public class EventMeshSourceTaskTests
{
    private const string ServiceUrl = "https://em.test/";
    private const string TokenUrl = "https://auth.test/oauth/token";
    private const string QueueName = "surgewave/orders";
    private const string ConsumptionUrl = "https://em.test/messagingrest/v1/queues/surgewave%2Forders/messages/consumption";
    private const string AcknowledgementUrl = "https://em.test/messagingrest/v1/queues/surgewave%2Forders/messages/acknowledgement";

    /// <summary>
    /// Consumption payload as the task parses it - property names are matched case-sensitively
    /// against the response model.
    /// </summary>
    private const string ConsumptionResponse = """
        {"Messages":[
          {"MessageId":"em-1","Data":{"orderId":4711},"CeId":"ce-1","CeType":"com.sap.order.created","CeSource":"/sap/s4/orders","CeSpecVersion":"1.0","CeTime":"2026-08-21T10:00:00Z"},
          {"MessageId":"em-2","Data":{"orderId":4712}},
          {"MessageId":"em-3"}
        ]}
        """;

    [Fact]
    public async Task PollAsync_ConsumesTheQueueWithABoundedBatch()
    {
        using var handler = new StubHandler(request => Route(request, HttpStatusCode.OK, ConsumptionResponse));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSourceTask(http);
        task.Initialize(new TaskContext());
        task.Start(SourceConfig());

        await task.PollAsync(CancellationToken.None);

        Assert.Equal(2, handler.Requests.Count);

        var token = handler.Requests[0];
        Assert.Equal(TokenUrl, token.Uri);
        Assert.Contains("grant_type=client_credentials", token.Body, StringComparison.Ordinal);

        var consumption = handler.Requests[1];
        Assert.Equal(HttpMethod.Post, consumption.Method);
        // The queue name is a path segment, so a '/' inside it has to stay escaped.
        Assert.Equal(ConsumptionUrl, consumption.Uri);
        Assert.Equal("Bearer token-1", consumption.Authorization);
        // x-qos 1 is at-least-once delivery - without it the broker would forget the message.
        Assert.Equal("1", consumption.Headers["x-qos"]);
        Assert.Equal("""{"maxMessages":25}""", consumption.Body);
    }

    [Fact]
    public async Task PollAsync_MapsEveryMessageToItsOwnRecord()
    {
        using var handler = new StubHandler(request => Route(request, HttpStatusCode.OK, ConsumptionResponse));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSourceTask(http);
        task.Initialize(new TaskContext());
        task.Start(SourceConfig());

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(3, records.Count);

        var first = records[0];
        Assert.Equal("eventmesh-events", first.Topic);
        Assert.Equal(QueueName, first.SourcePartition["queue"]);
        Assert.Equal("eventmesh", first.SourcePartition["source"]);
        // The CloudEvent id identifies the event end to end, so it wins over the broker's id.
        Assert.Equal("ce-1", Encoding.UTF8.GetString(first.Key!));
        Assert.Equal("""{"orderId":4711}""", Encoding.UTF8.GetString(first.Value));
        Assert.Equal("com.sap.order.created", Encoding.UTF8.GetString(first.Headers!["ce_type"]));
        Assert.Equal("/sap/s4/orders", Encoding.UTF8.GetString(first.Headers!["ce_source"]));
        Assert.Equal("1.0", Encoding.UTF8.GetString(first.Headers!["ce_specversion"]));
        Assert.Equal("em-1", Encoding.UTF8.GetString(first.Headers!["eventmesh.message_id"]));
        Assert.Equal(QueueName, Encoding.UTF8.GetString(first.Headers!["eventmesh.queue"]));

        // Every record carries its own offset so a restart does not replay the whole batch.
        Assert.Equal(1L, first.SourceOffset["message_id"]);
        Assert.Equal("em-1", first.SourceOffset["eventmesh_id"]);
        Assert.Equal(2L, records[1].SourceOffset["message_id"]);
        Assert.Equal(3L, records[2].SourceOffset["message_id"]);

        // Without a CloudEvent id the broker's message id keys the record.
        Assert.Equal("em-2", Encoding.UTF8.GetString(records[1].Key!));
        Assert.DoesNotContain("ce_type", records[1].Headers!.Keys);

        // A message without a payload still produces a record, with an empty value.
        Assert.Empty(records[2].Value);
    }

    [Fact]
    public async Task PollAsync_HonoursThePollInterval()
    {
        using var handler = new StubHandler(request => Route(request, HttpStatusCode.OK, ConsumptionResponse));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSourceTask(http);
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config[EventMeshConnectorConfig.PollIntervalMs] = "60000";
        task.Start(config);

        Assert.Equal(3, (await task.PollAsync(CancellationToken.None)).Count);

        // The worker calls PollAsync in a tight loop; the second call inside the interval must
        // return without hammering the queue again.
        Assert.Empty(await task.PollAsync(CancellationToken.None));
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task PollAsync_ReusesTheAccessTokenAcrossPolls()
    {
        using var handler = new StubHandler(request => Route(request, HttpStatusCode.OK, ConsumptionResponse));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSourceTask(http);
        task.Initialize(new TaskContext());
        task.Start(SourceConfig());

        await task.PollAsync(CancellationToken.None);
        await task.PollAsync(CancellationToken.None);

        Assert.Equal(3, handler.Requests.Count);
        Assert.Single(handler.Requests, r => r.Uri == TokenUrl);
    }

    [Fact]
    public async Task CommitAsync_AcknowledgesTheConsumedMessagesOnce()
    {
        using var handler = new StubHandler(request => Route(request, HttpStatusCode.OK, ConsumptionResponse));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSourceTask(http);
        task.Initialize(new TaskContext());
        task.Start(SourceConfig());

        await task.PollAsync(CancellationToken.None);
        await task.CommitAsync(CancellationToken.None);

        Assert.Equal(3, handler.Requests.Count);
        var acknowledgement = handler.Requests[2];
        Assert.Equal(HttpMethod.Post, acknowledgement.Method);
        Assert.Equal(AcknowledgementUrl, acknowledgement.Uri);
        Assert.Equal("Bearer token-1", acknowledgement.Authorization);

        using var document = JsonDocument.Parse(acknowledgement.Body);
        var ids = document.RootElement.GetProperty("messageIds").EnumerateArray().Select(e => e.GetString()!).ToList();
        Assert.Equal(new[] { "em-1", "em-2", "em-3" }, ids);

        // A second commit without a poll in between has nothing left to acknowledge.
        await task.CommitAsync(CancellationToken.None);
        Assert.Equal(3, handler.Requests.Count);
    }

    [Fact]
    public async Task CommitAsync_InAutoAckMode_SendsNothing()
    {
        using var handler = new StubHandler(request => Route(request, HttpStatusCode.OK, ConsumptionResponse));
        using var http = new HttpClient(handler);
        using var task = new EventMeshSourceTask(http);
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config[EventMeshConnectorConfig.AckMode] = "auto";
        task.Start(config);

        await task.PollAsync(CancellationToken.None);
        await task.CommitAsync(CancellationToken.None);

        // In auto mode the broker acknowledges on delivery, so the task must not send a second
        // acknowledgement of its own.
        Assert.Equal(2, handler.Requests.Count);
        Assert.DoesNotContain(handler.Requests, r => r.Uri == AcknowledgementUrl);
    }

    [Fact]
    public void Start_WithoutTheQueueName_FailsBeforeAnyRequest()
    {
        using var task = new EventMeshSourceTask();
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config.Remove(EventMeshConnectorConfig.QueueName);

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [EventMeshConnectorConfig.Topic] = "eventmesh-events",
        [EventMeshConnectorConfig.ServiceUrl] = ServiceUrl,
        [EventMeshConnectorConfig.TokenUrl] = TokenUrl,
        [EventMeshConnectorConfig.ClientId] = "client-1",
        [EventMeshConnectorConfig.ClientSecret] = "secret-1",
        [EventMeshConnectorConfig.QueueName] = QueueName,
        [EventMeshConnectorConfig.PollIntervalMs] = "0",
        [EventMeshConnectorConfig.MaxMessages] = "25"
    };

    private static HttpResponseMessage Route(HttpRequestMessage request, HttpStatusCode status, string consumptionBody)
        => request.RequestUri?.OriginalString == TokenUrl
            ? Json(HttpStatusCode.OK, """{"access_token":"token-1","expires_in":3600}""")
            : Json(status, consumptionBody);

    private static HttpResponseMessage Json(HttpStatusCode status, string json)
        => new(status) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    private sealed class CapturedRequest
    {
        public required HttpMethod Method { get; init; }

        public required string Uri { get; init; }

        public required string Body { get; init; }

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
                Authorization = request.Headers.Authorization?.ToString(),
                Headers = request.Headers.ToDictionary(h => h.Key, h => string.Join(",", h.Value), StringComparer.OrdinalIgnoreCase)
            });

            return responder(request);
        }
    }
}
