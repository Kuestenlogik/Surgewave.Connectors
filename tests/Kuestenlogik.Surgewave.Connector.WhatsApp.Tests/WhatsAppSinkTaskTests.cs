using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.WhatsApp.Tests;

/// <summary>
/// Drives the sink against a stubbed Cloud API: the request shape it sends and - more
/// importantly - that a rejected send fails the batch instead of being dropped silently.
/// </summary>
public class WhatsAppSinkTaskTests
{
    [Fact]
    public async Task PutAsync_WhenTheApiRejectsTheSend_ThrowsSoTheWorkerCanRetryOrDlq()
    {
        var errors = new List<Exception>();
        using var handler = new RecordingHandler(_ => new HttpResponseMessage(HttpStatusCode.Unauthorized));
        using var task = new WhatsAppSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SinkConfig());

        var thrown = await Assert.ThrowsAsync<HttpRequestException>(
            () => task.PutAsync([Record("""{"to":"4915100000","text":"hi"}""")], TestContext.Current.CancellationToken));

        var error = Assert.Single(errors);
        Assert.Same(thrown, error);
    }

    [Fact]
    public async Task PutAsync_StopsAtTheFirstFailedRecordSoLaterOnesAreNotReportedAsSent()
    {
        using var handler = new RecordingHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError));
        using var task = new WhatsAppSinkTask(handler);
        task.Start(SinkConfig());

        await Assert.ThrowsAsync<HttpRequestException>(() => task.PutAsync(
            [
                Record("""{"to":"4915100000","text":"first"}"""),
                Record("""{"to":"4915100001","text":"second"}""", 1)
            ],
            TestContext.Current.CancellationToken));

        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task PutAsync_SendsATextMessageToThePhoneNumberEndpointWithBearerAuth()
    {
        using var handler = new RecordingHandler(_ => Accepted());
        using var task = new WhatsAppSinkTask(handler);
        task.Start(SinkConfig());

        await task.PutAsync([Record("""{"to":"4915100000","text":"hello there"}""")], TestContext.Current.CancellationToken);

        Assert.Equal("https://graph.facebook.com/v18.0/123456/messages", Assert.Single(handler.Requests));
        Assert.Equal(HttpMethod.Post, Assert.Single(handler.Methods));
        Assert.Equal("Bearer test-token", handler.AuthorizationHeader);

        var body = Assert.Single(handler.Bodies);
        Assert.Contains("\"to\":\"4915100000\"", body, StringComparison.Ordinal);
        Assert.Contains("\"type\":\"text\"", body, StringComparison.Ordinal);
        Assert.Contains("\"body\":\"hello there\"", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_HonoursTheConfiguredApiVersion()
    {
        using var handler = new RecordingHandler(_ => Accepted());
        using var task = new WhatsAppSinkTask(handler);
        var config = SinkConfig();
        config[WhatsAppConnectorConfig.ApiVersion] = "v20.0";
        task.Start(config);

        await task.PutAsync([Record("""{"to":"4915100000","text":"hi"}""")], TestContext.Current.CancellationToken);

        Assert.Equal("https://graph.facebook.com/v20.0/123456/messages", Assert.Single(handler.Requests));
    }

    [Fact]
    public async Task PutAsync_WithoutAnyRecipient_SkipsTheRecordInsteadOfSending()
    {
        using var handler = new RecordingHandler(_ => Accepted());
        using var task = new WhatsAppSinkTask(handler);
        task.Start(SinkConfig());

        await task.PutAsync([Record("""{"text":"nobody to send this to"}""")], TestContext.Current.CancellationToken);

        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task PutAsync_FallsBackToTheDefaultRecipientWhenThePayloadHasNone()
    {
        using var handler = new RecordingHandler(_ => Accepted());
        using var task = new WhatsAppSinkTask(handler);
        var config = SinkConfig();
        config[WhatsAppConnectorConfig.DefaultRecipient] = "4915199999";
        task.Start(config);

        await task.PutAsync([Record("""{"text":"broadcast"}""")], TestContext.Current.CancellationToken);

        var body = Assert.Single(handler.Bodies);
        Assert.Contains("\"to\":\"4915199999\"", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_UsesTheConfiguredRecipientAndMessageFieldNames()
    {
        using var handler = new RecordingHandler(_ => Accepted());
        using var task = new WhatsAppSinkTask(handler);
        var config = SinkConfig();
        config[WhatsAppConnectorConfig.RecipientField] = "msisdn";
        config[WhatsAppConnectorConfig.MessageField] = "payload";
        task.Start(config);

        await task.PutAsync(
            [Record("""{"msisdn":"4915100000","payload":"mapped fields"}""")],
            TestContext.Current.CancellationToken);

        var body = Assert.Single(handler.Bodies);
        Assert.Contains("\"to\":\"4915100000\"", body, StringComparison.Ordinal);
        Assert.Contains("\"body\":\"mapped fields\"", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_TemplateMessageType_SendsTheTemplateNameAndLanguage()
    {
        using var handler = new RecordingHandler(_ => Accepted());
        using var task = new WhatsAppSinkTask(handler);
        var config = SinkConfig();
        config[WhatsAppConnectorConfig.MessageType] = "template";
        task.Start(config);

        await task.PutAsync(
            [Record("""{"to":"4915100000","template_name":"order_update","template_language":"de_DE"}""")],
            TestContext.Current.CancellationToken);

        var body = Assert.Single(handler.Bodies);
        Assert.Contains("\"type\":\"template\"", body, StringComparison.Ordinal);
        Assert.Contains("\"name\":\"order_update\"", body, StringComparison.Ordinal);
        Assert.Contains("\"code\":\"de_DE\"", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_WhenTheMessageFieldIsAbsent_SendsTheWholePayloadAsTheBody()
    {
        using var handler = new RecordingHandler(_ => Accepted());
        using var task = new WhatsAppSinkTask(handler);
        task.Start(SinkConfig());

        await task.PutAsync([Record("""{"to":"4915100000","note":"no text field"}""")], TestContext.Current.CancellationToken);

        var body = Assert.Single(handler.Bodies);
        Assert.Contains("no text field", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_WithAnUnparseablePayload_RaisesTheErrorAndFailsTheBatch()
    {
        var errors = new List<Exception>();
        using var handler = new RecordingHandler(_ => Accepted());
        using var task = new WhatsAppSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SinkConfig());

        await Assert.ThrowsAnyAsync<Exception>(
            () => task.PutAsync([Record("this is not json")], TestContext.Current.CancellationToken));

        Assert.Single(errors);
        Assert.Empty(handler.Requests);
    }

    private static HttpResponseMessage Accepted() =>
        new(HttpStatusCode.OK) { Content = new StringContent("""{"messages":[{"id":"wamid.1"}]}""", Encoding.UTF8, "application/json") };

    private static SinkRecord Record(string json, long offset = 0) => new()
    {
        Topic = "outbound",
        Partition = 0,
        Offset = offset,
        Value = Encoding.UTF8.GetBytes(json)
    };

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [WhatsAppConnectorConfig.AccessToken] = "test-token",
        [WhatsAppConnectorConfig.PhoneNumberId] = "123456"
    };

    /// <summary>Answers every request from a canned responder and records what was sent.</summary>
    private sealed class RecordingHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _respond;

        public RecordingHandler(Func<HttpRequestMessage, HttpResponseMessage> respond) => _respond = respond;

        public List<string> Requests { get; } = [];

        public List<HttpMethod> Methods { get; } = [];

        public List<string> Bodies { get; } = [];

        public string? AuthorizationHeader { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request.RequestUri!.ToString());
            Methods.Add(request.Method);
            AuthorizationHeader = request.Headers.Authorization?.ToString();

            if (request.Content is not null)
            {
                Bodies.Add(await request.Content.ReadAsStringAsync(cancellationToken));
            }

            return _respond(request);
        }
    }
}
