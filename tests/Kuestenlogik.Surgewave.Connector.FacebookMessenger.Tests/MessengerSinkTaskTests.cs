using System.Net;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.FacebookMessenger.Tests;

/// <summary>
/// Tests for <see cref="MessengerSinkTask"/> driven through a stub transport: where the page
/// token travels, what the Send API payload looks like for each message type, and what happens
/// to a message the API refuses - records are acknowledged the moment PutAsync returns.
/// </summary>
public class MessengerSinkTaskTests
{
    private const string PageAccessToken = "page-access-token";
    private const string SendApiUrl = "https://graph.facebook.com/v18.0/me/messages";

    [Fact]
    public async Task PutAsync_SendsThePageTokenAsABearerHeaderNotInTheUrl()
    {
        // A token in the query string ends up in every proxy and diagnostics log.
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(handler, errors, (MessengerConnectorConfig.DefaultRecipientId, "psid-1"));

        await task.PutAsync([Record("""{"text":"hello"}""")], CancellationToken.None);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal(SendApiUrl, request.Url?.AbsoluteUri);
        Assert.Equal(string.Empty, request.Url?.Query);
        Assert.Equal($"Bearer {PageAccessToken}", request.Authorization);
    }

    [Fact]
    public async Task PutAsync_PrefersTheRecipientFromTheRecordOverTheDefault()
    {
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(handler, errors, (MessengerConnectorConfig.DefaultRecipientId, "default-psid"));

        await task.PutAsync(
            [Record("""{"recipient_id":"record-psid","text":"hi"}""")],
            CancellationToken.None);

        using var payload = JsonDocument.Parse(Assert.Single(handler.Requests).Body);
        Assert.Equal("record-psid", payload.RootElement.GetProperty("recipient").GetProperty("id").GetString());
        Assert.Equal("hi", payload.RootElement.GetProperty("message").GetProperty("text").GetString());
    }

    [Fact]
    public async Task PutAsync_WithoutAnyRecipient_ReportsTheRecordInsteadOfSendingIt()
    {
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(handler, errors);

        await task.PutAsync([Record("""{"text":"nobody to send this to"}""")], CancellationToken.None);

        Assert.IsType<InvalidOperationException>(Assert.Single(errors));
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task PutAsync_QuickRepliesType_AttachesTheRepliesToTheMessage()
    {
        // The configured message type has to reach the payload - otherwise every message is
        // sent as plain text no matter what was configured.
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(
            handler,
            errors,
            (MessengerConnectorConfig.DefaultRecipientId, "psid-1"),
            (MessengerConnectorConfig.MessageType, "quick_replies"));

        await task.PutAsync(
            [Record("""{"text":"Continue?","quick_replies":["Yes","No"]}""")],
            CancellationToken.None);

        using var payload = JsonDocument.Parse(Assert.Single(handler.Requests).Body);
        var replies = payload.RootElement.GetProperty("message").GetProperty("quick_replies");

        Assert.Equal(2, replies.GetArrayLength());
        Assert.Equal("text", replies[0].GetProperty("content_type").GetString());
        Assert.Equal("Yes", replies[0].GetProperty("title").GetString());
        Assert.Equal("Yes", replies[0].GetProperty("payload").GetString());
        Assert.Equal("No", replies[1].GetProperty("title").GetString());
    }

    [Fact]
    public async Task PutAsync_QuickRepliesType_KeepsTitleAndPayloadOfObjectEntriesApart()
    {
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(
            handler,
            errors,
            (MessengerConnectorConfig.DefaultRecipientId, "psid-1"),
            (MessengerConnectorConfig.MessageType, "quick_replies"));

        await task.PutAsync(
            [Record("""{"text":"Pick one","quick_replies":[{"title":"Ship it","payload":"SHIP","content_type":"text"},{"payload":"NO_TITLE"}]}""")],
            CancellationToken.None);

        using var payload = JsonDocument.Parse(Assert.Single(handler.Requests).Body);
        var replies = payload.RootElement.GetProperty("message").GetProperty("quick_replies");

        // The entry without a title is unusable and is left out.
        Assert.Equal(1, replies.GetArrayLength());
        Assert.Equal("Ship it", replies[0].GetProperty("title").GetString());
        Assert.Equal("SHIP", replies[0].GetProperty("payload").GetString());
    }

    [Fact]
    public async Task PutAsync_QuickRepliesType_WithoutUsableReplies_ReportsTheRecord()
    {
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(
            handler,
            errors,
            (MessengerConnectorConfig.DefaultRecipientId, "psid-1"),
            (MessengerConnectorConfig.MessageType, "quick_replies"));

        await task.PutAsync([Record("""{"text":"no options"}""")], CancellationToken.None);

        Assert.IsType<InvalidOperationException>(Assert.Single(errors));
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task PutAsync_TextType_IgnoresQuickRepliesInTheRecord()
    {
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(handler, errors, (MessengerConnectorConfig.DefaultRecipientId, "psid-1"));

        await task.PutAsync(
            [Record("""{"text":"plain","quick_replies":["Yes"]}""")],
            CancellationToken.None);

        using var payload = JsonDocument.Parse(Assert.Single(handler.Requests).Body);
        Assert.False(payload.RootElement.GetProperty("message").TryGetProperty("quick_replies", out _));
    }

    [Fact]
    public async Task PutAsync_ThrowsWhenTheSendApiRejectsTheMessage()
    {
        // An undelivered message must fail the batch so the worker can retry or DLQ it.
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.Forbidden, """{"error":{"message":"outside the 24h window"}}""");
        var errors = new List<Exception>();
        using var task = StartTask(handler, errors, (MessengerConnectorConfig.DefaultRecipientId, "psid-1"));

        var ex = await Assert.ThrowsAsync<HttpRequestException>(
            () => task.PutAsync([Record("""{"text":"too late"}""")], CancellationToken.None));

        Assert.Contains("403", ex.Message, StringComparison.Ordinal);
        Assert.Contains("outside the 24h window", ex.Message, StringComparison.Ordinal);
        Assert.Same(ex, Assert.Single(errors));
    }

    [Fact]
    public async Task PutAsync_MalformedJson_IsReportedAndTheBatchContinues()
    {
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(handler, errors, (MessengerConnectorConfig.DefaultRecipientId, "psid-1"));

        await task.PutAsync(
            [Record("this is not json"), Record("""{"text":"still delivered"}""")],
            CancellationToken.None);

        Assert.IsType<InvalidOperationException>(Assert.Single(errors));
        using var payload = JsonDocument.Parse(Assert.Single(handler.Requests).Body);
        Assert.Equal("still delivered", payload.RootElement.GetProperty("message").GetProperty("text").GetString());
    }

    [Fact]
    public void Start_RejectsAMessageTypeTheConnectorCannotSend()
    {
        using var handler = new StubHandler();
        using var task = new MessengerSinkTask(handler);

        var config = BaseConfig();
        config[MessengerConnectorConfig.MessageType] = "template";

        var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
        Assert.Contains(MessengerConnectorConfig.MessageType, ex.Message, StringComparison.Ordinal);
    }

    private static MessengerSinkTask StartTask(
        StubHandler handler,
        List<Exception> errors,
        params (string Key, string Value)[] settings)
    {
        var config = BaseConfig();
        foreach (var (key, value) in settings)
        {
            config[key] = value;
        }

        var task = new MessengerSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(config);
        return task;
    }

    private static Dictionary<string, string> BaseConfig() => new(StringComparer.Ordinal)
    {
        [MessengerConnectorConfig.PageAccessToken] = PageAccessToken
    };

    private static SinkRecord Record(string json) => new()
    {
        Topic = "messages",
        Partition = 0,
        Offset = 42,
        Value = Encoding.UTF8.GetBytes(json)
    };

    private sealed record RecordedRequest(HttpMethod Method, Uri? Url, string? Authorization, string Body);

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Queue<(HttpStatusCode Status, string Body)> _responses = new();

        public List<RecordedRequest> Requests { get; } = [];

        public void EnqueueResponse(HttpStatusCode status, string body) => _responses.Enqueue((status, body));

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var body = request.Content == null
                ? string.Empty
                : await request.Content.ReadAsStringAsync(cancellationToken);

            Requests.Add(new RecordedRequest(
                request.Method,
                request.RequestUri,
                request.Headers.Authorization?.ToString(),
                body));

            var response = _responses.Count > 0
                ? _responses.Dequeue()
                : (Status: HttpStatusCode.OK, Body: """{"recipient_id":"psid-1","message_id":"mid.1"}""");

            return new HttpResponseMessage(response.Status) { Content = new StringContent(response.Body) };
        }
    }
}
