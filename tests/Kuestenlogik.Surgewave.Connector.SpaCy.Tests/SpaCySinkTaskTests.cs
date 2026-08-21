using System.Net;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.SpaCy.Tests;

/// <summary>
/// Tests for <see cref="SpaCySinkTask"/> driven through a stub transport: which text is
/// extracted from a record, what the spaCy server is actually asked for, and which records
/// never cost a round trip at all.
/// </summary>
public class SpaCySinkTaskTests
{
    private const string ServerUrl = "http://spacy.invalid:8080/";
    private const string ProcessUrl = "http://spacy.invalid:8080/process";
    private const string EmptyResponse = """{"tokens":[],"ents":[]}""";

    [Fact]
    public void Start_WithoutTheOutputTopic_FailsBeforeAnyRequest()
    {
        using var task = new SpaCySinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var config = SinkConfig();
        config.Remove(SpaCyConnectorConfig.OutputTopic);

        var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
        Assert.Contains(SpaCyConnectorConfig.OutputTopic, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_SendsEveryRecordToTheProcessEndpoint()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, EmptyResponse));
        using var task = StartTask(handler, SinkConfig());

        await task.PutAsync(
            [Record("""{"text":"Berlin is a city"}"""), Record("""{"text":"So is Kiel"}""")],
            TestContext.Current.CancellationToken);

        Assert.Equal(2, handler.Requests.Count);

        var first = handler.Requests[0];
        Assert.Equal(HttpMethod.Post, first.Method);
        Assert.Equal("application/json", first.ContentType);

        // The configured server URL ends in a slash; concatenating "/process" onto it
        // unchanged would ask for "//process", which most gateways answer with a 404.
        Assert.Equal(ProcessUrl, first.Uri);

        using var body = JsonDocument.Parse(first.Body);
        Assert.Equal("Berlin is a city", body.RootElement.GetProperty("text").GetString());
        Assert.Equal(SpaCyConnectorConfig.DefaultModel, body.RootElement.GetProperty("model").GetString());
        Assert.Empty(body.RootElement.GetProperty("disable").EnumerateArray());
    }

    [Fact]
    public async Task PutAsync_AsksForTheConfiguredModelWithTheDisabledComponents()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, EmptyResponse));
        var config = SinkConfig();
        config[SpaCyConnectorConfig.Model] = "de_core_news_lg";
        config[SpaCyConnectorConfig.DisablePipeline] = "ner, parser";
        using var task = StartTask(handler, config);

        await task.PutAsync([Record("""{"text":"Kiel liegt am Meer"}""")], TestContext.Current.CancellationToken);

        using var body = JsonDocument.Parse(Assert.Single(handler.Requests).Body);
        Assert.Equal("de_core_news_lg", body.RootElement.GetProperty("model").GetString());

        // Disabling components is the only lever the connector has over spaCy's runtime
        // cost, so the list has to arrive trimmed and split.
        var disabled = body.RootElement.GetProperty("disable");
        Assert.Equal(2, disabled.GetArrayLength());
        Assert.Equal("ner", disabled[0].GetString());
        Assert.Equal("parser", disabled[1].GetString());
    }

    [Fact]
    public async Task PutAsync_TakesTheTextFromTheConfiguredField()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, EmptyResponse));
        var config = SinkConfig();
        config[SpaCyConnectorConfig.TextField] = "body";
        using var task = StartTask(handler, config);

        await task.PutAsync(
            [Record("""{"body":"the interesting part","text":"the wrong one"}""")],
            TestContext.Current.CancellationToken);

        using var body = JsonDocument.Parse(Assert.Single(handler.Requests).Body);
        Assert.Equal("the interesting part", body.RootElement.GetProperty("text").GetString());
    }

    [Fact]
    public async Task PutAsync_WithAValueThatIsNotJson_ProcessesItAsPlainText()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, EmptyResponse));
        using var task = StartTask(handler, SinkConfig());

        // Plain-text topics are a legitimate input; they must not be silently skipped just
        // because the value does not parse as JSON.
        await task.PutAsync([Record("Berlin is a city")], TestContext.Current.CancellationToken);

        using var body = JsonDocument.Parse(Assert.Single(handler.Requests).Body);
        Assert.Equal("Berlin is a city", body.RootElement.GetProperty("text").GetString());
    }

    [Theory]
    [InlineData("""{"other":1}""")]
    [InlineData("""{"text":null}""")]
    [InlineData("""{"text":"   "}""")]
    public async Task PutAsync_WithoutUsableText_NeverCallsTheServer(string value)
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, EmptyResponse));
        using var task = StartTask(handler, SinkConfig());

        await task.PutAsync([Record(value)], TestContext.Current.CancellationToken);

        // An NLP call per empty record would burn the model's throughput on nothing.
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task PutAsync_WithNothingToProcess_NeverCallsTheServer()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, EmptyResponse));
        using var task = StartTask(handler, SinkConfig());

        await task.PutAsync([], TestContext.Current.CancellationToken);
        await task.PutAsync([RecordWithoutValue()], TestContext.Current.CancellationToken);

        Assert.Empty(handler.Requests);
    }

    private static SpaCySinkTask StartTask(HttpMessageHandler handler, IDictionary<string, string> config)
    {
        var task = new SpaCySinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);
        return task;
    }

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [SpaCyConnectorConfig.Topics] = "documents",
        [SpaCyConnectorConfig.OutputTopic] = "documents-nlp",
        [SpaCyConnectorConfig.ServerUrl] = ServerUrl
    };

    private static SinkRecord Record(string value) => new()
    {
        Topic = "documents",
        Partition = 0,
        Offset = 1,
        Value = Encoding.UTF8.GetBytes(value),
        Timestamp = DateTimeOffset.UnixEpoch
    };

    private static SinkRecord RecordWithoutValue() => new()
    {
        Topic = "documents",
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

        public required string? ContentType { get; init; }
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
                ContentType = request.Content?.Headers.ContentType?.MediaType
            });

            return responder(request);
        }
    }
}
