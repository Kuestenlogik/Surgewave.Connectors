using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Hue.Tests;

/// <summary>
/// Drives the task against a stubbed bridge: no network, but the real Hue v1 resource URLs,
/// the real Q42 parsing and the real change detection.
/// </summary>
public class HueSourceTaskTests
{
    private const string LightsJson =
        """{"1":{"state":{"on":true,"bri":128,"reachable":true},"type":"Extended color light","name":"Desk Lamp","modelid":"LCT001","manufacturername":"Signify"}}""";

    private const string GroupsJson =
        """{"1":{"name":"Living Room","lights":["1","2"],"type":"Room","state":{"all_on":true,"any_on":true},"action":{"on":true}}}""";

    private const string ScenesJson =
        """{"s1":{"name":"Movie Night","lights":["1"],"owner":"abc","recycle":false,"locked":false,"version":2,"lastupdated":"2024-01-02T03:04:05"}}""";

    private const string SensorsJson =
        """{"2":{"state":{"daylight":true},"config":{"on":true},"name":"Daylight","type":"Daylight","modelid":"PHDL00","manufacturername":"Signify"}}""";

    [Fact]
    public async Task PollAsync_EmitsOneRecordPerLightKeyedByItsBridgeId()
    {
        using var handler = new StubHttpHandler(_ => Json(LightsJson));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSourceTask(http);
        var config = BaseConfig();
        config[HueConnectorConfig.IncludeLights] = "true";
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        var record = Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));

        Assert.Equal("/api/appkey123/lights", Assert.Single(handler.Paths));
        Assert.Equal("hue", record.Topic);
        Assert.Equal("light:1", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("light", HeaderValue(record, "hue.type"));
        Assert.Equal("1", HeaderValue(record, "hue.id"));
        Assert.Equal("Desk Lamp", HeaderValue(record, "hue.name"));
        Assert.Equal("hue", record.SourcePartition["source"]);
        Assert.Equal("light", record.SourcePartition["type"]);
        Assert.Equal(1L, record.SourceOffset["message_id"]);
        Assert.Equal("1", record.SourceOffset["light_id"]);

        var payload = Encoding.UTF8.GetString(record.Value);
        Assert.Contains("\"name\":\"Desk Lamp\"", payload, StringComparison.Ordinal);
        Assert.Contains("\"manufacturer\":\"Signify\"", payload, StringComparison.Ordinal);
        Assert.Contains("\"on\":true", payload, StringComparison.Ordinal);
        Assert.Contains("\"brightness\":128", payload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_WithEventsOnly_EmitsAgainOnlyAfterTheLightStateChanged()
    {
        var body = LightsJson;
        using var handler = new StubHttpHandler(_ => Json(body));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSourceTask(http);
        var config = BaseConfig();
        config[HueConnectorConfig.IncludeLights] = "true";
        config[HueConnectorConfig.EventsOnly] = "true";
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        var first = await task.PollAsync(TestContext.Current.CancellationToken);
        var unchanged = await task.PollAsync(TestContext.Current.CancellationToken);
        body = LightsJson.Replace("\"bri\":128", "\"bri\":200", StringComparison.Ordinal);
        var changed = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Single(first);
        Assert.Empty(unchanged);
        Assert.Single(changed);
        Assert.Equal(3, handler.Paths.Count);
    }

    [Fact]
    public async Task PollAsync_WithoutEventsOnly_EmitsTheSameLightOnEveryPoll()
    {
        using var handler = new StubHttpHandler(_ => Json(LightsJson));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSourceTask(http);
        var config = BaseConfig();
        config[HueConnectorConfig.IncludeLights] = "true";
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));
        Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task PollAsync_WithIncludeScenes_EmitsSceneRecords()
    {
        // hue.include.scenes used to be declared in the ConfigDef and read nowhere.
        using var handler = new StubHttpHandler(_ => Json(ScenesJson));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSourceTask(http);
        var config = BaseConfig();
        config[HueConnectorConfig.IncludeScenes] = "true";
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        var record = Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));

        Assert.Equal("/api/appkey123/scenes", Assert.Single(handler.Paths));
        Assert.Equal("scene:s1", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("scene", HeaderValue(record, "hue.type"));
        Assert.Equal("Movie Night", HeaderValue(record, "hue.name"));
        Assert.Equal("s1", record.SourceOffset["scene_id"]);
        Assert.Contains("\"name\":\"Movie Night\"", Encoding.UTF8.GetString(record.Value), StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_WithoutIncludeScenes_NeverAsksTheBridgeForScenes()
    {
        using var handler = new StubHttpHandler(request =>
            request.RequestUri!.AbsolutePath.EndsWith("groups", StringComparison.Ordinal)
                ? Json(GroupsJson)
                : Json(LightsJson));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSourceTask(http);
        var config = BaseConfig();
        config[HueConnectorConfig.IncludeLights] = "true";
        config[HueConnectorConfig.IncludeGroups] = "true";
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, records.Count);
        Assert.Equal(2, handler.Paths.Count);
        Assert.Equal("/api/appkey123/lights", handler.Paths[0]);
        Assert.Equal("/api/appkey123/groups", handler.Paths[1]);
    }

    [Fact]
    public async Task PollAsync_GroupRecord_CarriesTheAggregatedOnState()
    {
        using var handler = new StubHttpHandler(_ => Json(GroupsJson));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSourceTask(http);
        var config = BaseConfig();
        config[HueConnectorConfig.IncludeGroups] = "true";
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        var record = Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));

        Assert.Equal("group:1", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("Living Room", HeaderValue(record, "hue.name"));
        var payload = Encoding.UTF8.GetString(record.Value);
        Assert.Contains("\"allOn\":true", payload, StringComparison.Ordinal);
        Assert.Contains("\"anyOn\":true", payload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_WhenTheBridgeRejectsTheRequest_RaisesTheErrorInsteadOfStayingSilent()
    {
        // The empty catch used to make auth and connectivity failures completely invisible.
        var errors = new List<Exception>();
        using var handler = new StubHttpHandler(_ => new HttpResponseMessage(HttpStatusCode.Unauthorized));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSourceTask(http);
        var config = BaseConfig();
        config[HueConnectorConfig.IncludeLights] = "true";
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(config);

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Empty(records);
        Assert.IsType<HttpRequestException>(Assert.Single(errors), exactMatch: false);
    }

    [Fact]
    public async Task PollAsync_WhenALaterResourceFails_KeepsTheRecordsAlreadyCollected()
    {
        var errors = new List<Exception>();
        using var handler = new StubHttpHandler(request =>
            request.RequestUri!.AbsolutePath.EndsWith("sensors", StringComparison.Ordinal)
                ? new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                : Json(LightsJson));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSourceTask(http);
        var config = BaseConfig();
        config[HueConnectorConfig.IncludeLights] = "true";
        config[HueConnectorConfig.IncludeSensors] = "true";
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(config);

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Equal("light:1", Encoding.UTF8.GetString(Assert.Single(records).Key!));
        Assert.Single(errors);
    }

    [Fact]
    public async Task PollAsync_BeforeTheIntervalElapsed_ReturnsEmptyWithoutCallingTheBridgeAgain()
    {
        using var handler = new StubHttpHandler(_ => Json(SensorsJson));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSourceTask(http);
        var config = BaseConfig();
        config[HueConnectorConfig.IncludeSensors] = "true";
        config[HueConnectorConfig.PollIntervalMs] = "600000";
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        var first = await task.PollAsync(TestContext.Current.CancellationToken);
        var second = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Single(first);
        Assert.Empty(second);
        Assert.Equal("/api/appkey123/sensors", Assert.Single(handler.Paths));
    }

    private static string HeaderValue(SourceRecord record, string name) =>
        Encoding.UTF8.GetString(record.Headers![name]);

    private static HttpResponseMessage Json(string body) =>
        new(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    private static Dictionary<string, string> BaseConfig() => new()
    {
        [HueConnectorConfig.Topic] = "hue",
        [HueConnectorConfig.BridgeIp] = "192.0.2.10",
        [HueConnectorConfig.AppKey] = "appkey123",
        [HueConnectorConfig.PollIntervalMs] = "0",
        [HueConnectorConfig.IncludeLights] = "false",
        [HueConnectorConfig.IncludeSensors] = "false",
        [HueConnectorConfig.IncludeGroups] = "false",
        [HueConnectorConfig.IncludeScenes] = "false",
        [HueConnectorConfig.EventsOnly] = "false"
    };

    /// <summary>Answers every bridge request from a canned responder and records the paths it saw.</summary>
    private sealed class StubHttpHandler(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
    {
        public List<string> Paths { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Paths.Add(request.RequestUri!.AbsolutePath);
            return Task.FromResult(respond(request));
        }
    }
}
