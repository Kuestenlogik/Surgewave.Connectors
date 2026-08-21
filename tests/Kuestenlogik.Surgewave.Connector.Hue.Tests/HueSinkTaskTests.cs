using System.Net;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Hue.Tests;

/// <summary>
/// Drives the task against a stubbed bridge and asserts what actually reaches it: the Hue v1
/// resource the command is routed to and the command body that was built from the record.
/// </summary>
public class HueSinkTaskTests
{
    [Fact]
    public async Task PutAsync_LightCommand_PutsTheStateOnThatLight()
    {
        using var handler = new StubHttpHandler();
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(BaseConfig());

        await task.PutAsync([Record("""{"type":"light","id":"7","on":true,"brightness":200}""")],
            TestContext.Current.CancellationToken);

        Assert.Equal("/api/appkey123/lights/7/state", Assert.Single(handler.Paths));
        var body = Assert.Single(handler.Bodies);
        Assert.Contains("\"on\":true", body, StringComparison.Ordinal);
        Assert.Contains("\"bri\":200", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_GroupCommand_TargetsTheGroupAction()
    {
        using var handler = new StubHttpHandler();
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(BaseConfig());

        await task.PutAsync([Record("""{"type":"group","id":"3","on":false}""")],
            TestContext.Current.CancellationToken);

        Assert.Equal("/api/appkey123/groups/3/action", Assert.Single(handler.Paths));
        Assert.Contains("\"on\":false", Assert.Single(handler.Bodies), StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_SceneCommand_RecallsTheSceneOnTheTargetGroup()
    {
        using var handler = new StubHttpHandler();
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(BaseConfig());

        await task.PutAsync([Record("""{"type":"group","id":"3","scene":"scene-42","on":true}""")],
            TestContext.Current.CancellationToken);

        Assert.Equal("/api/appkey123/groups/3/action", Assert.Single(handler.Paths));
        Assert.Equal("""{"scene":"scene-42"}""", Assert.Single(handler.Bodies));
    }

    [Fact]
    public async Task PutAsync_ColorAndEffectFields_AreTranslatedToBridgeCommandFields()
    {
        using var handler = new StubHttpHandler();
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(BaseConfig());

        await task.PutAsync(
            [Record("""{"type":"light","id":"7","hue":12000,"saturation":150,"colorTemperature":370,"effect":"colorloop","alert":"select"}""")],
            TestContext.Current.CancellationToken);

        var body = Assert.Single(handler.Bodies);
        Assert.Contains("\"hue\":12000", body, StringComparison.Ordinal);
        Assert.Contains("\"sat\":150", body, StringComparison.Ordinal);
        Assert.Contains("\"ct\":370", body, StringComparison.Ordinal);
        Assert.Contains("\"effect\":\"colorloop\"", body, StringComparison.Ordinal);
        Assert.Contains("\"alert\":\"select\"", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_WithoutAnIdInThePayload_FallsBackToTheConfiguredDefaultLight()
    {
        using var handler = new StubHttpHandler();
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSinkTask(http);
        var config = BaseConfig();
        config[HueConnectorConfig.DefaultLightId] = "9";
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        await task.PutAsync([Record("""{"on":true}""")], TestContext.Current.CancellationToken);

        Assert.Equal("/api/appkey123/lights/9/state", Assert.Single(handler.Paths));
    }

    [Fact]
    public async Task PutAsync_TakesTheTargetFromTheHueHeadersWhenThePayloadOmitsIt()
    {
        using var handler = new StubHttpHandler();
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(BaseConfig());

        var record = Record("""{"on":true}""", new Dictionary<string, byte[]>
        {
            ["hue.type"] = Encoding.UTF8.GetBytes("group"),
            ["hue.id"] = Encoding.UTF8.GetBytes("3")
        });

        await task.PutAsync([record], TestContext.Current.CancellationToken);

        Assert.Equal("/api/appkey123/groups/3/action", Assert.Single(handler.Paths));
    }

    [Fact]
    public async Task PutAsync_WithoutAnIdAnywhere_SkipsTheRecordWithoutTouchingTheBridge()
    {
        using var handler = new StubHttpHandler();
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(BaseConfig());

        await task.PutAsync([Record("""{"on":true}"""), RecordWithoutValue()], TestContext.Current.CancellationToken);

        Assert.Empty(handler.Paths);
    }

    [Fact]
    public async Task PutAsync_AppliesTheConfiguredTransitionTime()
    {
        using var handler = new StubHttpHandler();
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSinkTask(http);
        var config = BaseConfig();
        config[HueConnectorConfig.TransitionTimeMs] = "2000";
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        await task.PutAsync([Record("""{"type":"light","id":"7","on":true}""")],
            TestContext.Current.CancellationToken);

        // The bridge takes the transition time in deciseconds.
        Assert.Contains("\"transitiontime\":20", Assert.Single(handler.Bodies), StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_APerRecordTransitionTimeWinsOverTheConfiguredDefault()
    {
        using var handler = new StubHttpHandler();
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSinkTask(http);
        var config = BaseConfig();
        config[HueConnectorConfig.TransitionTimeMs] = "2000";
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        await task.PutAsync([Record("""{"type":"light","id":"7","on":true,"transitionTime":500}""")],
            TestContext.Current.CancellationToken);

        Assert.Contains("\"transitiontime\":5", Assert.Single(handler.Bodies), StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_WithAMalformedCommand_RaisesTheErrorAndStillDeliversTheNextRecord()
    {
        // The empty catch used to drop malformed commands without a trace.
        var errors = new List<Exception>();
        using var handler = new StubHttpHandler();
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(BaseConfig());

        await task.PutAsync(
            [Record("this is not json"), Record("""{"type":"light","id":"7","on":true}""")],
            TestContext.Current.CancellationToken);

        Assert.IsType<JsonException>(Assert.Single(errors), exactMatch: false);
        Assert.Equal("/api/appkey123/lights/7/state", Assert.Single(handler.Paths));
    }

    [Fact]
    public async Task PutAsync_WithACommandFieldOfTheWrongType_RaisesTheErrorInsteadOfDroppingIt()
    {
        var errors = new List<Exception>();
        using var handler = new StubHttpHandler();
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new HueSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(BaseConfig());

        await task.PutAsync([Record("""{"type":"light","id":"7","on":"yes"}""")],
            TestContext.Current.CancellationToken);

        Assert.IsType<InvalidOperationException>(Assert.Single(errors), exactMatch: false);
        Assert.Empty(handler.Paths);
    }

    [Fact]
    public async Task PutAsync_WhenSendingTheCommandFails_RaisesAndRethrowsInsteadOfAcknowledging()
    {
        // Start was deliberately not called, so the very first thing PutAsync touches - the
        // bridge client - fails. What is pinned here is where the failure goes: the audit found
        // failed commands swallowed while the worker went on to commit their offsets.
        var errors = new List<Exception>();
        using var task = new HueSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });

        var thrown = await Assert.ThrowsAnyAsync<Exception>(() =>
            task.PutAsync([Record("""{"type":"light","id":"7","on":true}""")],
                TestContext.Current.CancellationToken));

        Assert.Same(thrown, Assert.Single(errors));
    }

    private static SinkRecord Record(string json, IReadOnlyDictionary<string, byte[]>? headers = null) => new()
    {
        Topic = "hue-commands",
        Partition = 0,
        Offset = 0,
        Value = Encoding.UTF8.GetBytes(json),
        Headers = headers
    };

    private static SinkRecord RecordWithoutValue() => new()
    {
        Topic = "hue-commands",
        Partition = 0,
        Offset = 0,
        Value = null!
    };

    private static Dictionary<string, string> BaseConfig() => new()
    {
        [HueConnectorConfig.Topics] = "hue-commands",
        [HueConnectorConfig.BridgeIp] = "192.0.2.10",
        [HueConnectorConfig.AppKey] = "appkey123"
    };

    /// <summary>Accepts every bridge command and records the resource path and body it saw.</summary>
    private sealed class StubHttpHandler : HttpMessageHandler
    {
        public List<string> Paths { get; } = [];

        public List<string> Bodies { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Paths.Add(request.RequestUri!.AbsolutePath);
            Bodies.Add(request.Content == null
                ? string.Empty
                : await request.Content.ReadAsStringAsync(cancellationToken));

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("[]", Encoding.UTF8, "application/json")
            };
        }
    }
}
