using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Matter.Tests;

/// <summary>
/// Command translation of the Matter sink: records become REST bridge commands, and a controller
/// that rejects a command must fail the batch instead of silently dropping the device command.
/// </summary>
public class MatterSinkTaskTests
{
    [Fact]
    public async Task PutAsync_SendsExplicitCommandToTheController()
    {
        using var handler = new StubHandler();

        using var task = new MatterSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync(
            [Record("""{"nodeId":"node-1","command":"MoveToLevel","cluster":"LevelControl","args":{"level":128}}""")],
            cts.Token);

        var request = Assert.Single(handler.Requests);
        Assert.Equal("http://matter.local:5580/api/command", request.Url);
        Assert.Contains("\"node_id\":\"node-1\"", request.Body, StringComparison.Ordinal);
        Assert.Contains("\"endpoint_id\":1", request.Body, StringComparison.Ordinal);
        Assert.Contains("\"cluster\":\"LevelControl\"", request.Body, StringComparison.Ordinal);
        Assert.Contains("\"command\":\"MoveToLevel\"", request.Body, StringComparison.Ordinal);
        Assert.Contains("\"args\":{\"level\":128}", request.Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_InfersClusterFromCommand_AndHonoursPayloadEndpoint()
    {
        using var handler = new StubHandler();

        using var task = new MatterSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync(
            [Record("""{"nodeId":"node-2","command":"LockDoor","endpointId":3,"args":{}}""")],
            cts.Token);

        var request = Assert.Single(handler.Requests);
        Assert.Contains("\"cluster\":\"DoorLock\"", request.Body, StringComparison.Ordinal);
        Assert.Contains("\"endpoint_id\":3", request.Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_ThrowsAndReportsError_WhenTheControllerRejectsTheCommand()
    {
        var errors = new List<Exception>();
        using var handler = new StubHandler { Status = HttpStatusCode.ServiceUnavailable };

        using var task = new MatterSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var error = await Assert.ThrowsAsync<HttpRequestException>(
            () => task.PutAsync([Record("""{"nodeId":"node-1","on":true}""")], cts.Token));

        Assert.Contains("node-1", error.Message, StringComparison.Ordinal);
        Assert.Contains("503", error.Message, StringComparison.Ordinal);
        Assert.Same(error, Assert.Single(errors));
    }

    [Fact]
    public async Task PutAsync_TranslatesOnOffState()
    {
        using var handler = new StubHandler();

        using var task = new MatterSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"nodeId":"node-1","on":false}""")], cts.Token);

        var request = Assert.Single(handler.Requests);
        Assert.Contains("\"cluster\":\"OnOff\"", request.Body, StringComparison.Ordinal);
        Assert.Contains("\"command\":\"Off\"", request.Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_SendsOneCommandPerStateProperty()
    {
        using var handler = new StubHandler();

        using var task = new MatterSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"nodeId":"node-1","on":true,"brightness":42}""")], cts.Token);

        Assert.Equal(2, handler.Requests.Count);
        Assert.Contains("\"command\":\"On\"", handler.Requests[0].Body, StringComparison.Ordinal);
        Assert.Contains("\"cluster\":\"LevelControl\"", handler.Requests[1].Body, StringComparison.Ordinal);
        Assert.Contains("\"args\":{\"level\":42,\"transition_time\":10}", handler.Requests[1].Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_FallsBackToTheNodeIdHeader()
    {
        using var handler = new StubHandler();

        using var task = new MatterSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        var record = new SinkRecord
        {
            Topic = "matter-commands",
            Partition = 0,
            Offset = 0,
            Value = Encoding.UTF8.GetBytes("""{"on":true}"""),
            Headers = new Dictionary<string, byte[]>
            {
                ["matter.nodeId"] = Encoding.UTF8.GetBytes("from-header")
            }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([record], cts.Token);

        Assert.Contains("\"node_id\":\"from-header\"", Assert.Single(handler.Requests).Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_ReportsRecordsWithoutAddressableNode()
    {
        var errors = new List<Exception>();
        using var handler = new StubHandler();

        using var task = new MatterSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"on":true}""", offset: 12)], cts.Token);

        Assert.Empty(handler.Requests);
        Assert.Contains("no nodeId", Assert.Single(errors).Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_ReportsPoisonRecordAndKeepsProcessingTheBatch()
    {
        var errors = new List<Exception>();
        using var handler = new StubHandler();

        using var task = new MatterSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync(
            [Record("not-json"), Record("""{"nodeId":"node-1","on":true}""", offset: 1)],
            cts.Token);

        Assert.Single(handler.Requests);
        Assert.Single(errors);
    }

    [Fact]
    public async Task PutAsync_AuthenticatesWithTheConfiguredApiKey()
    {
        using var handler = new StubHandler();

        var config = SinkConfig();
        config[MatterConnectorConfig.ApiKey] = "secret";

        using var task = new MatterSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"nodeId":"node-1","on":true}""")], cts.Token);

        Assert.Equal("Bearer secret", Assert.Single(handler.Requests).Authorization);
    }

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [MatterConnectorConfig.Topics] = "matter-commands",
        [MatterConnectorConfig.ControllerUrl] = "http://matter.local:5580/"
    };

    private static SinkRecord Record(string json, long offset = 0) => new()
    {
        Topic = "matter-commands",
        Partition = 0,
        Offset = offset,
        Value = Encoding.UTF8.GetBytes(json)
    };

    private sealed class StubHandler : HttpMessageHandler
    {
        public HttpStatusCode Status { get; init; } = HttpStatusCode.OK;

        public List<CapturedRequest> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var body = request.Content is null
                ? string.Empty
                : await request.Content.ReadAsStringAsync(cancellationToken);

            Requests.Add(new CapturedRequest(
                request.RequestUri?.ToString() ?? string.Empty,
                body,
                request.Headers.Authorization?.ToString()));

            return new HttpResponseMessage(Status)
            {
                Content = new StringContent("{}", Encoding.UTF8, "application/json")
            };
        }
    }

    private sealed record CapturedRequest(string Url, string Body, string? Authorization);
}
