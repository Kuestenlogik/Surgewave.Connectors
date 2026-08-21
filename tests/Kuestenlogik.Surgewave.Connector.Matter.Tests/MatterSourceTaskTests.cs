using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Matter.Tests;

/// <summary>
/// Device polling of the Matter source: node payloads become records, and an unreachable or
/// erroring controller must be reported instead of looking like an idle one.
/// </summary>
public class MatterSourceTaskTests
{
    [Fact]
    public async Task PollAsync_TurnsNodesIntoRecords()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.OK, """
            {"nodes":[{"node_id":"node-1","device_type":"light","name":"Kitchen","vendor_name":"ACME",
            "product_name":"Bulb","attributes":{"OnOff":{"on_off":true},"LevelControl":{"current_level":200}}}]}
            """);

        using var task = new MatterSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SourceConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var record = Assert.Single(await task.PollAsync(cts.Token));

        Assert.Equal("http://matter.local:5580/api/nodes", Assert.Single(handler.Requests).Url);
        Assert.Equal("matter-events", record.Topic);
        Assert.Equal("matter:node-1", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("node-1", Encoding.UTF8.GetString(record.Headers!["matter.node.id"]));
        Assert.Equal("light", Encoding.UTF8.GetString(record.Headers!["matter.type"]));
        Assert.Equal("Kitchen", Encoding.UTF8.GetString(record.Headers!["matter.name"]));

        var payload = Encoding.UTF8.GetString(record.Value);
        Assert.Contains("\"deviceType\":\"light\"", payload, StringComparison.Ordinal);
        Assert.Contains("\"on\":true", payload, StringComparison.Ordinal);
        Assert.Contains("\"brightness\":200", payload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_ScalesSensorMeasurements()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.OK, """
            {"nodes":[{"node_id":"sensor-1","device_type":"sensor","attributes":{
            "TemperatureMeasurement":{"measured_value":2150},"OccupancySensing":{"occupancy":1}}}]}
            """);

        using var task = new MatterSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SourceConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var record = Assert.Single(await task.PollAsync(cts.Token));

        var payload = Encoding.UTF8.GetString(record.Value);
        Assert.Contains("\"temperature\":21.5", payload, StringComparison.Ordinal);
        Assert.Contains("\"occupied\":true", payload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_ReportsControllerErrorResponses()
    {
        var errors = new List<Exception>();
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.Unauthorized, "");

        using var task = new MatterSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SourceConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Assert.Empty(await task.PollAsync(cts.Token));

        Assert.Contains("401", Assert.Single(errors).Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_ReportsUnreachableController()
    {
        var errors = new List<Exception>();
        using var handler = new StubHandler { Fault = new HttpRequestException("connection refused") };

        using var task = new MatterSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SourceConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Assert.Empty(await task.PollAsync(cts.Token));

        Assert.Single(errors);
    }

    [Fact]
    public async Task PollAsync_EmitsOnlyWhenTheDeviceStateChanged()
    {
        const string unchanged = """{"nodes":[{"node_id":"node-1","device_type":"switch","attributes":{"OnOff":{"on_off":true}}}]}""";
        const string changed = """{"nodes":[{"node_id":"node-1","device_type":"switch","attributes":{"OnOff":{"on_off":false}}}]}""";

        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.OK, unchanged);
        handler.Enqueue(HttpStatusCode.OK, unchanged);
        handler.Enqueue(HttpStatusCode.OK, changed);

        using var task = new MatterSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SourceConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Assert.Single(await task.PollAsync(cts.Token));
        Assert.Empty(await task.PollAsync(cts.Token));
        Assert.Single(await task.PollAsync(cts.Token));
    }

    [Fact]
    public async Task PollAsync_HonoursTheNodeIdFilter()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.OK, """
            {"nodes":[{"node_id":"node-1","device_type":"switch"},{"node_id":"node-2","device_type":"switch"}]}
            """);

        var config = SourceConfig();
        config[MatterConnectorConfig.FilterNodeIds] = "node-2";

        using var task = new MatterSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var record = Assert.Single(await task.PollAsync(cts.Token));

        Assert.Equal("node-2", Encoding.UTF8.GetString(record.Headers!["matter.node.id"]));
    }

    [Fact]
    public async Task PollAsync_InfersDeviceTypeFromClusters()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.OK, """
            {"nodes":[{"node_id":"node-1","clusters":["OnOff","LevelControl"]}]}
            """);

        using var task = new MatterSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SourceConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var record = Assert.Single(await task.PollAsync(cts.Token));

        Assert.Equal("light", Encoding.UTF8.GetString(record.Headers!["matter.type"]));
    }

    [Fact]
    public async Task PollAsync_SkipsDeviceTypesTheConnectorWasToldToIgnore()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.OK, """
            {"nodes":[{"node_id":"lamp","device_type":"light"},{"node_id":"probe","device_type":"sensor"}]}
            """);

        var config = SourceConfig();
        config[MatterConnectorConfig.IncludeLighting] = "false";

        using var task = new MatterSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var record = Assert.Single(await task.PollAsync(cts.Token));

        Assert.Equal("probe", Encoding.UTF8.GetString(record.Headers!["matter.node.id"]));
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [MatterConnectorConfig.Topic] = "matter-events",
        [MatterConnectorConfig.ControllerUrl] = "http://matter.local:5580",
        [MatterConnectorConfig.PollIntervalMs] = "0"
    };

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Queue<HttpResponseMessage> _responses = new();

        public Exception? Fault { get; init; }

        public List<CapturedRequest> Requests { get; } = [];

        public void Enqueue(HttpStatusCode status, string body) =>
            _responses.Enqueue(new HttpResponseMessage(status)
            {
                Content = new StringContent(body, Encoding.UTF8, "application/json")
            });

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Requests.Add(new CapturedRequest(request.RequestUri?.ToString() ?? string.Empty));

            if (Fault is not null)
            {
                return Task.FromException<HttpResponseMessage>(Fault);
            }

            return Task.FromResult(_responses.Count > 0
                ? _responses.Dequeue()
                : new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("""{"nodes":[]}""", Encoding.UTF8, "application/json")
                });
        }
    }

    private sealed record CapturedRequest(string Url);
}
