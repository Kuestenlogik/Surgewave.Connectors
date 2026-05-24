using System.Net;
using System.Net.Sockets;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Udp;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Udp.Tests;

/// <summary>
/// Tests for UDP source task.
/// </summary>
public sealed class UdpSourceTaskTests : IDisposable
{
    private readonly List<UdpSourceTask> _tasks = [];

    public void Dispose()
    {
        foreach (var task in _tasks)
        {
            try { task.Stop(); } catch { }
            try { task.Dispose(); } catch { }
        }
    }

    private UdpSourceTask CreateTask()
    {
        var task = new UdpSourceTask();
        _tasks.Add(task);
        return task;
    }

    private static int GetFreePort()
    {
        using var listener = new UdpClient(0);
        return ((IPEndPoint)listener.Client.LocalEndPoint!).Port;
    }

    [Fact]
    public void UdpSourceTask_HasCorrectVersion()
    {
        var task = CreateTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task UdpSourceTask_ReceivesDatagrams()
    {
        var port = GetFreePort();
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Topic] = "test-topic",
            [UdpConnectorConfig.ListenAddress] = "127.0.0.1",
            [UdpConnectorConfig.ListenPort] = port.ToString()
        };

        task.Start(config);

        // Give time for the receiver to start
        await Task.Delay(100);

        // Send a UDP datagram
        using var client = new UdpClient();
        var message = Encoding.UTF8.GetBytes("Hello UDP!");
        client.Send(message, message.Length, "127.0.0.1", port);

        // Give time for message to be received
        await Task.Delay(200);

        // Poll for records
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.NotEmpty(records);
        Assert.Equal("Hello UDP!", Encoding.UTF8.GetString(records[0].Value!));
        Assert.Equal("test-topic", records[0].Topic);

        task.Stop();
    }

    [Fact]
    public async Task UdpSourceTask_IncludesSourceInfo()
    {
        var port = GetFreePort();
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Topic] = "test-topic",
            [UdpConnectorConfig.ListenAddress] = "127.0.0.1",
            [UdpConnectorConfig.ListenPort] = port.ToString(),
            [UdpConnectorConfig.IncludeSourceInfo] = "true"
        };

        task.Start(config);
        await Task.Delay(100);

        using var client = new UdpClient();
        var message = Encoding.UTF8.GetBytes("test");
        client.Send(message, message.Length, "127.0.0.1", port);

        await Task.Delay(200);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.NotEmpty(records);
        Assert.NotNull(records[0].Headers);
        Assert.True(records[0].Headers!.ContainsKey("udp_source_ip"));
        Assert.True(records[0].Headers!.ContainsKey("udp_source_port"));

        task.Stop();
    }

    [Fact]
    public async Task UdpSourceTask_HandlesMultipleDatagrams()
    {
        var port = GetFreePort();
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Topic] = "test-topic",
            [UdpConnectorConfig.ListenAddress] = "127.0.0.1",
            [UdpConnectorConfig.ListenPort] = port.ToString()
        };

        task.Start(config);
        await Task.Delay(100);

        using var client = new UdpClient();
        for (int i = 0; i < 5; i++)
        {
            var message = Encoding.UTF8.GetBytes($"Message {i}");
            client.Send(message, message.Length, "127.0.0.1", port);
        }

        await Task.Delay(500);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.True(records.Count >= 3); // At least some should arrive

        task.Stop();
    }

    [Fact]
    public async Task UdpSourceTask_ReturnsEmptyWhenNoData()
    {
        var port = GetFreePort();
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Topic] = "test-topic",
            [UdpConnectorConfig.ListenAddress] = "127.0.0.1",
            [UdpConnectorConfig.ListenPort] = port.ToString()
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        var records = await task.PollAsync(cts.Token);

        Assert.Empty(records);

        task.Stop();
    }

    [Fact]
    public void UdpSourceTask_StopsCleanly()
    {
        var port = GetFreePort();
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Topic] = "test-topic",
            [UdpConnectorConfig.ListenAddress] = "127.0.0.1",
            [UdpConnectorConfig.ListenPort] = port.ToString()
        };

        task.Start(config);
        task.Stop();

        // Should not throw
    }
}
