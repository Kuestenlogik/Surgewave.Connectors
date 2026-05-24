using System.Net;
using System.Net.Sockets;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Udp;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Udp.Tests;

/// <summary>
/// Tests for UDP sink task.
/// </summary>
public sealed class UdpSinkTaskTests : IDisposable
{
    private readonly List<UdpSinkTask> _tasks = [];
    private readonly List<UdpClient> _receivers = [];

    public void Dispose()
    {
        foreach (var task in _tasks)
        {
            try { task.Stop(); } catch { }
            try { task.Dispose(); } catch { }
        }
        foreach (var receiver in _receivers)
        {
            try { receiver.Close(); } catch { }
            try { receiver.Dispose(); } catch { }
        }
    }

    private UdpSinkTask CreateTask()
    {
        var task = new UdpSinkTask();
        _tasks.Add(task);
        return task;
    }

    private UdpClient CreateReceiver(int port)
    {
        var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, port));
        _receivers.Add(receiver);
        return receiver;
    }

    private static int GetFreePort()
    {
        using var listener = new UdpClient(0);
        return ((IPEndPoint)listener.Client.LocalEndPoint!).Port;
    }

    [Fact]
    public void UdpSinkTask_HasCorrectVersion()
    {
        var task = CreateTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task UdpSinkTask_SendsDatagrams()
    {
        var port = GetFreePort();
        var receiver = CreateReceiver(port);

        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Host] = "127.0.0.1",
            [UdpConnectorConfig.Port] = port.ToString()
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("Hello UDP!") }
        };

        // Start receiving before sending
        var receiveTask = receiver.ReceiveAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);

        // Wait for the message
        var result = await receiveTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal("Hello UDP!", Encoding.UTF8.GetString(result.Buffer));

        task.Stop();
    }

    [Fact]
    public async Task UdpSinkTask_SendsMultipleDatagrams()
    {
        var port = GetFreePort();
        var receiver = CreateReceiver(port);

        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Host] = "127.0.0.1",
            [UdpConnectorConfig.Port] = port.ToString()
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("Message 1") },
            new() { Topic = "test", Partition = 0, Offset = 1, Value = Encoding.UTF8.GetBytes("Message 2") },
            new() { Topic = "test", Partition = 0, Offset = 2, Value = Encoding.UTF8.GetBytes("Message 3") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);

        // Receive all messages
        var received = new List<string>();
        for (int i = 0; i < 3; i++)
        {
            try
            {
                var result = await receiver.ReceiveAsync().WaitAsync(TimeSpan.FromSeconds(2));
                received.Add(Encoding.UTF8.GetString(result.Buffer));
            }
            catch (TimeoutException)
            {
                break;
            }
        }

        Assert.True(received.Count >= 2);

        task.Stop();
    }

    [Fact]
    public async Task UdpSinkTask_SkipsEmptyValues()
    {
        var port = GetFreePort();
        var receiver = CreateReceiver(port);

        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Host] = "127.0.0.1",
            [UdpConnectorConfig.Port] = port.ToString()
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = [] },
            new() { Topic = "test", Partition = 0, Offset = 1, Value = null! },
            new() { Topic = "test", Partition = 0, Offset = 2, Value = Encoding.UTF8.GetBytes("valid") }
        };

        // Start receiving before sending
        var receiveTask = receiver.ReceiveAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);

        var result = await receiveTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("valid", Encoding.UTF8.GetString(result.Buffer));

        task.Stop();
    }

    [Fact]
    public async Task UdpSinkTask_HandlesEmptyRecordList()
    {
        var port = GetFreePort();
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Host] = "127.0.0.1",
            [UdpConnectorConfig.Port] = port.ToString()
        };

        task.Start(config);

        var records = new List<SinkRecord>();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);

        // Should not throw
        task.Stop();
    }

    [Fact]
    public void UdpSinkTask_StopsCleanly()
    {
        var port = GetFreePort();
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Host] = "127.0.0.1",
            [UdpConnectorConfig.Port] = port.ToString()
        };

        task.Start(config);
        task.Stop();

        // Should not throw
    }
}
