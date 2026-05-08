using System.Net;
using System.Net.Sockets;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Tcp;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Tcp.Tests;

/// <summary>
/// Tests for TCP sink task.
/// </summary>
public sealed class TcpSinkTaskTests : IDisposable
{
    private readonly List<TcpSinkTask> _tasks = [];
    private readonly List<TcpListener> _listeners = [];

    public void Dispose()
    {
        foreach (var task in _tasks)
        {
            try { task.Stop(); } catch { }
            try { task.Dispose(); } catch { }
        }
        foreach (var listener in _listeners)
        {
            try { listener.Stop(); } catch { }
        }
    }

    private TcpSinkTask CreateTask()
    {
        var task = new TcpSinkTask();
        _tasks.Add(task);
        return task;
    }

    private TcpListener CreateListener(int port)
    {
        var listener = new TcpListener(IPAddress.Loopback, port);
        _listeners.Add(listener);
        return listener;
    }

    [Fact]
    public void TcpSinkTask_HasCorrectVersion()
    {
        var task = CreateTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task TcpSinkTask_ConnectsToServer()
    {
        var port = GetFreePort();
        var listener = CreateListener(port);
        listener.Start();

        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Host] = "127.0.0.1",
            [TcpConnectorConfig.Port] = port.ToString()
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("test") }
        };

        var acceptTask = listener.AcceptTcpClientAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);

        var client = await acceptTask;
        Assert.True(client.Connected);

        client.Dispose();
        task.Stop();
    }

    [Fact]
    public async Task TcpSinkTask_SendsLineFramedMessages()
    {
        var port = GetFreePort();
        var listener = CreateListener(port);
        listener.Start();

        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Host] = "127.0.0.1",
            [TcpConnectorConfig.Port] = port.ToString(),
            [TcpConnectorConfig.Framing] = TcpConnectorConfig.FramingLine
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("message1") },
            new() { Topic = "test", Partition = 0, Offset = 1, Value = Encoding.UTF8.GetBytes("message2") }
        };

        // Start accept but don't await yet - connection happens in PutAsync
        var acceptTask = listener.AcceptTcpClientAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);

        // Now await the accept
        var client = await acceptTask;
        var stream = client.GetStream();
        var reader = new StreamReader(stream);

        // Give time for data to arrive
        await Task.Delay(100);

        var line1 = await reader.ReadLineAsync();
        var line2 = await reader.ReadLineAsync();

        Assert.Equal("message1", line1);
        Assert.Equal("message2", line2);

        client.Dispose();
        task.Stop();
    }

    [Fact]
    public async Task TcpSinkTask_SendsDelimiterFramedMessages()
    {
        var port = GetFreePort();
        var listener = CreateListener(port);
        listener.Start();

        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Host] = "127.0.0.1",
            [TcpConnectorConfig.Port] = port.ToString(),
            [TcpConnectorConfig.Framing] = TcpConnectorConfig.FramingDelimiter,
            [TcpConnectorConfig.Delimiter] = "||"
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("msg1") },
            new() { Topic = "test", Partition = 0, Offset = 1, Value = Encoding.UTF8.GetBytes("msg2") }
        };

        // Start accept but don't await yet - connection happens in PutAsync
        var acceptTask = listener.AcceptTcpClientAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);

        // Now await the accept
        var client = await acceptTask;
        var stream = client.GetStream();

        await Task.Delay(100);

        var buffer = new byte[1024];
        var bytesRead = await stream.ReadAsync(buffer.AsMemory());
        var received = Encoding.UTF8.GetString(buffer, 0, bytesRead);

        Assert.Contains("msg1||", received);
        Assert.Contains("msg2||", received);

        client.Dispose();
        task.Stop();
    }

    [Fact]
    public async Task TcpSinkTask_SendsLengthPrefixedMessages()
    {
        var port = GetFreePort();
        var listener = CreateListener(port);
        listener.Start();

        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Host] = "127.0.0.1",
            [TcpConnectorConfig.Port] = port.ToString(),
            [TcpConnectorConfig.Framing] = TcpConnectorConfig.FramingLengthPrefix,
            [TcpConnectorConfig.LengthPrefixBytes] = "4",
            [TcpConnectorConfig.LengthPrefixBigEndian] = "true"
        };

        task.Start(config);

        var message = "Hello!";
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes(message) }
        };

        // Start accept but don't await yet - connection happens in PutAsync
        var acceptTask = listener.AcceptTcpClientAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);

        // Now await the accept
        var client = await acceptTask;
        var stream = client.GetStream();

        await Task.Delay(100);

        // Read length prefix
        var lengthBytes = new byte[4];
        await stream.ReadExactlyAsync(lengthBytes, 0, 4);
        var length = (lengthBytes[0] << 24) | (lengthBytes[1] << 16) | (lengthBytes[2] << 8) | lengthBytes[3];

        Assert.Equal(message.Length, length);

        // Read message
        var msgBytes = new byte[length];
        await stream.ReadExactlyAsync(msgBytes, 0, length);
        var received = Encoding.UTF8.GetString(msgBytes);

        Assert.Equal(message, received);

        client.Dispose();
        task.Stop();
    }

    [Fact]
    public async Task TcpSinkTask_SkipsEmptyValues()
    {
        var port = GetFreePort();
        var listener = CreateListener(port);
        listener.Start();

        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Host] = "127.0.0.1",
            [TcpConnectorConfig.Port] = port.ToString()
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = [] },
            new() { Topic = "test", Partition = 0, Offset = 1, Value = null! },
            new() { Topic = "test", Partition = 0, Offset = 2, Value = Encoding.UTF8.GetBytes("valid") }
        };

        // Start accept but don't await yet - connection happens in PutAsync
        var acceptTask = listener.AcceptTcpClientAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);

        // Now await the accept
        var client = await acceptTask;
        var stream = client.GetStream();
        var reader = new StreamReader(stream);

        await Task.Delay(100);

        var line = await reader.ReadLineAsync();
        Assert.Equal("valid", line);

        client.Dispose();
        task.Stop();
    }

    [Fact]
    public async Task TcpSinkTask_HandlesEmptyRecordList()
    {
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Host] = "127.0.0.1",
            [TcpConnectorConfig.Port] = "9999"
        };

        task.Start(config);

        var records = new List<SinkRecord>();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);

        // Should not throw
        task.Stop();
    }

    private static int GetFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
