using System.Net;
using System.Net.Sockets;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Tcp;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Tcp.Tests;

/// <summary>
/// Tests for TCP source task.
/// </summary>
public sealed class TcpSourceTaskTests : IDisposable
{
    private readonly List<TcpSourceTask> _tasks = [];

    public void Dispose()
    {
        foreach (var task in _tasks)
        {
            try { task.Stop(); } catch { }
            try { task.Dispose(); } catch { }
        }
    }

    private TcpSourceTask CreateTask()
    {
        var task = new TcpSourceTask();
        _tasks.Add(task);
        return task;
    }

    [Fact]
    public void TcpSourceTask_HasCorrectVersion()
    {
        var task = CreateTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task TcpSourceTask_AcceptsConnection()
    {
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var port = GetFreePort();
        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Topic] = "test-topic",
            [TcpConnectorConfig.ListenAddress] = "127.0.0.1",
            [TcpConnectorConfig.ListenPort] = port.ToString()
        };

        task.Start(config);
        await Task.Delay(100);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port);

        Assert.True(client.Connected);

        task.Stop();
    }

    [Fact]
    public async Task TcpSourceTask_ReceivesLineFramedMessages()
    {
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var port = GetFreePort();
        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Topic] = "test-topic",
            [TcpConnectorConfig.ListenAddress] = "127.0.0.1",
            [TcpConnectorConfig.ListenPort] = port.ToString(),
            [TcpConnectorConfig.Framing] = TcpConnectorConfig.FramingLine
        };

        task.Start(config);
        await Task.Delay(100);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port);
        var stream = client.GetStream();
        var writer = new StreamWriter(stream) { AutoFlush = true };

        await writer.WriteLineAsync("message1");
        await writer.WriteLineAsync("message2");
        await writer.WriteLineAsync("message3");

        await Task.Delay(200);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.True(records.Count >= 3);
        Assert.All(records, r => Assert.Equal("test-topic", r.Topic));

        var messages = records.Select(r => Encoding.UTF8.GetString(r.Value)).ToList();
        Assert.Contains("message1", messages);
        Assert.Contains("message2", messages);
        Assert.Contains("message3", messages);

        task.Stop();
    }

    [Fact]
    public async Task TcpSourceTask_ReceivesDelimiterFramedMessages()
    {
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var port = GetFreePort();
        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Topic] = "test-topic",
            [TcpConnectorConfig.ListenAddress] = "127.0.0.1",
            [TcpConnectorConfig.ListenPort] = port.ToString(),
            [TcpConnectorConfig.Framing] = TcpConnectorConfig.FramingDelimiter,
            [TcpConnectorConfig.Delimiter] = "||"
        };

        task.Start(config);
        await Task.Delay(100);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port);
        var stream = client.GetStream();

        var data = Encoding.UTF8.GetBytes("msg1||msg2||msg3||");
        await stream.WriteAsync(data);
        await stream.FlushAsync();

        await Task.Delay(200);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.True(records.Count >= 3);

        var messages = records.Select(r => Encoding.UTF8.GetString(r.Value)).ToList();
        Assert.Contains("msg1", messages);
        Assert.Contains("msg2", messages);
        Assert.Contains("msg3", messages);

        task.Stop();
    }

    [Fact]
    public async Task TcpSourceTask_ReceivesLengthPrefixedMessages()
    {
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var port = GetFreePort();
        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Topic] = "test-topic",
            [TcpConnectorConfig.ListenAddress] = "127.0.0.1",
            [TcpConnectorConfig.ListenPort] = port.ToString(),
            [TcpConnectorConfig.Framing] = TcpConnectorConfig.FramingLengthPrefix,
            [TcpConnectorConfig.LengthPrefixBytes] = "4",
            [TcpConnectorConfig.LengthPrefixBigEndian] = "true"
        };

        task.Start(config);
        await Task.Delay(100);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port);
        var stream = client.GetStream();

        // Send length-prefixed message (4-byte big-endian length + data)
        var message = Encoding.UTF8.GetBytes("Hello, World!");
        var lengthBytes = new byte[4];
        lengthBytes[0] = (byte)(message.Length >> 24);
        lengthBytes[1] = (byte)(message.Length >> 16);
        lengthBytes[2] = (byte)(message.Length >> 8);
        lengthBytes[3] = (byte)message.Length;

        await stream.WriteAsync(lengthBytes);
        await stream.WriteAsync(message);
        await stream.FlushAsync();

        await Task.Delay(200);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.Single(records);
        Assert.Equal("Hello, World!", Encoding.UTF8.GetString(records[0].Value));

        task.Stop();
    }

    [Fact]
    public async Task TcpSourceTask_HandlesMultipleConnections()
    {
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var port = GetFreePort();
        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Topic] = "test-topic",
            [TcpConnectorConfig.ListenAddress] = "127.0.0.1",
            [TcpConnectorConfig.ListenPort] = port.ToString(),
            [TcpConnectorConfig.MaxConnections] = "10"
        };

        task.Start(config);
        await Task.Delay(100);

        var clients = new List<TcpClient>();
        try
        {
            for (int i = 0; i < 3; i++)
            {
                var client = new TcpClient();
                await client.ConnectAsync(IPAddress.Loopback, port);
                clients.Add(client);

                var stream = client.GetStream();
                var writer = new StreamWriter(stream) { AutoFlush = true };
                await writer.WriteLineAsync($"client{i}");
            }

            await Task.Delay(300);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var records = await task.PollAsync(cts.Token);

            Assert.True(records.Count >= 3);
        }
        finally
        {
            foreach (var client in clients)
            {
                client.Dispose();
            }
        }

        task.Stop();
    }

    [Fact]
    public async Task TcpSourceTask_SetsConnectionIdInKey()
    {
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var port = GetFreePort();
        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Topic] = "test-topic",
            [TcpConnectorConfig.ListenAddress] = "127.0.0.1",
            [TcpConnectorConfig.ListenPort] = port.ToString()
        };

        task.Start(config);
        await Task.Delay(100);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port);
        var stream = client.GetStream();
        var writer = new StreamWriter(stream) { AutoFlush = true };
        await writer.WriteLineAsync("test");

        await Task.Delay(200);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.NotEmpty(records);
        var key = Encoding.UTF8.GetString(records[0].Key!);
        Assert.StartsWith("conn-", key);

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
