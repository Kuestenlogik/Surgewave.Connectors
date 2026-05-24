using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Kuestenlogik.Surgewave.Connector.SocketServer.Tests;

public class SocketServerSourceTests : IDisposable
{
    private SocketServerSourceTask? _task;
    private int _port;
    private readonly List<TcpClient> _clients = [];

    private void StartServer(string framing = "line", string protocol = "tcp")
    {
        _port = 31000 + Random.Shared.Next(1000);
        _task = new SocketServerSourceTask();

        var config = new Dictionary<string, string>
        {
            [SocketServerConnectorConfig.Protocol] = protocol,
            [SocketServerConnectorConfig.ListenAddress] = "127.0.0.1",
            [SocketServerConnectorConfig.ListenPort] = _port.ToString(),
            [SocketServerConnectorConfig.Topic] = "test-topic",
            [SocketServerConnectorConfig.Framing] = framing,
            [SocketServerConnectorConfig.IncludeClientInfo] = "true"
        };

        _task.Start(config);
    }

    public void Dispose()
    {
        foreach (var client in _clients)
        {
            try { client.Dispose(); } catch { }
        }
        _clients.Clear();
        _task?.Stop();
        _task?.Dispose();
    }

    private async Task<TcpClient> ConnectClientAsync()
    {
        var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", _port);
        _clients.Add(client);
        return client;
    }

    [Fact]
    public async Task AcceptsTcpConnection_LineFraming()
    {
        StartServer("line");
        await Task.Delay(100); // Wait for server to start

        var client = await ConnectClientAsync();
        var stream = client.GetStream();

        await stream.WriteAsync(Encoding.UTF8.GetBytes("Hello\nWorld\n"));
        await stream.FlushAsync();

        await Task.Delay(200);

        var records = await _task!.PollAsync(CancellationToken.None);
        Assert.Equal(2, records.Count);
        Assert.Equal("Hello", Encoding.UTF8.GetString(records[0].Value!));
        Assert.Equal("World", Encoding.UTF8.GetString(records[1].Value!));
    }

    [Fact]
    public async Task AcceptsMultipleClients()
    {
        StartServer("line");
        await Task.Delay(100);

        var client1 = await ConnectClientAsync();
        var client2 = await ConnectClientAsync();

        var stream1 = client1.GetStream();
        var stream2 = client2.GetStream();

        await stream1.WriteAsync(Encoding.UTF8.GetBytes("From Client1\n"));
        await stream2.WriteAsync(Encoding.UTF8.GetBytes("From Client2\n"));
        await stream1.FlushAsync();
        await stream2.FlushAsync();

        await Task.Delay(300);

        var records = await _task!.PollAsync(CancellationToken.None);
        Assert.Equal(2, records.Count);

        var messages = records.Select(r => Encoding.UTF8.GetString(r.Value!)).ToHashSet();
        Assert.Contains("From Client1", messages);
        Assert.Contains("From Client2", messages);
    }

    [Fact]
    public async Task IncludesClientInfoHeaders()
    {
        StartServer("line");
        await Task.Delay(100);

        var client = await ConnectClientAsync();
        var stream = client.GetStream();

        await stream.WriteAsync(Encoding.UTF8.GetBytes("Test\n"));
        await stream.FlushAsync();

        await Task.Delay(200);

        var records = await _task!.PollAsync(CancellationToken.None);
        Assert.Single(records);

        var record = records[0];
        Assert.NotNull(record.Headers);
        Assert.True(record.Headers.ContainsKey("connection_id"));
        Assert.True(record.Headers.ContainsKey("remote_endpoint"));
        Assert.Equal("tcp", Encoding.UTF8.GetString(record.Headers["protocol"]));
    }

    [Fact]
    public async Task HandlesDelimiterFraming()
    {
        _port = 31000 + Random.Shared.Next(1000);
        _task = new SocketServerSourceTask();

        var config = new Dictionary<string, string>
        {
            [SocketServerConnectorConfig.Protocol] = "tcp",
            [SocketServerConnectorConfig.ListenAddress] = "127.0.0.1",
            [SocketServerConnectorConfig.ListenPort] = _port.ToString(),
            [SocketServerConnectorConfig.Topic] = "test-topic",
            [SocketServerConnectorConfig.Framing] = "delimiter",
            [SocketServerConnectorConfig.Delimiter] = ":::"
        };

        _task.Start(config);
        await Task.Delay(100);

        var client = await ConnectClientAsync();
        var stream = client.GetStream();

        await stream.WriteAsync(Encoding.UTF8.GetBytes("Msg1:::Msg2:::"));
        await stream.FlushAsync();

        await Task.Delay(200);

        var records = await _task.PollAsync(CancellationToken.None);
        Assert.Equal(2, records.Count);
        Assert.Equal("Msg1", Encoding.UTF8.GetString(records[0].Value!));
        Assert.Equal("Msg2", Encoding.UTF8.GetString(records[1].Value!));
    }

    [Fact]
    public async Task HandlesLengthPrefixFraming()
    {
        _port = 31000 + Random.Shared.Next(1000);
        _task = new SocketServerSourceTask();

        var config = new Dictionary<string, string>
        {
            [SocketServerConnectorConfig.Protocol] = "tcp",
            [SocketServerConnectorConfig.ListenAddress] = "127.0.0.1",
            [SocketServerConnectorConfig.ListenPort] = _port.ToString(),
            [SocketServerConnectorConfig.Topic] = "test-topic",
            [SocketServerConnectorConfig.Framing] = "length-prefix",
            [SocketServerConnectorConfig.LengthPrefixBytes] = "2",
            [SocketServerConnectorConfig.LengthPrefixBigEndian] = "true"
        };

        _task.Start(config);
        await Task.Delay(100);

        var client = await ConnectClientAsync();
        var stream = client.GetStream();

        // Send 2-byte big-endian length prefix + message
        var message = Encoding.UTF8.GetBytes("Short");
        var length = (ushort)message.Length;
        await stream.WriteAsync(new byte[] { (byte)(length >> 8), (byte)length });
        await stream.WriteAsync(message);
        await stream.FlushAsync();

        await Task.Delay(200);

        var records = await _task.PollAsync(CancellationToken.None);
        Assert.Single(records);
        Assert.Equal("Short", Encoding.UTF8.GetString(records[0].Value!));
    }

    [Fact]
    public async Task HandlesRawFraming()
    {
        _port = 31000 + Random.Shared.Next(1000);
        _task = new SocketServerSourceTask();

        var config = new Dictionary<string, string>
        {
            [SocketServerConnectorConfig.Protocol] = "tcp",
            [SocketServerConnectorConfig.ListenAddress] = "127.0.0.1",
            [SocketServerConnectorConfig.ListenPort] = _port.ToString(),
            [SocketServerConnectorConfig.Topic] = "test-topic",
            [SocketServerConnectorConfig.Framing] = "raw"
        };

        _task.Start(config);
        await Task.Delay(100);

        var client = await ConnectClientAsync();
        var stream = client.GetStream();

        await stream.WriteAsync(Encoding.UTF8.GetBytes("Raw chunk"));
        await stream.FlushAsync();

        await Task.Delay(200);

        var records = await _task.PollAsync(CancellationToken.None);
        Assert.Single(records);
        Assert.Equal("Raw chunk", Encoding.UTF8.GetString(records[0].Value!));
    }

    [Fact]
    public async Task ReceivesUdpDatagrams()
    {
        _port = 31000 + Random.Shared.Next(1000);
        _task = new SocketServerSourceTask();

        var config = new Dictionary<string, string>
        {
            [SocketServerConnectorConfig.Protocol] = "udp",
            [SocketServerConnectorConfig.ListenAddress] = "127.0.0.1",
            [SocketServerConnectorConfig.ListenPort] = _port.ToString(),
            [SocketServerConnectorConfig.Topic] = "test-topic",
            [SocketServerConnectorConfig.UdpIncludeSourceInfo] = "true"
        };

        _task.Start(config);
        await Task.Delay(100);

        using var udpClient = new UdpClient();
        var data = Encoding.UTF8.GetBytes("UDP Message");
        await udpClient.SendAsync(data, data.Length, new IPEndPoint(IPAddress.Loopback, _port));

        await Task.Delay(200);

        var records = await _task.PollAsync(CancellationToken.None);
        Assert.Single(records);
        Assert.Equal("UDP Message", Encoding.UTF8.GetString(records[0].Value!));
        Assert.Equal("udp", Encoding.UTF8.GetString(records[0].Headers!["protocol"]));
    }

    [Fact]
    public async Task UdpIncludesSourceInfo()
    {
        _port = 31000 + Random.Shared.Next(1000);
        _task = new SocketServerSourceTask();

        var config = new Dictionary<string, string>
        {
            [SocketServerConnectorConfig.Protocol] = "udp",
            [SocketServerConnectorConfig.ListenAddress] = "127.0.0.1",
            [SocketServerConnectorConfig.ListenPort] = _port.ToString(),
            [SocketServerConnectorConfig.Topic] = "test-topic",
            [SocketServerConnectorConfig.UdpIncludeSourceInfo] = "true"
        };

        _task.Start(config);
        await Task.Delay(100);

        using var udpClient = new UdpClient();
        var data = Encoding.UTF8.GetBytes("Test");
        await udpClient.SendAsync(data, data.Length, new IPEndPoint(IPAddress.Loopback, _port));

        await Task.Delay(200);

        var records = await _task.PollAsync(CancellationToken.None);
        Assert.Single(records);
        Assert.True(records[0].Headers!.ContainsKey("source_ip"));
        Assert.True(records[0].Headers!.ContainsKey("source_port"));
    }

    [Fact]
    public async Task HandlesWindowsLineEndings()
    {
        StartServer("line");
        await Task.Delay(100);

        var client = await ConnectClientAsync();
        var stream = client.GetStream();

        await stream.WriteAsync(Encoding.UTF8.GetBytes("Line1\r\nLine2\r\n"));
        await stream.FlushAsync();

        await Task.Delay(200);

        var records = await _task!.PollAsync(CancellationToken.None);
        Assert.Equal(2, records.Count);
        Assert.Equal("Line1", Encoding.UTF8.GetString(records[0].Value!));
        Assert.Equal("Line2", Encoding.UTF8.GetString(records[1].Value!));
    }

    [Fact]
    public async Task TracksDifferentConnectionIds()
    {
        StartServer("line");
        await Task.Delay(100);

        var client1 = await ConnectClientAsync();
        var client2 = await ConnectClientAsync();

        var stream1 = client1.GetStream();
        var stream2 = client2.GetStream();

        await stream1.WriteAsync(Encoding.UTF8.GetBytes("C1\n"));
        await stream2.WriteAsync(Encoding.UTF8.GetBytes("C2\n"));
        await stream1.FlushAsync();
        await stream2.FlushAsync();

        await Task.Delay(300);

        var records = await _task!.PollAsync(CancellationToken.None);
        Assert.Equal(2, records.Count);

        var connectionIds = records.Select(r => Encoding.UTF8.GetString(r.Headers!["connection_id"])).ToList();
        Assert.NotEqual(connectionIds[0], connectionIds[1]);
    }

    [Fact]
    public void ConnectorValidation_RequiresTopic()
    {
        var connector = new SocketServerSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SocketServerConnectorConfig.ListenPort] = "9999"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void ConnectorValidation_RequiresUnixPath()
    {
        var connector = new SocketServerSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SocketServerConnectorConfig.Protocol] = "unix",
            [SocketServerConnectorConfig.Topic] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void ConnectorValidation_InvalidProtocol()
    {
        var connector = new SocketServerSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SocketServerConnectorConfig.Protocol] = "invalid",
            [SocketServerConnectorConfig.Topic] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void ConnectorValidation_InvalidFraming()
    {
        var connector = new SocketServerSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SocketServerConnectorConfig.Protocol] = "tcp",
            [SocketServerConnectorConfig.ListenPort] = "9999",
            [SocketServerConnectorConfig.Topic] = "test-topic",
            [SocketServerConnectorConfig.Framing] = "invalid"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void ConnectorReturns_SingleTaskConfig()
    {
        var connector = new SocketServerSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SocketServerConnectorConfig.ListenPort] = "9999",
            [SocketServerConnectorConfig.Topic] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        Assert.Single(taskConfigs);
    }
}
