using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Kuestenlogik.Surgewave.Connector.SocketStream.Tests;

public class SocketStreamSourceTests : IDisposable
{
    private TcpListener? _server;
    private readonly List<TcpClient> _clients = [];
    private int _port;

    private void StartTcpServer()
    {
        _port = 30000 + Random.Shared.Next(1000);
        _server = new TcpListener(IPAddress.Loopback, _port);
        _server.Start();
    }

    public void Dispose()
    {
        foreach (var client in _clients)
        {
            try { client.Dispose(); } catch { }
        }
        _clients.Clear();
        try { _server?.Stop(); } catch { }
    }

    [Fact]
    public async Task ConnectsToTcpServer_LineFraming()
    {
        StartTcpServer();

        var task = new SocketStreamSourceTask();
        var config = new Dictionary<string, string>
        {
            [SocketStreamConnectorConfig.SocketType] = "tcp",
            [SocketStreamConnectorConfig.Host] = "127.0.0.1",
            [SocketStreamConnectorConfig.Port] = _port.ToString(),
            [SocketStreamConnectorConfig.Topic] = "test-topic",
            [SocketStreamConnectorConfig.Framing] = "line",
            [SocketStreamConnectorConfig.ReconnectEnabled] = "false"
        };

        task.Start(config);

        // Accept connection and send data
        var client = await _server!.AcceptTcpClientAsync();
        _clients.Add(client);

        var stream = client.GetStream();
        await stream.WriteAsync(Encoding.UTF8.GetBytes("Hello\nWorld\n"));
        await stream.FlushAsync();

        // Wait for messages to be processed
        await Task.Delay(200);

        var records = await task.PollAsync(CancellationToken.None);
        Assert.Equal(2, records.Count);
        Assert.Equal("Hello", Encoding.UTF8.GetString(records[0].Value!));
        Assert.Equal("World", Encoding.UTF8.GetString(records[1].Value!));
        Assert.Equal("test-topic", records[0].Topic);

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task HandlesDelimiterFraming()
    {
        StartTcpServer();

        var task = new SocketStreamSourceTask();
        var config = new Dictionary<string, string>
        {
            [SocketStreamConnectorConfig.SocketType] = "tcp",
            [SocketStreamConnectorConfig.Host] = "127.0.0.1",
            [SocketStreamConnectorConfig.Port] = _port.ToString(),
            [SocketStreamConnectorConfig.Topic] = "test-topic",
            [SocketStreamConnectorConfig.Framing] = "delimiter",
            [SocketStreamConnectorConfig.Delimiter] = "||",
            [SocketStreamConnectorConfig.ReconnectEnabled] = "false"
        };

        task.Start(config);

        var client = await _server!.AcceptTcpClientAsync();
        _clients.Add(client);

        var stream = client.GetStream();
        await stream.WriteAsync(Encoding.UTF8.GetBytes("Message1||Message2||"));
        await stream.FlushAsync();

        await Task.Delay(200);

        var records = await task.PollAsync(CancellationToken.None);
        Assert.Equal(2, records.Count);
        Assert.Equal("Message1", Encoding.UTF8.GetString(records[0].Value!));
        Assert.Equal("Message2", Encoding.UTF8.GetString(records[1].Value!));

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task HandlesLengthPrefixFraming_BigEndian()
    {
        StartTcpServer();

        var task = new SocketStreamSourceTask();
        var config = new Dictionary<string, string>
        {
            [SocketStreamConnectorConfig.SocketType] = "tcp",
            [SocketStreamConnectorConfig.Host] = "127.0.0.1",
            [SocketStreamConnectorConfig.Port] = _port.ToString(),
            [SocketStreamConnectorConfig.Topic] = "test-topic",
            [SocketStreamConnectorConfig.Framing] = "length-prefix",
            [SocketStreamConnectorConfig.LengthPrefixBytes] = "4",
            [SocketStreamConnectorConfig.LengthPrefixBigEndian] = "true",
            [SocketStreamConnectorConfig.ReconnectEnabled] = "false"
        };

        task.Start(config);

        var client = await _server!.AcceptTcpClientAsync();
        _clients.Add(client);

        var stream = client.GetStream();

        // Send message with 4-byte big-endian length prefix
        var message = Encoding.UTF8.GetBytes("Test Message");
        var length = message.Length;
        var lengthBytes = new byte[] { (byte)(length >> 24), (byte)(length >> 16), (byte)(length >> 8), (byte)length };

        await stream.WriteAsync(lengthBytes);
        await stream.WriteAsync(message);
        await stream.FlushAsync();

        await Task.Delay(200);

        var records = await task.PollAsync(CancellationToken.None);
        Assert.Single(records);
        Assert.Equal("Test Message", Encoding.UTF8.GetString(records[0].Value!));

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task HandlesRawFraming()
    {
        StartTcpServer();

        var task = new SocketStreamSourceTask();
        var config = new Dictionary<string, string>
        {
            [SocketStreamConnectorConfig.SocketType] = "tcp",
            [SocketStreamConnectorConfig.Host] = "127.0.0.1",
            [SocketStreamConnectorConfig.Port] = _port.ToString(),
            [SocketStreamConnectorConfig.Topic] = "test-topic",
            [SocketStreamConnectorConfig.Framing] = "raw",
            [SocketStreamConnectorConfig.ReconnectEnabled] = "false"
        };

        task.Start(config);

        var client = await _server!.AcceptTcpClientAsync();
        _clients.Add(client);

        var stream = client.GetStream();
        await stream.WriteAsync(Encoding.UTF8.GetBytes("Raw data chunk"));
        await stream.FlushAsync();

        await Task.Delay(200);

        var records = await task.PollAsync(CancellationToken.None);
        Assert.Single(records);
        Assert.Equal("Raw data chunk", Encoding.UTF8.GetString(records[0].Value!));

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task IncludesMetadataHeaders()
    {
        StartTcpServer();

        var task = new SocketStreamSourceTask();
        var config = new Dictionary<string, string>
        {
            [SocketStreamConnectorConfig.SocketType] = "tcp",
            [SocketStreamConnectorConfig.Host] = "127.0.0.1",
            [SocketStreamConnectorConfig.Port] = _port.ToString(),
            [SocketStreamConnectorConfig.Topic] = "test-topic",
            [SocketStreamConnectorConfig.Framing] = "line",
            [SocketStreamConnectorConfig.ReconnectEnabled] = "false"
        };

        task.Start(config);

        var client = await _server!.AcceptTcpClientAsync();
        _clients.Add(client);

        var stream = client.GetStream();
        await stream.WriteAsync(Encoding.UTF8.GetBytes("Test\n"));
        await stream.FlushAsync();

        await Task.Delay(200);

        var records = await task.PollAsync(CancellationToken.None);
        Assert.Single(records);

        var record = records[0];
        Assert.NotNull(record.Headers);
        Assert.Equal("tcp", Encoding.UTF8.GetString(record.Headers["socket_type"]));
        Assert.Equal("line", Encoding.UTF8.GetString(record.Headers["framing"]));

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task HandlesWindowsLineEndings()
    {
        StartTcpServer();

        var task = new SocketStreamSourceTask();
        var config = new Dictionary<string, string>
        {
            [SocketStreamConnectorConfig.SocketType] = "tcp",
            [SocketStreamConnectorConfig.Host] = "127.0.0.1",
            [SocketStreamConnectorConfig.Port] = _port.ToString(),
            [SocketStreamConnectorConfig.Topic] = "test-topic",
            [SocketStreamConnectorConfig.Framing] = "line",
            [SocketStreamConnectorConfig.ReconnectEnabled] = "false"
        };

        task.Start(config);

        var client = await _server!.AcceptTcpClientAsync();
        _clients.Add(client);

        var stream = client.GetStream();
        await stream.WriteAsync(Encoding.UTF8.GetBytes("Line1\r\nLine2\r\n"));
        await stream.FlushAsync();

        await Task.Delay(200);

        var records = await task.PollAsync(CancellationToken.None);
        Assert.Equal(2, records.Count);
        Assert.Equal("Line1", Encoding.UTF8.GetString(records[0].Value!));
        Assert.Equal("Line2", Encoding.UTF8.GetString(records[1].Value!));

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task MultipleMessages_IncrementingOffsets()
    {
        StartTcpServer();

        var task = new SocketStreamSourceTask();
        var config = new Dictionary<string, string>
        {
            [SocketStreamConnectorConfig.SocketType] = "tcp",
            [SocketStreamConnectorConfig.Host] = "127.0.0.1",
            [SocketStreamConnectorConfig.Port] = _port.ToString(),
            [SocketStreamConnectorConfig.Topic] = "test-topic",
            [SocketStreamConnectorConfig.Framing] = "line",
            [SocketStreamConnectorConfig.ReconnectEnabled] = "false"
        };

        task.Start(config);

        var client = await _server!.AcceptTcpClientAsync();
        _clients.Add(client);

        var stream = client.GetStream();
        for (int i = 0; i < 5; i++)
        {
            await stream.WriteAsync(Encoding.UTF8.GetBytes($"Message{i}\n"));
        }
        await stream.FlushAsync();

        await Task.Delay(300);

        var records = await task.PollAsync(CancellationToken.None);
        Assert.Equal(5, records.Count);

        for (int i = 0; i < 5; i++)
        {
            Assert.Equal($"Message{i}", Encoding.UTF8.GetString(records[i].Value!));
            Assert.Equal((long)(i + 1), records[i].SourceOffset!["message_id"]);
        }

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public void ConnectorValidation_RequiresTopic()
    {
        var connector = new SocketStreamSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SocketStreamConnectorConfig.Host] = "localhost",
            [SocketStreamConnectorConfig.Port] = "9999"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void ConnectorValidation_RequiresUnixPath()
    {
        var connector = new SocketStreamSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SocketStreamConnectorConfig.SocketType] = "unix",
            [SocketStreamConnectorConfig.Topic] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void ConnectorValidation_InvalidFraming()
    {
        var connector = new SocketStreamSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SocketStreamConnectorConfig.Host] = "localhost",
            [SocketStreamConnectorConfig.Port] = "9999",
            [SocketStreamConnectorConfig.Topic] = "test-topic",
            [SocketStreamConnectorConfig.Framing] = "invalid"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }
}
