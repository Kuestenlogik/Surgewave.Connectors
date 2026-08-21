using System.Net.Sockets;
using System.Text;

namespace Kuestenlogik.Surgewave.Connector.Beanstalkd;

/// <summary>
/// Simple beanstalkd client using the text-based protocol over TCP.
/// </summary>
internal sealed class BeanstalkClient : IDisposable
{
    private readonly string _host;
    private readonly int _port;
    private readonly byte[] _readBuffer = new byte[8192];
    private int _readBufferPos;
    private int _readBufferLen;
    private TcpClient? _tcpClient;
    private NetworkStream? _stream;
    private StreamWriter? _writer;

    public BeanstalkClient(string host, int port)
    {
        _host = host;
        _port = port;
    }

    public async Task ConnectAsync()
    {
        _tcpClient = new TcpClient();
        await _tcpClient.ConnectAsync(_host, _port);
        _stream = _tcpClient.GetStream();
        // The beanstalkd protocol requires \r\n line termination regardless of platform.
        _writer = new StreamWriter(_stream, Encoding.ASCII) { AutoFlush = true, NewLine = "\r\n" };
        _readBufferPos = 0;
        _readBufferLen = 0;
    }

    public async Task UseAsync(string tube)
    {
        await SendCommandAsync($"use {tube}");
        var response = await ReadLineAsync();
        // Expected: USING <tube>
        if (!response.StartsWith("USING", StringComparison.Ordinal))
            throw new InvalidOperationException($"Failed to use tube: {response}");
    }

    public async Task WatchAsync(string tube)
    {
        await SendCommandAsync($"watch {tube}");
        var response = await ReadLineAsync();
        // Expected: WATCHING <count>
        if (!response.StartsWith("WATCHING", StringComparison.Ordinal))
            throw new InvalidOperationException($"Failed to watch tube: {response}");
    }

    public async Task IgnoreAsync(string tube)
    {
        await SendCommandAsync($"ignore {tube}");
        await ReadLineAsync();
        // Expected: WATCHING <count> or NOT_IGNORED
        // NOT_IGNORED if it's the last tube being watched
    }

    public async Task<long> PutAsync(byte[] data, uint priority, TimeSpan delay, TimeSpan ttr)
    {
        var delaySeconds = (int)delay.TotalSeconds;
        var ttrSeconds = (int)ttr.TotalSeconds;

        await SendCommandAsync($"put {priority} {delaySeconds} {ttrSeconds} {data.Length}");
        await _writer!.FlushAsync();
        await _stream!.WriteAsync(data);
        await _stream.WriteAsync(Encoding.ASCII.GetBytes("\r\n"));
        await _stream.FlushAsync();

        var response = await ReadLineAsync();
        // Expected: INSERTED <id>
        if (response.StartsWith("INSERTED ", StringComparison.Ordinal))
        {
            return long.Parse(response[9..]);
        }
        throw new InvalidOperationException($"Failed to put job: {response}");
    }

    public async Task<BeanstalkJob?> ReserveAsync(TimeSpan timeout)
    {
        await SendCommandAsync($"reserve-with-timeout {(int)timeout.TotalSeconds}");
        var response = await ReadLineAsync();

        if (response == "TIMED_OUT")
            return null;

        // Expected: RESERVED <id> <bytes>
        if (response.StartsWith("RESERVED ", StringComparison.Ordinal))
        {
            var parts = response.Split(' ');
            var id = long.Parse(parts[1]);
            var bytes = int.Parse(parts[2]);

            var data = new byte[bytes];
            await ReadExactAsync(data);

            // Consume the trailing \r\n after the job body
            await ReadLineAsync();

            return new BeanstalkJob(id, data);
        }

        throw new InvalidOperationException($"Failed to reserve job: {response}");
    }

    public async Task DeleteAsync(long jobId)
    {
        await SendCommandAsync($"delete {jobId}");
        var response = await ReadLineAsync();
        // Expected: DELETED or NOT_FOUND
        if (response != "DELETED" && response != "NOT_FOUND")
            throw new InvalidOperationException($"Failed to delete job: {response}");
    }

    private async Task SendCommandAsync(string command)
    {
        await _writer!.WriteLineAsync(command);
        await _writer.FlushAsync();
    }

    private async Task<string> ReadLineAsync()
    {
        var line = new StringBuilder();
        while (true)
        {
            var b = await ReadByteAsync();
            if (b < 0 || b == '\n')
                break;
            line.Append((char)b);
        }
        if (line.Length > 0 && line[^1] == '\r')
            line.Length--;
        return line.ToString();
    }

    private async Task<int> ReadByteAsync()
    {
        if (_readBufferPos >= _readBufferLen)
        {
            _readBufferLen = await _stream!.ReadAsync(_readBuffer.AsMemory());
            _readBufferPos = 0;
            if (_readBufferLen == 0)
                return -1;
        }
        return _readBuffer[_readBufferPos++];
    }

    private async Task ReadExactAsync(byte[] data)
    {
        var offset = 0;
        while (offset < data.Length)
        {
            var buffered = _readBufferLen - _readBufferPos;
            if (buffered > 0)
            {
                var take = Math.Min(buffered, data.Length - offset);
                Array.Copy(_readBuffer, _readBufferPos, data, offset, take);
                _readBufferPos += take;
                offset += take;
                continue;
            }

            var read = await _stream!.ReadAsync(data.AsMemory(offset));
            if (read == 0)
                throw new InvalidOperationException("Connection closed while reading job data");
            offset += read;
        }
    }

    public void Dispose()
    {
        _writer?.Dispose();
        _stream?.Dispose();
        _tcpClient?.Dispose();
    }
}

/// <summary>
/// Represents a beanstalkd job.
/// </summary>
internal sealed record BeanstalkJob(long Id, byte[] Data);
