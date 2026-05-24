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
    private TcpClient? _tcpClient;
    private NetworkStream? _stream;
    private StreamReader? _reader;
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
        _reader = new StreamReader(_stream, Encoding.ASCII);
        _writer = new StreamWriter(_stream, Encoding.ASCII) { AutoFlush = true };
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
            var offset = 0;
            while (offset < bytes)
            {
                var read = await _stream!.ReadAsync(data.AsMemory(offset, bytes - offset));
                if (read == 0) break;
                offset += read;
            }

            // Read trailing \r\n
            await _reader!.ReadLineAsync();

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
        return await _reader!.ReadLineAsync() ?? string.Empty;
    }

    public void Dispose()
    {
        _reader?.Dispose();
        _writer?.Dispose();
        _stream?.Dispose();
        _tcpClient?.Dispose();
    }
}

/// <summary>
/// Represents a beanstalkd job.
/// </summary>
internal sealed record BeanstalkJob(long Id, byte[] Data);
