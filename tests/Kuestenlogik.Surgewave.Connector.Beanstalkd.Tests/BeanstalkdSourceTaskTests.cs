using System.Text;

namespace Kuestenlogik.Surgewave.Connector.Beanstalkd.Tests;

/// <summary>
/// Tests for <see cref="BeanstalkdSourceTask"/> driven through a scripted beanstalkd connection.
/// </summary>
public class BeanstalkdSourceTaskTests
{
    private static Dictionary<string, string> Config(string? batchSize = null) =>
        new()
        {
            [BeanstalkdConnectorConfig.Topic] = "jobs",
            [BeanstalkdConnectorConfig.Tube] = "inbox",
            [BeanstalkdConnectorConfig.Host] = "beans.example.com",
            [BeanstalkdConnectorConfig.Port] = "11301",
            [BeanstalkdConnectorConfig.ReserveTimeoutSeconds] = "2",
            [BeanstalkdConnectorConfig.BatchSize] = batchSize ?? "100"
        };

    [Fact]
    public async Task PollAsync_ReturnsEmptyWhenNoConnectionHasBeenEstablished()
    {
        using var task = new BeanstalkdSourceTask();

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Empty(records);
    }

    [Fact]
    public async Task PollAsync_MapsEveryReservedJobToASourceRecord()
    {
        using var stream = new ScriptedStream("RESERVED 1 5\r\nalpha\r\nRESERVED 2 4\r\nbeta\r\nTIMED_OUT\r\n");
        using var task = new BeanstalkdSourceTask();
        task.StartWith(Config(), new BeanstalkClient(stream));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(2, records.Count);

        var first = records[0];
        Assert.Equal("jobs", first.Topic);
        Assert.Equal("1", Encoding.UTF8.GetString(first.Key!));
        Assert.Equal("alpha", Encoding.UTF8.GetString(first.Value));
        Assert.Equal(1L, first.SourceOffset[BeanstalkdConnectorConfig.OffsetJobId]);
        Assert.Equal("inbox", Encoding.UTF8.GetString(first.Headers!["beanstalkd.tube"]));
        Assert.Equal("beans.example.com", first.SourcePartition["host"]);
        Assert.Equal("inbox", first.SourcePartition["tube"]);

        Assert.Equal("beta", Encoding.UTF8.GetString(records[1].Value));
        Assert.Equal(2L, records[1].SourceOffset[BeanstalkdConnectorConfig.OffsetJobId]);
    }

    [Fact]
    public async Task PollAsync_StopsReservingAtTheConfiguredBatchSize()
    {
        using var stream = new ScriptedStream("RESERVED 1 5\r\nalpha\r\nRESERVED 2 4\r\nbeta\r\n");
        using var task = new BeanstalkdSourceTask();
        task.StartWith(Config(batchSize: "1"), new BeanstalkClient(stream));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Single(records);
        Assert.Equal("reserve-with-timeout 2\r\n", stream.WrittenText);
    }

    [Fact]
    public async Task PollAsync_SwallowsProtocolFailuresAndKeepsWhatItAlreadyRead()
    {
        // Second reserve gets a response the client cannot parse.
        using var stream = new ScriptedStream("RESERVED 1 5\r\nalpha\r\nGARBAGE\r\n");
        using var task = new BeanstalkdSourceTask();
        task.StartWith(Config(), new BeanstalkClient(stream));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Single(records);
        Assert.Equal("alpha", Encoding.UTF8.GetString(records[0].Value));
    }

    [Fact]
    public async Task CommitAsync_DeletesEveryJobThatWasHandedOut()
    {
        using var stream = new ScriptedStream(
            "RESERVED 1 5\r\nalpha\r\nRESERVED 2 4\r\nbeta\r\nTIMED_OUT\r\nDELETED\r\nDELETED\r\n");
        using var task = new BeanstalkdSourceTask();
        task.StartWith(Config(), new BeanstalkClient(stream));

        await task.PollAsync(CancellationToken.None);
        await task.CommitAsync(CancellationToken.None);

        Assert.EndsWith("delete 1\r\ndelete 2\r\n", stream.WrittenText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CommitAsync_DoesNothingWhenNothingWasReserved()
    {
        using var stream = new ScriptedStream("");
        using var task = new BeanstalkdSourceTask();
        task.StartWith(Config(), new BeanstalkClient(stream));

        await task.CommitAsync(CancellationToken.None);

        Assert.Equal("", stream.WrittenText);
    }

    /// <summary>
    /// Stand-in for the beanstalkd TCP connection: reads are served from a scripted server
    /// response, writes are captured for assertions.
    /// </summary>
    private sealed class ScriptedStream : Stream
    {
        private readonly byte[] _serverResponse;
        private readonly List<byte> _written = [];
        private int _readPos;

        public ScriptedStream(string serverResponse)
        {
            _serverResponse = Encoding.ASCII.GetBytes(serverResponse);
        }

        public string WrittenText => Encoding.ASCII.GetString(_written.ToArray());

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            var available = _serverResponse.Length - _readPos;
            if (available <= 0 || buffer.Length == 0)
                return 0;

            var take = Math.Min(available, buffer.Length);
            _serverResponse.AsSpan(_readPos, take).CopyTo(buffer);
            _readPos += take;
            return take;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => Task.FromResult(Read(buffer.AsSpan(offset, count)));

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(Read(buffer.Span));

        public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

        public override void Write(ReadOnlySpan<byte> buffer) => _written.AddRange(buffer.ToArray());

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            Write(buffer.AsSpan(offset, count));
            return Task.CompletedTask;
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Write(buffer.Span);
            return ValueTask.CompletedTask;
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();
    }
}
