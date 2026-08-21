using System.Text;

namespace Kuestenlogik.Surgewave.Connector.Beanstalkd.Tests;

/// <summary>
/// Wire-protocol tests for <see cref="BeanstalkClient"/>. The TCP connection is replaced by a
/// scripted stream, so the framing is verified byte for byte without a beanstalkd server.
/// </summary>
public class BeanstalkClientTests
{
    [Fact]
    public async Task UseAsync_TerminatesTheCommandWithCarriageReturnLineFeed()
    {
        // beanstalkd requires \r\n; Environment.NewLine would break every command on Linux.
        using var stream = new ScriptedStream("USING inbox\r\n");
        using var client = new BeanstalkClient(stream);

        await client.UseAsync("inbox");

        Assert.Equal("use inbox\r\n", stream.WrittenText);
    }

    [Fact]
    public async Task WatchAsync_ThrowsWhenTheServerRejectsTheTube()
    {
        using var stream = new ScriptedStream("NOT_FOUND\r\n");
        using var client = new BeanstalkClient(stream);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => client.WatchAsync("inbox"));

        Assert.Contains("NOT_FOUND", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReserveAsync_ReadsAJobBodyThatArrivedTogetherWithItsHeader()
    {
        // Header and body arrive in a single segment, so the body is already sitting in the
        // client's read buffer once the RESERVED line has been consumed.
        using var stream = new ScriptedStream("RESERVED 42 5\r\nhello\r\n");
        using var client = new BeanstalkClient(stream);

        var job = await client.ReserveAsync(TimeSpan.FromSeconds(3));

        Assert.NotNull(job);
        Assert.Equal(42L, job.Id);
        Assert.Equal("hello", Encoding.ASCII.GetString(job.Data));
        Assert.Equal("reserve-with-timeout 3\r\n", stream.WrittenText);
    }

    [Fact]
    public async Task ReserveAsync_ReadsAJobBodyThatArrivesOneByteAtATime()
    {
        using var stream = new ScriptedStream("RESERVED 7 11\r\nhello world\r\n", maxChunk: 1);
        using var client = new BeanstalkClient(stream);

        var job = await client.ReserveAsync(TimeSpan.FromSeconds(1));

        Assert.NotNull(job);
        Assert.Equal(7L, job.Id);
        Assert.Equal("hello world", Encoding.ASCII.GetString(job.Data));
    }

    [Fact]
    public async Task ReserveAsync_ReadsABinaryBodyContainingLineBreaks()
    {
        var body = new byte[] { 0x00, (byte)'a', 0x0D, 0x0A, (byte)'b', 0xFF };
        var script = new List<byte>();
        script.AddRange(Encoding.ASCII.GetBytes($"RESERVED 9 {body.Length}\r\n"));
        script.AddRange(body);
        script.AddRange(Encoding.ASCII.GetBytes("\r\n"));

        using var stream = new ScriptedStream(script.ToArray());
        using var client = new BeanstalkClient(stream);

        var job = await client.ReserveAsync(TimeSpan.FromSeconds(1));

        Assert.NotNull(job);
        Assert.Equal(body, job.Data);
    }

    [Fact]
    public async Task ReserveAsync_ReturnsNullWhenTheReserveTimesOut()
    {
        using var stream = new ScriptedStream("TIMED_OUT\r\n");
        using var client = new BeanstalkClient(stream);

        var job = await client.ReserveAsync(TimeSpan.FromSeconds(5));

        Assert.Null(job);
        Assert.Equal("reserve-with-timeout 5\r\n", stream.WrittenText);
    }

    [Fact]
    public async Task ReserveAsync_LeavesTheConnectionInSyncForTheNextCommand()
    {
        // The \r\n after the job body must be consumed, otherwise the delete response is
        // read as an empty line and every following command is off by one.
        using var stream = new ScriptedStream("RESERVED 7 2\r\nok\r\nDELETED\r\n");
        using var client = new BeanstalkClient(stream);

        var job = await client.ReserveAsync(TimeSpan.FromSeconds(1));
        Assert.NotNull(job);

        await client.DeleteAsync(job.Id);

        Assert.Equal("reserve-with-timeout 1\r\ndelete 7\r\n", stream.WrittenText);
    }

    [Fact]
    public async Task PutAsync_WritesTheJobFrameAndReturnsTheInsertedId()
    {
        using var stream = new ScriptedStream("INSERTED 15\r\n");
        using var client = new BeanstalkClient(stream);

        var id = await client.PutAsync(
            Encoding.ASCII.GetBytes("hello"),
            priority: 1024,
            delay: TimeSpan.FromSeconds(0),
            ttr: TimeSpan.FromSeconds(60));

        Assert.Equal(15L, id);
        Assert.Equal("put 1024 0 60 5\r\nhello\r\n", stream.WrittenText);
    }

    [Fact]
    public async Task PutAsync_ThrowsWhenTheServerRejectsTheJob()
    {
        using var stream = new ScriptedStream("JOB_TOO_BIG\r\n");
        using var client = new BeanstalkClient(stream);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => client.PutAsync(Encoding.ASCII.GetBytes("x"), 1, TimeSpan.Zero, TimeSpan.FromSeconds(1)));

        Assert.Contains("JOB_TOO_BIG", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DeleteAsync_TreatsNotFoundAsSuccess()
    {
        using var stream = new ScriptedStream("NOT_FOUND\r\n");
        using var client = new BeanstalkClient(stream);

        await client.DeleteAsync(3);

        Assert.Equal("delete 3\r\n", stream.WrittenText);
    }

    [Fact]
    public async Task DeleteAsync_ThrowsOnAnUnexpectedResponse()
    {
        using var stream = new ScriptedStream("BAD_FORMAT\r\n");
        using var client = new BeanstalkClient(stream);

        await Assert.ThrowsAsync<InvalidOperationException>(() => client.DeleteAsync(3));
    }

    /// <summary>
    /// Stand-in for the beanstalkd TCP connection: reads are served from a scripted server
    /// response (optionally in small chunks), writes are captured for assertions.
    /// </summary>
    private sealed class ScriptedStream : Stream
    {
        private readonly byte[] _serverResponse;
        private readonly int _maxChunk;
        private readonly List<byte> _written = [];
        private int _readPos;

        public ScriptedStream(string serverResponse, int maxChunk = int.MaxValue)
            : this(Encoding.ASCII.GetBytes(serverResponse), maxChunk)
        {
        }

        public ScriptedStream(byte[] serverResponse, int maxChunk = int.MaxValue)
        {
            _serverResponse = serverResponse;
            _maxChunk = maxChunk;
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

            var take = Math.Min(Math.Min(available, buffer.Length), _maxChunk);
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
