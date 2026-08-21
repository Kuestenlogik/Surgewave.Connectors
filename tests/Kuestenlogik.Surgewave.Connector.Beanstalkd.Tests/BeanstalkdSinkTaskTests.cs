using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Beanstalkd.Tests;

/// <summary>
/// Tests for <see cref="BeanstalkdSinkTask"/> driven through a scripted beanstalkd connection.
/// </summary>
public class BeanstalkdSinkTaskTests
{
    private static Dictionary<string, string> Config() =>
        new()
        {
            [BeanstalkdConnectorConfig.Topics] = "jobs",
            [BeanstalkdConnectorConfig.Tube] = "inbox",
            [BeanstalkdConnectorConfig.Priority] = "7",
            [BeanstalkdConnectorConfig.DelaySeconds] = "3",
            [BeanstalkdConnectorConfig.TtrSeconds] = "90"
        };

    private static SinkRecord CreateRecord(string value, long offset = 0) =>
        new()
        {
            Topic = "jobs",
            Partition = 0,
            Offset = offset,
            Value = Encoding.UTF8.GetBytes(value)
        };

    [Fact]
    public async Task PutAsync_SendsThePutCommandWithTheConfiguredPriorityDelayAndTtr()
    {
        using var stream = new ScriptedStream("INSERTED 1\r\n");
        using var task = new BeanstalkdSinkTask();
        task.StartWith(Config(), new BeanstalkClient(stream));

        await task.PutAsync([CreateRecord("hi")], CancellationToken.None);

        Assert.Equal("put 7 3 90 2\r\nhi\r\n", stream.WrittenText);
    }

    [Fact]
    public async Task PutAsync_SendsOneJobPerRecord()
    {
        using var stream = new ScriptedStream("INSERTED 1\r\nINSERTED 2\r\n");
        using var task = new BeanstalkdSinkTask();
        task.StartWith(Config(), new BeanstalkClient(stream));

        await task.PutAsync([CreateRecord("one"), CreateRecord("two", 1)], CancellationToken.None);

        Assert.Equal("put 7 3 90 3\r\none\r\nput 7 3 90 3\r\ntwo\r\n", stream.WrittenText);
    }

    [Fact]
    public async Task PutAsync_ThrowsWhenBeanstalkdRejectsTheJob()
    {
        // The batch must fail so the worker retries or dead-letters it instead of
        // committing offsets for a job that was never queued.
        using var stream = new ScriptedStream("OUT_OF_MEMORY\r\n");
        using var task = new BeanstalkdSinkTask();
        task.StartWith(Config(), new BeanstalkClient(stream));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => task.PutAsync([CreateRecord("hi")], CancellationToken.None));

        Assert.Contains("OUT_OF_MEMORY", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_HonoursCancellationBeforeSendingAnything()
    {
        using var stream = new ScriptedStream("INSERTED 1\r\n");
        using var task = new BeanstalkdSinkTask();
        task.StartWith(Config(), new BeanstalkClient(stream));

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => task.PutAsync([CreateRecord("hi")], cts.Token));

        Assert.Equal("", stream.WrittenText);
    }

    [Fact]
    public async Task PutAsync_DoesNothingWithoutAConnection()
    {
        using var task = new BeanstalkdSinkTask();

        var put = task.PutAsync([CreateRecord("hi")], CancellationToken.None);
        await put;

        Assert.True(put.IsCompletedSuccessfully);
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
