using System.Globalization;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.FileStream.Tests;

/// <summary>
/// Tests for <see cref="FileStreamSourceTask"/>. The stored offset is the only thing standing
/// between a restart and duplicated or skipped lines, so it has to count consumed bytes - a
/// read-ahead position would jump past lines that were never emitted.
/// </summary>
public class FileStreamSourceTaskTests : IDisposable
{
    private const string Topic = "lines";

    private readonly string _directory = Path.Combine(
        Path.GetTempPath(),
        "surgewave-filestream-tests",
        Guid.NewGuid().ToString("N"));

    public FileStreamSourceTaskTests() => Directory.CreateDirectory(_directory);

    [Fact]
    public async Task PollAsync_EmitsOneRecordPerLine()
    {
        var path = Write("alpha\nbeta\ngamma\n");
        using var task = StartTask(path, new FakeOffsetStorageReader(null));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(3, records.Count);
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, records.Select(Text));
        Assert.All(records, record => Assert.Equal(Topic, record.Topic));
        Assert.All(records, record => Assert.Equal(path, Assert.IsType<string>(record.SourcePartition["filename"])));
    }

    [Fact]
    public async Task PollAsync_CountsConsumedBytesAsTheOffset()
    {
        // "alpha\n" = 6, "beta\n" = 5, "gamma\n" = 6 - the last offset is the whole file.
        var path = Write("alpha\nbeta\ngamma\n");
        using var task = StartTask(path, new FakeOffsetStorageReader(null));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(new[] { 6L, 11L, 17L }, records.Select(Position));
        Assert.Equal(new FileInfo(path).Length, Position(records[^1]));
    }

    [Fact]
    public async Task PollAsync_StripsCarriageReturnsButStillCountsThem()
    {
        var path = Write("alpha\r\nbeta\r\n");
        using var task = StartTask(path, new FakeOffsetStorageReader(null));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(new[] { "alpha", "beta" }, records.Select(Text));
        Assert.Equal(new[] { 7L, 13L }, records.Select(Position));
        Assert.Equal(new FileInfo(path).Length, Position(records[^1]));
    }

    [Fact]
    public async Task PollAsync_HoldsBackAPartialLineUntilItsTerminatorArrives()
    {
        var path = Write("line1\nparti");
        using var task = StartTask(path, new FakeOffsetStorageReader(null));

        var first = await task.PollAsync(CancellationToken.None);

        var complete = Assert.Single(first);
        Assert.Equal("line1", Text(complete));
        Assert.Equal(6L, Position(complete));

        Append(path, "al-line\n");
        var second = await task.PollAsync(CancellationToken.None);

        var rejoined = Assert.Single(second);
        Assert.Equal("partial-line", Text(rejoined));
        Assert.Equal(new FileInfo(path).Length, Position(rejoined));
    }

    [Fact]
    public async Task Start_ResumesAfterTheStoredPosition()
    {
        var path = Write("a\nb\nc\n");
        var reader = new FakeOffsetStorageReader(new Dictionary<string, object> { ["position"] = 4L });
        using var task = StartTask(path, reader);

        var records = await task.PollAsync(CancellationToken.None);

        var record = Assert.Single(records);
        Assert.Equal("c", Text(record));
        Assert.Equal(6L, Position(record));
        Assert.Equal(path, Assert.IsType<string>(reader.RequestedPartition["filename"]));
    }

    [Fact]
    public async Task PollAsync_OpensAFileThatOnlyAppearsAfterStart()
    {
        // A file that does not exist yet must not park the task forever - the connector is
        // routinely pointed at a log file that some other process creates later.
        var path = Path.Combine(_directory, "appears-later.txt");
        using var task = StartTask(path, new FakeOffsetStorageReader(null));

        Write("written after start\n", "appears-later.txt");
        var records = await task.PollAsync(CancellationToken.None);

        var record = Assert.Single(records);
        Assert.Equal("written after start", Text(record));
    }

    [Fact]
    public async Task PollAsync_CapsABatchAndReturnsTheRestOnTheNextPoll()
    {
        var content = new StringBuilder();
        for (var i = 1; i <= 150; i++)
        {
            content.Append("line-").Append(i.ToString(CultureInfo.InvariantCulture))
                .Append(new string('x', 40)).Append('\n');
        }

        var path = Write(content.ToString());
        using var task = StartTask(path, new FakeOffsetStorageReader(null));

        var first = await task.PollAsync(CancellationToken.None);
        var second = await task.PollAsync(CancellationToken.None);

        Assert.Equal(100, first.Count);
        Assert.Equal(50, second.Count);
        Assert.StartsWith("line-101", Text(second[0]), StringComparison.Ordinal);
        Assert.Equal(new FileInfo(path).Length, Position(second[^1]));
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    private static string Text(SourceRecord record) => Encoding.UTF8.GetString(record.Value);

    private static long Position(SourceRecord record) => Assert.IsType<long>(record.SourceOffset["position"]);

    private static FileStreamSourceTask StartTask(string path, IOffsetStorageReader reader)
    {
        var task = new FileStreamSourceTask();
        task.Initialize(new TaskContext { OffsetStorageReader = reader });
        task.Start(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["file"] = path,
            ["topic"] = Topic
        });
        return task;
    }

    private string Write(string content, string fileName = "input.txt")
    {
        var path = Path.Combine(_directory, fileName);
        File.WriteAllBytes(path, Encoding.UTF8.GetBytes(content));
        return path;
    }

    private static void Append(string path, string content)
    {
        using var stream = new System.IO.FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        var bytes = Encoding.UTF8.GetBytes(content);
        stream.Write(bytes, 0, bytes.Length);
    }

    private sealed class FakeOffsetStorageReader : IOffsetStorageReader
    {
        private readonly IDictionary<string, object>? _offset;

        public FakeOffsetStorageReader(IDictionary<string, object>? offset) => _offset = offset;

        public IDictionary<string, object> RequestedPartition { get; private set; } = new Dictionary<string, object>();

        public IDictionary<string, object>? Offset(IDictionary<string, object> partition)
        {
            RequestedPartition = new Dictionary<string, object>(partition);
            return _offset;
        }

        public IDictionary<IDictionary<string, object>, IDictionary<string, object>> Offsets(
            IReadOnlyCollection<IDictionary<string, object>> partitions) =>
            new Dictionary<IDictionary<string, object>, IDictionary<string, object>>();
    }
}
