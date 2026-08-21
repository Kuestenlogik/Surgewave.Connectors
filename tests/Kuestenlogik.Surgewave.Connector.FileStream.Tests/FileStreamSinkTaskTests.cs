using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.FileStream.Tests;

/// <summary>
/// Tests for <see cref="FileStreamSinkTask"/>. A compacted topic delivers tombstones with a
/// null value; decoding one without a guard takes the whole task down.
/// </summary>
public class FileStreamSinkTaskTests : IDisposable
{
    private readonly string _directory = Path.Combine(
        Path.GetTempPath(),
        "surgewave-filestream-sink-tests",
        Guid.NewGuid().ToString("N"));

    public FileStreamSinkTaskTests() => Directory.CreateDirectory(_directory);

    [Fact]
    public async Task PutAsync_WritesOneLinePerRecord()
    {
        var path = Path.Combine(_directory, "out.txt");

        using (var task = StartTask(path))
        {
            await task.PutAsync([Record("first"), Record("second")], CancellationToken.None);
            await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
        }

        Assert.Equal(new[] { "first", "second" }, File.ReadAllLines(path));
    }

    [Fact]
    public async Task PutAsync_SkipsATombstoneInsteadOfFailingTheBatch()
    {
        var path = Path.Combine(_directory, "out.txt");

        using (var task = StartTask(path))
        {
            await task.PutAsync([Record("before"), Tombstone(), Record("after")], CancellationToken.None);
            await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
        }

        Assert.Equal(new[] { "before", "after" }, File.ReadAllLines(path));
    }

    [Fact]
    public async Task Start_AppendsToAnExistingFile()
    {
        var path = Path.Combine(_directory, "out.txt");
        File.WriteAllBytes(path, Encoding.UTF8.GetBytes("from an earlier run\n"));

        using (var task = StartTask(path))
        {
            await task.PutAsync([Record("from this run")], CancellationToken.None);
            await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
        }

        Assert.Equal(new[] { "from an earlier run", "from this run" }, File.ReadAllLines(path));
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    private static FileStreamSinkTask StartTask(string path)
    {
        var task = new FileStreamSinkTask();
        task.Initialize(new TaskContext());
        task.Start(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["file"] = path,
            ["topics"] = "lines"
        });
        return task;
    }

    private static SinkRecord Record(string value) => new()
    {
        Topic = "lines",
        Partition = 0,
        Offset = 1,
        Value = Encoding.UTF8.GetBytes(value)
    };

    private static SinkRecord Tombstone() => new()
    {
        Topic = "lines",
        Partition = 0,
        Offset = 2,
        Key = Encoding.UTF8.GetBytes("deleted-key"),
        Value = null!
    };
}
