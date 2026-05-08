using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sequence.Tests;

/// <summary>
/// A test source task that produces a configurable number of records.
/// </summary>
public sealed class TestSourceTask : SourceTask
{
    private string _topic = "test-topic";
    private int _recordCount = 10;
    private int _currentIndex;
    private bool _disposed;

    public const string RecordCountConfig = "record.count";
    public const string TopicConfig = "topic";

    public override string Version => "1.0.0";

    public int RecordsProduced => _currentIndex;

    public override void Start(IDictionary<string, string> config)
    {
        if (config.TryGetValue(RecordCountConfig, out var count))
            _recordCount = int.Parse(count);

        if (config.TryGetValue(TopicConfig, out var topic))
            _topic = topic;

        _currentIndex = 0;
    }

    public override void Stop()
    {
        _currentIndex = 0;
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_currentIndex >= _recordCount)
            return Task.FromResult<IReadOnlyList<SourceRecord>>([]);

        var record = new SourceRecord
        {
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes($"key-{_currentIndex}"),
            Value = Encoding.UTF8.GetBytes($"{{\"index\":{_currentIndex}}}"),
            Timestamp = DateTimeOffset.UtcNow,
            SourcePartition = new Dictionary<string, object> { ["source"] = "test" },
            SourceOffset = new Dictionary<string, object> { ["index"] = _currentIndex }
        };

        _currentIndex++;

        return Task.FromResult<IReadOnlyList<SourceRecord>>([record]);
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            _disposed = true;
        }
        base.Dispose(disposing);
    }
}

/// <summary>
/// A test source task that throws an error after a certain number of records.
/// </summary>
public sealed class ErrorSourceTask : SourceTask
{
    private int _errorAfter = 5;
    private int _currentIndex;
    private bool _disposed;

    public const string ErrorAfterConfig = "error.after";

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        if (config.TryGetValue(ErrorAfterConfig, out var errorAfter))
            _errorAfter = int.Parse(errorAfter);

        _currentIndex = 0;
    }

    public override void Stop()
    {
        _currentIndex = 0;
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_currentIndex >= _errorAfter)
            throw new InvalidOperationException("Simulated error");

        var record = new SourceRecord
        {
            Topic = "test-topic",
            Key = Encoding.UTF8.GetBytes($"key-{_currentIndex}"),
            Value = Encoding.UTF8.GetBytes($"{{\"index\":{_currentIndex}}}"),
            Timestamp = DateTimeOffset.UtcNow,
            SourcePartition = new Dictionary<string, object> { ["source"] = "error-test" },
            SourceOffset = new Dictionary<string, object> { ["index"] = _currentIndex }
        };

        _currentIndex++;

        return Task.FromResult<IReadOnlyList<SourceRecord>>([record]);
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            _disposed = true;
        }
        base.Dispose(disposing);
    }
}

/// <summary>
/// A test source task that produces records infinitely.
/// </summary>
public sealed class InfiniteSourceTask : SourceTask
{
    private int _currentIndex;
    private bool _disposed;

    public override string Version => "1.0.0";

    public int RecordsProduced => _currentIndex;

    public override void Start(IDictionary<string, string> config)
    {
        _currentIndex = 0;
    }

    public override void Stop()
    {
        _currentIndex = 0;
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var record = new SourceRecord
        {
            Topic = "test-topic",
            Key = Encoding.UTF8.GetBytes($"key-{_currentIndex}"),
            Value = Encoding.UTF8.GetBytes($"{{\"index\":{_currentIndex}}}"),
            Timestamp = DateTimeOffset.UtcNow,
            SourcePartition = new Dictionary<string, object> { ["source"] = "infinite" },
            SourceOffset = new Dictionary<string, object> { ["index"] = _currentIndex }
        };

        _currentIndex++;

        return Task.FromResult<IReadOnlyList<SourceRecord>>([record]);
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            _disposed = true;
        }
        base.Dispose(disposing);
    }
}
