using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.TextChunking;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.TextChunking.Tests;

/// <summary>
/// Tests for the TextChunkingTask class.
/// </summary>
public sealed class TextChunkingTaskTests : IDisposable
{
    private readonly TextChunkingTask _task = new();
    private readonly List<(string Topic, byte[]? Key, byte[]? Value, Dictionary<string, byte[]>? Headers)> _produced = [];

    public TextChunkingTaskTests()
    {
        var producer = new TestProducer(_produced);
        var context = new TaskContext
        {
            RaiseError = _ => { },
            Producer = producer
        };
        _task.Initialize(context);
    }

    public void Dispose()
    {
        _task.Stop();
        _task.Dispose();
    }

    [Fact]
    public void Version_ReturnsExpected()
    {
        Assert.Equal("1.0.0", _task.Version);
    }

    [Fact]
    public async Task PutAsync_EmptyRecords_ProducesNothing()
    {
        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.OutputTopic] = "output",
            [TextChunkingConfig.Strategy] = "fixed-size",
            [TextChunkingConfig.ChunkSize] = "100",
            [TextChunkingConfig.ChunkOverlap] = "20"
        };

        _task.Start(config);
        await _task.PutAsync([], CancellationToken.None);

        Assert.Empty(_produced);
    }

    [Fact]
    public async Task PutAsync_SmallText_ProducesSingleChunk()
    {
        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.OutputTopic] = "output",
            [TextChunkingConfig.Strategy] = "fixed-size",
            [TextChunkingConfig.ChunkSize] = "100",
            [TextChunkingConfig.ChunkOverlap] = "20"
        };

        _task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("Hello, world!") }
        };

        await _task.PutAsync(records, CancellationToken.None);

        Assert.Single(_produced);
        Assert.Equal("output", _produced[0].Topic);
        Assert.Equal("Hello, world!", Encoding.UTF8.GetString(_produced[0].Value!));
    }

    [Fact]
    public async Task PutAsync_LargeText_ProducesMultipleChunks()
    {
        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.OutputTopic] = "output",
            [TextChunkingConfig.Strategy] = "fixed-size",
            [TextChunkingConfig.ChunkSize] = "10",
            [TextChunkingConfig.ChunkOverlap] = "0"
        };

        _task.Start(config);

        var text = "12345678901234567890";
        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes(text) }
        };

        await _task.PutAsync(records, CancellationToken.None);

        Assert.Equal(2, _produced.Count);
    }

    [Fact]
    public async Task PutAsync_IncludesMetadata_WhenEnabled()
    {
        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.OutputTopic] = "output",
            [TextChunkingConfig.Strategy] = "fixed-size",
            [TextChunkingConfig.ChunkSize] = "10",
            [TextChunkingConfig.ChunkOverlap] = "0",
            [TextChunkingConfig.IncludeMetadata] = "true",
            [TextChunkingConfig.MetadataPrefix] = "chunk_"
        };

        _task.Start(config);

        var text = "12345678901234567890";
        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 42, Value = Encoding.UTF8.GetBytes(text) }
        };

        await _task.PutAsync(records, CancellationToken.None);

        Assert.Equal(2, _produced.Count);

        // Check first chunk metadata
        var headers = _produced[0].Headers!;
        Assert.Equal("0", Encoding.UTF8.GetString(headers["chunk_index"]));
        Assert.Equal("2", Encoding.UTF8.GetString(headers["chunk_total"]));
        Assert.Equal("input", Encoding.UTF8.GetString(headers["chunk_original_topic"]));
        Assert.Equal("0", Encoding.UTF8.GetString(headers["chunk_original_partition"]));
        Assert.Equal("42", Encoding.UTF8.GetString(headers["chunk_original_offset"]));
    }

    [Fact]
    public async Task PutAsync_PreservesOriginalHeaders()
    {
        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.OutputTopic] = "output",
            [TextChunkingConfig.Strategy] = "fixed-size",
            [TextChunkingConfig.ChunkSize] = "100",
            [TextChunkingConfig.ChunkOverlap] = "20",
            [TextChunkingConfig.IncludeMetadata] = "true"
        };

        _task.Start(config);

        var originalHeaders = new Dictionary<string, byte[]>
        {
            ["custom_header"] = Encoding.UTF8.GetBytes("custom_value")
        };

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "input",
                Partition = 0,
                Offset = 0,
                Value = Encoding.UTF8.GetBytes("Hello"),
                Headers = originalHeaders
            }
        };

        await _task.PutAsync(records, CancellationToken.None);

        var headers = _produced[0].Headers!;
        Assert.Equal("custom_value", Encoding.UTF8.GetString(headers["custom_header"]));
    }

    [Fact]
    public async Task PutAsync_SentenceStrategy_SplitsOnSentences()
    {
        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.OutputTopic] = "output",
            [TextChunkingConfig.Strategy] = "sentence",
            [TextChunkingConfig.ChunkSize] = "30",
            [TextChunkingConfig.ChunkOverlap] = "0"
        };

        _task.Start(config);

        var text = "First sentence. Second sentence. Third sentence.";
        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes(text) }
        };

        await _task.PutAsync(records, CancellationToken.None);

        Assert.True(_produced.Count >= 1);
    }

    [Fact]
    public async Task PutAsync_ParagraphStrategy_SplitsOnParagraphs()
    {
        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.OutputTopic] = "output",
            [TextChunkingConfig.Strategy] = "paragraph",
            [TextChunkingConfig.ChunkSize] = "30",
            [TextChunkingConfig.ChunkOverlap] = "0"
        };

        _task.Start(config);

        var text = "First para.\n\nSecond para.\n\nThird para.";
        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes(text) }
        };

        await _task.PutAsync(records, CancellationToken.None);

        Assert.True(_produced.Count >= 1);
    }

    [Fact]
    public async Task PutAsync_RecursiveStrategy_Works()
    {
        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.OutputTopic] = "output",
            [TextChunkingConfig.Strategy] = "recursive",
            [TextChunkingConfig.ChunkSize] = "50",
            [TextChunkingConfig.ChunkOverlap] = "0"
        };

        _task.Start(config);

        var text = "Paragraph one here.\n\nParagraph two here.\n\nParagraph three here.";
        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes(text) }
        };

        await _task.PutAsync(records, CancellationToken.None);

        Assert.True(_produced.Count >= 1);
    }

    [Fact]
    public async Task PutAsync_EmptyValue_SkipsWhenConfigured()
    {
        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.OutputTopic] = "output",
            [TextChunkingConfig.Strategy] = "fixed-size",
            [TextChunkingConfig.ChunkSize] = "100",
            [TextChunkingConfig.ChunkOverlap] = "20",
            [TextChunkingConfig.SkipEmpty] = "true"
        };

        _task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Value = [] },
            new() { Topic = "input", Partition = 0, Offset = 1, Value = null! }
        };

        await _task.PutAsync(records, CancellationToken.None);

        Assert.Empty(_produced);
    }

    [Fact]
    public async Task PutAsync_PreservesKey()
    {
        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.OutputTopic] = "output",
            [TextChunkingConfig.Strategy] = "fixed-size",
            [TextChunkingConfig.ChunkSize] = "100",
            [TextChunkingConfig.ChunkOverlap] = "20"
        };

        _task.Start(config);

        var key = Encoding.UTF8.GetBytes("my-key");
        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Key = key, Value = Encoding.UTF8.GetBytes("Hello") }
        };

        await _task.PutAsync(records, CancellationToken.None);

        Assert.Single(_produced);
        Assert.Equal("my-key", Encoding.UTF8.GetString(_produced[0].Key!));
    }

    [Fact]
    public async Task PutAsync_ChunksByWords_WhenConfigured()
    {
        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.OutputTopic] = "output",
            [TextChunkingConfig.Strategy] = "fixed-size",
            [TextChunkingConfig.ChunkSize] = "3",
            [TextChunkingConfig.ChunkOverlap] = "0",
            [TextChunkingConfig.ChunkUnit] = "words"
        };

        _task.Start(config);

        var text = "one two three four five six";
        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes(text) }
        };

        await _task.PutAsync(records, CancellationToken.None);

        Assert.Equal(2, _produced.Count);
        Assert.Equal("one two three", Encoding.UTF8.GetString(_produced[0].Value!));
        Assert.Equal("four five six", Encoding.UTF8.GetString(_produced[1].Value!));
    }

    [Fact]
    public void Start_ThrowsOnUnknownStrategy()
    {
        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.OutputTopic] = "output",
            [TextChunkingConfig.Strategy] = "unknown-strategy"
        };

        Assert.Throws<ArgumentException>(() => _task.Start(config));
    }

    private sealed class TestProducer : IConnectProducer
    {
        private readonly List<(string Topic, byte[]? Key, byte[]? Value, Dictionary<string, byte[]>? Headers)> _produced;

        public TestProducer(List<(string Topic, byte[]? Key, byte[]? Value, Dictionary<string, byte[]>? Headers)> produced)
        {
            _produced = produced;
        }

        public Task ProduceAsync(string topic, byte[]? key, byte[]? value, CancellationToken cancellationToken = default)
        {
            _produced.Add((topic, key, value, null));
            return Task.CompletedTask;
        }

        public Task ProduceAsync(string topic, byte[]? key, byte[]? value, IDictionary<string, byte[]>? headers, CancellationToken cancellationToken = default)
        {
            _produced.Add((topic, key, value, headers != null ? new Dictionary<string, byte[]>(headers) : null));
            return Task.CompletedTask;
        }
    }
}
