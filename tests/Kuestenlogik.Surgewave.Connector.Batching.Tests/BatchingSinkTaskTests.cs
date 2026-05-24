using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Xunit;
using Kuestenlogik.Surgewave.Connector.Batching;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Batching.Tests;

public class BatchingSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedVersion()
    {
        using var task = new BatchingSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithDefaultConfig_InitializesSuccessfully()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>();

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void CurrentBatchCount_InitiallyZero()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>();

        task.Start(config);

        Assert.Equal(0, task.CurrentBatchCount);

        task.Stop();
    }

    [Fact]
    public void CurrentBatchBytes_InitiallyZero()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>();

        task.Start(config);

        Assert.Equal(0, task.CurrentBatchBytes);

        task.Stop();
    }

    [Fact]
    public async Task PutAsync_IncrementsCount()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100"
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = null,
                Value = Encoding.UTF8.GetBytes("{\"test\":\"value\"}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(1, task.CurrentBatchCount);

        task.Stop();
    }

    [Fact]
    public async Task PutAsync_IncrementsBytesCount()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100"
        };

        task.Start(config);

        var value = Encoding.UTF8.GetBytes("{\"test\":\"value\"}");
        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = null,
                Value = value,
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(value.Length, task.CurrentBatchBytes);

        task.Stop();
    }

    [Fact]
    public async Task PutAsync_WithKey_IncrementsBytesIncludingKey()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100"
        };

        task.Start(config);

        var key = Encoding.UTF8.GetBytes("key1");
        var value = Encoding.UTF8.GetBytes("{\"test\":\"value\"}");
        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = key,
                Value = value,
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(key.Length + value.Length, task.CurrentBatchBytes);

        task.Stop();
    }

    [Fact]
    public async Task PutAsync_ReachesBatchMaxMessages_TriggersBatchComplete()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "2"
        };

        task.Start(config);

        var batchCount = 0;
        task.OnBatchReady += _ => batchCount++;

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = null,
                Value = Encoding.UTF8.GetBytes("{\"id\":1}"),
                Timestamp = DateTimeOffset.UtcNow
            },
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 1,
                Key = null,
                Value = Encoding.UTF8.GetBytes("{\"id\":2}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(1, batchCount);
        Assert.Equal(0, task.CurrentBatchCount);

        task.Stop();
    }

    [Fact]
    public async Task PutAsync_ExceedsBatchMaxBytes_FlushesBefore()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.BatchMaxBytesConfig] = "50"
        };

        task.Start(config);

        var batchCount = 0;
        task.OnBatchReady += _ => batchCount++;

        var records1 = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = null,
                Value = Encoding.UTF8.GetBytes(new string('a', 30)),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records1, CancellationToken.None);
        Assert.Equal(0, batchCount);
        Assert.Equal(1, task.CurrentBatchCount);

        // Adding another 30-byte message exceeds 50 bytes limit
        var records2 = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 1,
                Key = null,
                Value = Encoding.UTF8.GetBytes(new string('b', 30)),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records2, CancellationToken.None);
        Assert.Equal(1, batchCount);
        Assert.Equal(1, task.CurrentBatchCount); // New message is in buffer

        task.Stop();
    }

    [Fact]
    public void Flush_WithEmptyBuffer_ReturnsNull()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>();

        task.Start(config);

        var result = task.Flush();
        Assert.Null(result);

        task.Stop();
    }

    [Fact]
    public async Task Flush_WithMessages_ReturnsBatchedRecord()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100"
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = null,
                Value = Encoding.UTF8.GetBytes("{\"test\":\"value\"}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        var result = task.Flush();

        Assert.NotNull(result);
        Assert.NotNull(result.Value);

        // Buffer should be reset
        Assert.Equal(0, task.CurrentBatchCount);

        task.Stop();
    }

    [Fact]
    public async Task GetBatches_ReturnsCompletedBatches()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "2"
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = null,
                Value = Encoding.UTF8.GetBytes("{\"id\":1}"),
                Timestamp = DateTimeOffset.UtcNow
            },
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 1,
                Key = null,
                Value = Encoding.UTF8.GetBytes("{\"id\":2}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        var batches = task.GetBatches();

        Assert.Single(batches);
        Assert.Equal(2, batches[0].MessageCount);

        // GetBatches clears the list
        var batches2 = task.GetBatches();
        Assert.Empty(batches2);

        task.Stop();
    }

    [Fact]
    public async Task Flush_WithJsonArrayFormat_ProducesValidJsonArray()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.BatchFormatConfig] = BatchingConnectorConfig.FormatJsonArray
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = null,
                Value = Encoding.UTF8.GetBytes("{\"id\":1}"),
                Timestamp = DateTimeOffset.UtcNow
            },
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 1,
                Key = null,
                Value = Encoding.UTF8.GetBytes("{\"id\":2}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        var result = task.Flush();
        Assert.NotNull(result);

        var json = Encoding.UTF8.GetString(result.Value);
        var array = JsonNode.Parse(json) as JsonArray;

        Assert.NotNull(array);
        Assert.Equal(2, array.Count);

        task.Stop();
    }

    [Fact]
    public async Task Flush_WithJsonLinesFormat_ProducesValidJsonLines()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.BatchFormatConfig] = BatchingConnectorConfig.FormatJsonLines,
            [BatchingConnectorConfig.SeparatorConfig] = "\n"
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = null,
                Value = Encoding.UTF8.GetBytes("{\"id\":1}"),
                Timestamp = DateTimeOffset.UtcNow
            },
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 1,
                Key = null,
                Value = Encoding.UTF8.GetBytes("{\"id\":2}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        var result = task.Flush();
        Assert.NotNull(result);

        var json = Encoding.UTF8.GetString(result.Value);
        var lines = json.Split('\n');

        Assert.Equal(2, lines.Length);

        task.Stop();
    }

    [Fact]
    public async Task Flush_WithRawFormat_ConcatenatesValues()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.BatchFormatConfig] = BatchingConnectorConfig.FormatRaw,
            [BatchingConnectorConfig.SeparatorConfig] = "|"
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = null,
                Value = Encoding.UTF8.GetBytes("value1"),
                Timestamp = DateTimeOffset.UtcNow
            },
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 1,
                Key = null,
                Value = Encoding.UTF8.GetBytes("value2"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        var result = task.Flush();
        Assert.NotNull(result);

        var content = Encoding.UTF8.GetString(result.Value);
        Assert.Equal("value1|value2", content);

        task.Stop();
    }

    [Fact]
    public async Task Flush_WithKeyStrategyFirst_UsesFirstKey()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.KeyStrategyConfig] = BatchingConnectorConfig.KeyStrategyFirst
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = Encoding.UTF8.GetBytes("key1"),
                Value = Encoding.UTF8.GetBytes("{\"id\":1}"),
                Timestamp = DateTimeOffset.UtcNow
            },
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 1,
                Key = Encoding.UTF8.GetBytes("key2"),
                Value = Encoding.UTF8.GetBytes("{\"id\":2}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        var result = task.Flush();
        Assert.NotNull(result);
        Assert.Equal("key1", Encoding.UTF8.GetString(result.Key!));

        task.Stop();
    }

    [Fact]
    public async Task Flush_WithKeyStrategyLast_UsesLastKey()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.KeyStrategyConfig] = BatchingConnectorConfig.KeyStrategyLast
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = Encoding.UTF8.GetBytes("key1"),
                Value = Encoding.UTF8.GetBytes("{\"id\":1}"),
                Timestamp = DateTimeOffset.UtcNow
            },
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 1,
                Key = Encoding.UTF8.GetBytes("key2"),
                Value = Encoding.UTF8.GetBytes("{\"id\":2}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        var result = task.Flush();
        Assert.NotNull(result);
        Assert.Equal("key2", Encoding.UTF8.GetString(result.Key!));

        task.Stop();
    }

    [Fact]
    public async Task Flush_WithKeyStrategyNull_ReturnsNullKey()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.KeyStrategyConfig] = BatchingConnectorConfig.KeyStrategyNull
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = Encoding.UTF8.GetBytes("key1"),
                Value = Encoding.UTF8.GetBytes("{\"id\":1}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        var result = task.Flush();
        Assert.NotNull(result);
        Assert.Null(result.Key);

        task.Stop();
    }

    [Fact]
    public async Task Flush_WithKeyStrategyConcat_ConcatenatesKeys()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.KeyStrategyConfig] = BatchingConnectorConfig.KeyStrategyConcat,
            [BatchingConnectorConfig.SeparatorConfig] = ","
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = Encoding.UTF8.GetBytes("key1"),
                Value = Encoding.UTF8.GetBytes("{\"id\":1}"),
                Timestamp = DateTimeOffset.UtcNow
            },
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 1,
                Key = Encoding.UTF8.GetBytes("key2"),
                Value = Encoding.UTF8.GetBytes("{\"id\":2}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        var result = task.Flush();
        Assert.NotNull(result);
        Assert.Equal("key1,key2", Encoding.UTF8.GetString(result.Key!));

        task.Stop();
    }

    [Fact]
    public async Task Flush_WithIncludeMetadata_IncludesMetadataInOutput()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.BatchFormatConfig] = BatchingConnectorConfig.FormatJsonArray,
            [BatchingConnectorConfig.IncludeMetadataConfig] = "true"
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = Encoding.UTF8.GetBytes("key1"),
                Value = Encoding.UTF8.GetBytes("{\"id\":1}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        var result = task.Flush();
        Assert.NotNull(result);

        var json = Encoding.UTF8.GetString(result.Value);
        var array = JsonNode.Parse(json) as JsonArray;

        Assert.NotNull(array);
        Assert.Single(array);

        var item = array[0] as JsonObject;
        Assert.NotNull(item);
        Assert.NotNull(item["topic"]);
        Assert.NotNull(item["partition"]);
        Assert.NotNull(item["offset"]);
        Assert.NotNull(item["key"]);
        Assert.NotNull(item["value"]);
        Assert.NotNull(item["timestamp"]);

        task.Stop();
    }

    [Fact]
    public async Task Flush_WithGzipCompression_ProducesCompressedOutput()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.CompressionConfig] = BatchingConnectorConfig.CompressionGzip
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = null,
                Value = Encoding.UTF8.GetBytes("{\"id\":1}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        var result = task.Flush();
        Assert.NotNull(result);
        Assert.True(result.IsCompressed);

        // Verify it's valid gzip by decompressing
        using var input = new MemoryStream(result.Value);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        gzip.CopyTo(output);

        var decompressed = Encoding.UTF8.GetString(output.ToArray());
        Assert.Contains("1", decompressed);

        task.Stop();
    }

    [Fact]
    public async Task PutAsync_WithFlushOnKeyChange_FlushesOnKeyChange()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.FlushOnKeyChangeConfig] = "true"
        };

        task.Start(config);

        var batchCount = 0;
        task.OnBatchReady += _ => batchCount++;

        var key1 = Encoding.UTF8.GetBytes("key1");
        var key2 = Encoding.UTF8.GetBytes("key2");

        var records1 = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = key1,
                Value = Encoding.UTF8.GetBytes("{\"id\":1}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records1, CancellationToken.None);
        Assert.Equal(0, batchCount);
        Assert.Equal(1, task.CurrentBatchCount);

        // Different key should trigger flush
        var records2 = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 1,
                Key = key2,
                Value = Encoding.UTF8.GetBytes("{\"id\":2}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records2, CancellationToken.None);
        Assert.Equal(1, batchCount);
        Assert.Equal(1, task.CurrentBatchCount); // New message in buffer

        task.Stop();
    }

    [Fact]
    public void BatchedRecord_ContainsCorrectProperties()
    {
        var batch = new BatchedRecord
        {
            Key = Encoding.UTF8.GetBytes("test-key"),
            Value = Encoding.UTF8.GetBytes("test-value"),
            Timestamp = DateTimeOffset.UtcNow,
            MessageCount = 5,
            TotalBytes = 100,
            IsCompressed = true
        };

        Assert.Equal("test-key", Encoding.UTF8.GetString(batch.Key!));
        Assert.Equal("test-value", Encoding.UTF8.GetString(batch.Value));
        Assert.Equal(5, batch.MessageCount);
        Assert.Equal(100, batch.TotalBytes);
        Assert.True(batch.IsCompressed);
    }

    [Fact]
    public async Task OnBatchReady_EventFiresOnFlush()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100"
        };

        task.Start(config);

        BatchedRecord? receivedBatch = null;
        task.OnBatchReady += batch => receivedBatch = batch;

        // Manually add records and flush
        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = null,
                Value = Encoding.UTF8.GetBytes("{\"id\":1}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);
        task.Flush();

        Assert.NotNull(receivedBatch);
        Assert.Equal(1, receivedBatch!.MessageCount);

        task.Stop();
    }

    [Fact]
    public async Task Stop_FlushesRemainingMessages()
    {
        using var task = new BatchingSinkTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100"
        };

        task.Start(config);

        var batchCount = 0;
        task.OnBatchReady += _ => batchCount++;

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = null,
                Value = Encoding.UTF8.GetBytes("{\"test\":\"value\"}"),
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(1, task.CurrentBatchCount);
        Assert.Equal(0, batchCount);

        task.Stop();

        Assert.Equal(0, task.CurrentBatchCount);
        Assert.Equal(1, batchCount);
    }
}
