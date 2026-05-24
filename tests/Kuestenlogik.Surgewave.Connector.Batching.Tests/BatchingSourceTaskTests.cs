using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Xunit;
using Kuestenlogik.Surgewave.Connector.Batching;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Batching.Tests;

public class BatchingSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedVersion()
    {
        using var task = new BatchingSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithDefaultConfig_InitializesSuccessfully()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void CurrentBatchCount_InitiallyZero()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);

        Assert.Equal(0, task.CurrentBatchCount);

        task.Stop();
    }

    [Fact]
    public void CurrentBatchBytes_InitiallyZero()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);

        Assert.Equal(0, task.CurrentBatchBytes);

        task.Stop();
    }

    [Fact]
    public void AddMessage_IncrementsCount()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100"
        };

        task.Start(config);

        var value = Encoding.UTF8.GetBytes("{\"test\":\"value\"}");
        task.AddMessage(null, value, DateTimeOffset.UtcNow);

        Assert.Equal(1, task.CurrentBatchCount);

        task.Stop();
    }

    [Fact]
    public void AddMessage_IncrementsBytesCount()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100"
        };

        task.Start(config);

        var value = Encoding.UTF8.GetBytes("{\"test\":\"value\"}");
        task.AddMessage(null, value, DateTimeOffset.UtcNow);

        Assert.Equal(value.Length, task.CurrentBatchBytes);

        task.Stop();
    }

    [Fact]
    public void AddMessage_WithKey_IncrementsBytesIncludingKey()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100"
        };

        task.Start(config);

        var key = Encoding.UTF8.GetBytes("key1");
        var value = Encoding.UTF8.GetBytes("{\"test\":\"value\"}");
        task.AddMessage(key, value, DateTimeOffset.UtcNow);

        Assert.Equal(key.Length + value.Length, task.CurrentBatchBytes);

        task.Stop();
    }

    [Fact]
    public void AddMessage_ReachesBatchMaxMessages_ReturnsBatchedRecord()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "3"
        };

        task.Start(config);

        var value = Encoding.UTF8.GetBytes("{\"test\":\"value\"}");

        var result1 = task.AddMessage(null, value, DateTimeOffset.UtcNow);
        var result2 = task.AddMessage(null, value, DateTimeOffset.UtcNow);
        var result3 = task.AddMessage(null, value, DateTimeOffset.UtcNow);

        Assert.Empty(result1);
        Assert.Empty(result2);
        Assert.Single(result3);

        // Buffer should be reset
        Assert.Equal(0, task.CurrentBatchCount);

        task.Stop();
    }

    [Fact]
    public void AddMessage_ExceedsBatchMaxBytes_FlushesBefore()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.BatchMaxBytesConfig] = "50"
        };

        task.Start(config);

        var value = Encoding.UTF8.GetBytes(new string('a', 30));

        var result1 = task.AddMessage(null, value, DateTimeOffset.UtcNow);
        Assert.Empty(result1);
        Assert.Equal(1, task.CurrentBatchCount);

        // Adding another 30-byte message exceeds 50 bytes limit
        var result2 = task.AddMessage(null, value, DateTimeOffset.UtcNow);
        Assert.Single(result2);
        Assert.Equal(1, task.CurrentBatchCount); // New message is in buffer

        task.Stop();
    }

    [Fact]
    public void Flush_WithEmptyBuffer_ReturnsNull()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);

        var result = task.Flush();
        Assert.Null(result);

        task.Stop();
    }

    [Fact]
    public void Flush_WithMessages_ReturnsBatchedRecord()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100"
        };

        task.Start(config);

        var value = Encoding.UTF8.GetBytes("{\"test\":\"value\"}");
        task.AddMessage(null, value, DateTimeOffset.UtcNow);
        task.AddMessage(null, value, DateTimeOffset.UtcNow);

        var result = task.Flush();

        Assert.NotNull(result);
        Assert.Equal("test-topic", result.Topic);
        Assert.NotNull(result.Value);

        // Buffer should be reset
        Assert.Equal(0, task.CurrentBatchCount);

        task.Stop();
    }

    [Fact]
    public void Flush_WithJsonArrayFormat_ProducesValidJsonArray()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.BatchFormatConfig] = BatchingConnectorConfig.FormatJsonArray
        };

        task.Start(config);

        task.AddMessage(null, Encoding.UTF8.GetBytes("{\"id\":1}"), DateTimeOffset.UtcNow);
        task.AddMessage(null, Encoding.UTF8.GetBytes("{\"id\":2}"), DateTimeOffset.UtcNow);

        var result = task.Flush();
        Assert.NotNull(result);

        var json = Encoding.UTF8.GetString(result.Value!);
        var array = JsonNode.Parse(json) as JsonArray;

        Assert.NotNull(array);
        Assert.Equal(2, array.Count);

        task.Stop();
    }

    [Fact]
    public void Flush_WithJsonLinesFormat_ProducesValidJsonLines()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.BatchFormatConfig] = BatchingConnectorConfig.FormatJsonLines,
            [BatchingConnectorConfig.SeparatorConfig] = "\n"
        };

        task.Start(config);

        task.AddMessage(null, Encoding.UTF8.GetBytes("{\"id\":1}"), DateTimeOffset.UtcNow);
        task.AddMessage(null, Encoding.UTF8.GetBytes("{\"id\":2}"), DateTimeOffset.UtcNow);

        var result = task.Flush();
        Assert.NotNull(result);

        var json = Encoding.UTF8.GetString(result.Value!);
        var lines = json.Split('\n');

        Assert.Equal(2, lines.Length);

        task.Stop();
    }

    [Fact]
    public void Flush_WithRawFormat_ConcatenatesValues()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.BatchFormatConfig] = BatchingConnectorConfig.FormatRaw,
            [BatchingConnectorConfig.SeparatorConfig] = "|"
        };

        task.Start(config);

        task.AddMessage(null, Encoding.UTF8.GetBytes("value1"), DateTimeOffset.UtcNow);
        task.AddMessage(null, Encoding.UTF8.GetBytes("value2"), DateTimeOffset.UtcNow);

        var result = task.Flush();
        Assert.NotNull(result);

        var content = Encoding.UTF8.GetString(result.Value!);
        Assert.Equal("value1|value2", content);

        task.Stop();
    }

    [Fact]
    public void Flush_WithKeyStrategyFirst_UsesFirstKey()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.KeyStrategyConfig] = BatchingConnectorConfig.KeyStrategyFirst
        };

        task.Start(config);

        task.AddMessage(Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("{\"id\":1}"), DateTimeOffset.UtcNow);
        task.AddMessage(Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("{\"id\":2}"), DateTimeOffset.UtcNow);

        var result = task.Flush();
        Assert.NotNull(result);
        Assert.Equal("key1", Encoding.UTF8.GetString(result.Key!));

        task.Stop();
    }

    [Fact]
    public void Flush_WithKeyStrategyLast_UsesLastKey()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.KeyStrategyConfig] = BatchingConnectorConfig.KeyStrategyLast
        };

        task.Start(config);

        task.AddMessage(Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("{\"id\":1}"), DateTimeOffset.UtcNow);
        task.AddMessage(Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("{\"id\":2}"), DateTimeOffset.UtcNow);

        var result = task.Flush();
        Assert.NotNull(result);
        Assert.Equal("key2", Encoding.UTF8.GetString(result.Key!));

        task.Stop();
    }

    [Fact]
    public void Flush_WithKeyStrategyNull_ReturnsNullKey()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.KeyStrategyConfig] = BatchingConnectorConfig.KeyStrategyNull
        };

        task.Start(config);

        task.AddMessage(Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("{\"id\":1}"), DateTimeOffset.UtcNow);

        var result = task.Flush();
        Assert.NotNull(result);
        Assert.Null(result.Key);

        task.Stop();
    }

    [Fact]
    public void Flush_WithKeyStrategyConcat_ConcatenatesKeys()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.KeyStrategyConfig] = BatchingConnectorConfig.KeyStrategyConcat,
            [BatchingConnectorConfig.SeparatorConfig] = ","
        };

        task.Start(config);

        task.AddMessage(Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("{\"id\":1}"), DateTimeOffset.UtcNow);
        task.AddMessage(Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("{\"id\":2}"), DateTimeOffset.UtcNow);

        var result = task.Flush();
        Assert.NotNull(result);
        Assert.Equal("key1,key2", Encoding.UTF8.GetString(result.Key!));

        task.Stop();
    }

    [Fact]
    public void Flush_WithIncludeMetadata_IncludesMetadataInOutput()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.BatchFormatConfig] = BatchingConnectorConfig.FormatJsonArray,
            [BatchingConnectorConfig.IncludeMetadataConfig] = "true"
        };

        task.Start(config);

        task.AddMessage(Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("{\"id\":1}"), DateTimeOffset.UtcNow);

        var result = task.Flush();
        Assert.NotNull(result);

        var json = Encoding.UTF8.GetString(result.Value!);
        var array = JsonNode.Parse(json) as JsonArray;

        Assert.NotNull(array);
        Assert.Single(array);

        var item = array[0] as JsonObject;
        Assert.NotNull(item);
        Assert.NotNull(item["key"]);
        Assert.NotNull(item["value"]);
        Assert.NotNull(item["timestamp"]);

        task.Stop();
    }

    [Fact]
    public void Flush_WithGzipCompression_ProducesCompressedOutput()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.CompressionConfig] = BatchingConnectorConfig.CompressionGzip
        };

        task.Start(config);

        task.AddMessage(null, Encoding.UTF8.GetBytes("{\"id\":1}"), DateTimeOffset.UtcNow);

        var result = task.Flush();
        Assert.NotNull(result);

        // Verify it's valid gzip by decompressing
        using var input = new MemoryStream(result.Value!);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        gzip.CopyTo(output);

        var decompressed = Encoding.UTF8.GetString(output.ToArray());
        Assert.Contains("1", decompressed);

        task.Stop();
    }

    [Fact]
    public void AddMessage_WithFlushOnKeyChange_FlushesOnKeyChange()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.FlushOnKeyChangeConfig] = "true"
        };

        task.Start(config);

        var key1 = Encoding.UTF8.GetBytes("key1");
        var key2 = Encoding.UTF8.GetBytes("key2");
        var value = Encoding.UTF8.GetBytes("{\"test\":\"value\"}");

        var result1 = task.AddMessage(key1, value, DateTimeOffset.UtcNow);
        Assert.Empty(result1);
        Assert.Equal(1, task.CurrentBatchCount);

        // Different key should trigger flush
        var result2 = task.AddMessage(key2, value, DateTimeOffset.UtcNow);
        Assert.Single(result2);
        Assert.Equal(1, task.CurrentBatchCount); // New message in buffer

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_WithTimeoutExpired_FlushesBatch()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100",
            [BatchingConnectorConfig.BatchTimeoutMsConfig] = "50" // 50ms timeout
        };

        task.Start(config);

        var value = Encoding.UTF8.GetBytes("{\"test\":\"value\"}");
        task.AddMessage(null, value, DateTimeOffset.UtcNow);

        Assert.Equal(1, task.CurrentBatchCount);

        // Wait for timeout
        await Task.Delay(100);

        var records = await task.PollAsync(CancellationToken.None);
        Assert.Single(records);
        Assert.Equal(0, task.CurrentBatchCount);

        task.Stop();
    }

    [Fact]
    public void Stop_FlushesRemainingMessages()
    {
        using var task = new BatchingSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "100"
        };

        task.Start(config);

        var value = Encoding.UTF8.GetBytes("{\"test\":\"value\"}");
        task.AddMessage(null, value, DateTimeOffset.UtcNow);

        Assert.Equal(1, task.CurrentBatchCount);

        task.Stop();

        Assert.Equal(0, task.CurrentBatchCount);
    }
}
