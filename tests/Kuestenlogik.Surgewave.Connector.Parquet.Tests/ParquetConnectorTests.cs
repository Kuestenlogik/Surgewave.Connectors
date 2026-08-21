using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Parquet.Tests;

public class ParquetConnectorTests : IDisposable
{
    private static readonly long[] TestIds = [1, 2, 3];
    private static readonly string[] TestNames = ["Alice", "Bob", "Charlie"];
    private readonly string _testDir;

    public ParquetConnectorTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"parquet-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, recursive: true);
    }

    [Fact]
    public void ParquetConnectorConfig_HasExpectedConstants()
    {
        Assert.Equal("parquet.file.path", ParquetConnectorConfig.FilePath);
        Assert.Equal("parquet.topic", ParquetConnectorConfig.Topic);
        Assert.Equal("parquet.output.path", ParquetConnectorConfig.OutputPath);
        Assert.Equal("gzip", ParquetConnectorConfig.DefaultCompressionCodec);
    }

    [Fact]
    public void ParquetSourceConnector_HasCorrectConfig()
    {
        var connector = new ParquetSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(ParquetSourceTask), connector.TaskClass);
        Assert.NotNull(connector.Config);
    }

    [Fact]
    public void ParquetSourceConnector_ThrowsOnMissingFilePath()
    {
        var connector = new ParquetSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ParquetConnectorConfig.Topic] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void ParquetSourceConnector_ThrowsOnMissingTopic()
    {
        var connector = new ParquetSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ParquetConnectorConfig.FilePath] = "/path/to/file.parquet"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void ParquetSourceConnector_ProducesTaskConfigs()
    {
        var connector = new ParquetSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ParquetConnectorConfig.FilePath] = "/path/file1.parquet;/path/file2.parquet",
            [ParquetConnectorConfig.Topic] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(2);

        Assert.Equal(2, taskConfigs.Count);
    }

    [Fact]
    public void ParquetSinkConnector_HasCorrectConfig()
    {
        var connector = new ParquetSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(ParquetSinkTask), connector.TaskClass);
        Assert.NotNull(connector.Config);
    }

    [Fact]
    public void ParquetSinkConnector_ThrowsOnMissingTopics()
    {
        var connector = new ParquetSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ParquetConnectorConfig.OutputPath] = "/path/to/output"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void ParquetSinkConnector_ThrowsOnMissingOutputPath()
    {
        var connector = new ParquetSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ParquetConnectorConfig.Topics] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void ParquetSinkConnector_ThrowsOnInvalidCompressionCodec()
    {
        var connector = new ParquetSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ParquetConnectorConfig.Topics] = "test-topic",
            [ParquetConnectorConfig.OutputPath] = "/path/to/output",
            [ParquetConnectorConfig.CompressionCodec] = "invalid"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public async Task ParquetSourceTask_ReadsSimpleParquet()
    {
        // Create a test Parquet file
        var filePath = Path.Combine(_testDir, "test.parquet");
        await CreateTestParquetFileAsync(filePath);

        var task = new ParquetSourceTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ParquetConnectorConfig.FilePath] = filePath,
            [ParquetConnectorConfig.Topic] = "test-topic"
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.NotEmpty(records);
        Assert.Equal("test-topic", records[0].Topic);

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task ParquetSourceTask_ReturnsEmptyWhenFileDoesNotExist()
    {
        var task = new ParquetSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ParquetConnectorConfig.FilePath] = Path.Combine(_testDir, "nonexistent.parquet"),
            [ParquetConnectorConfig.Topic] = "test-topic"
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var records = await task.PollAsync(cts.Token);

        Assert.Empty(records);

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task ParquetSourceTask_AddsFileMetadataHeaders()
    {
        var filePath = Path.Combine(_testDir, "metadata-test.parquet");
        await CreateTestParquetFileAsync(filePath);

        var task = new ParquetSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ParquetConnectorConfig.FilePath] = filePath,
            [ParquetConnectorConfig.Topic] = "test-topic"
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.NotEmpty(records);
        Assert.NotNull(records[0].Headers);
        Assert.True(records[0].Headers!.ContainsKey("parquet.file"));
        Assert.True(records[0].Headers!.ContainsKey("parquet.row"));

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task ParquetSinkTask_WritesSimpleParquet()
    {
        var outputPath = Path.Combine(_testDir, "output.parquet");

        var task = new ParquetSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ParquetConnectorConfig.OutputPath] = outputPath,
            [ParquetConnectorConfig.OutputMode] = ParquetConnectorConfig.OutputModeOverwrite
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            CreateSinkRecord(new { id = 1, name = "Alice" }),
            CreateSinkRecord(new { id = 2, name = "Bob" })
        };

        await task.PutAsync(records, CancellationToken.None);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);

        task.Stop();
        task.Dispose();

        Assert.True(File.Exists(outputPath));
    }

    [Fact]
    public async Task ParquetSinkTask_WritesWithCompression()
    {
        var outputPath = Path.Combine(_testDir, "compressed.parquet");

        var task = new ParquetSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ParquetConnectorConfig.OutputPath] = outputPath,
            [ParquetConnectorConfig.OutputMode] = ParquetConnectorConfig.OutputModeOverwrite,
            [ParquetConnectorConfig.CompressionCodec] = ParquetConnectorConfig.CompressionSnappy
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            CreateSinkRecord(new { id = 1, value = "test data" }),
            CreateSinkRecord(new { id = 2, value = "more data" })
        };

        await task.PutAsync(records, CancellationToken.None);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);

        task.Stop();
        task.Dispose();

        Assert.True(File.Exists(outputPath));
    }

    [Fact]
    public async Task ParquetSinkTask_OverwritesExistingFile()
    {
        var outputPath = Path.Combine(_testDir, "overwrite.parquet");

        // First write
        var task1 = new ParquetSinkTask();
        var context1 = new TaskContext { RaiseError = _ => { } };
        task1.Initialize(context1);
        task1.Start(new Dictionary<string, string>
        {
            [ParquetConnectorConfig.OutputPath] = outputPath,
            [ParquetConnectorConfig.OutputMode] = ParquetConnectorConfig.OutputModeOverwrite
        });

        await task1.PutAsync([CreateSinkRecord(new { id = 1 })], CancellationToken.None);
        await task1.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
        task1.Stop();
        task1.Dispose();

        // Second write (overwrite)
        var task2 = new ParquetSinkTask();
        var context2 = new TaskContext { RaiseError = _ => { } };
        task2.Initialize(context2);
        task2.Start(new Dictionary<string, string>
        {
            [ParquetConnectorConfig.OutputPath] = outputPath,
            [ParquetConnectorConfig.OutputMode] = ParquetConnectorConfig.OutputModeOverwrite
        });

        await task2.PutAsync([CreateSinkRecord(new { id = 2 })], CancellationToken.None);
        await task2.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
        task2.Stop();
        task2.Dispose();

        // Read and verify only second record
        using var reader = await ParquetReader.CreateAsync(outputPath);
        using var groupReader = reader.OpenRowGroupReader(0);
        var columns = new List<DataColumn>();
        foreach (var field in reader.Schema.DataFields)
        {
            columns.Add(await groupReader.ReadColumnAsync(field));
        }

        Assert.Single(columns);
        Assert.Single(columns[0].Data);
    }

    [Fact]
    public async Task ParquetRoundTrip_PreservesData()
    {
        var filePath = Path.Combine(_testDir, "roundtrip.parquet");

        // Write
        var sinkTask = new ParquetSinkTask();
        var sinkContext = new TaskContext { RaiseError = _ => { } };
        sinkTask.Initialize(sinkContext);
        sinkTask.Start(new Dictionary<string, string>
        {
            [ParquetConnectorConfig.OutputPath] = filePath,
            [ParquetConnectorConfig.OutputMode] = ParquetConnectorConfig.OutputModeOverwrite
        });

        var originalData = new { id = 42L, name = "Test User", active = true };
        await sinkTask.PutAsync([CreateSinkRecord(originalData)], CancellationToken.None);
        await sinkTask.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
        sinkTask.Stop();
        sinkTask.Dispose();

        // Read
        var sourceTask = new ParquetSourceTask();
        var sourceContext = new TaskContext { RaiseError = _ => { } };
        sourceTask.Initialize(sourceContext);
        sourceTask.Start(new Dictionary<string, string>
        {
            [ParquetConnectorConfig.FilePath] = filePath,
            [ParquetConnectorConfig.Topic] = "roundtrip"
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await sourceTask.PollAsync(cts.Token);
        sourceTask.Stop();
        sourceTask.Dispose();

        Assert.Single(records);
        Assert.NotNull(records[0].Value);

        var json = Encoding.UTF8.GetString(records[0].Value!);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        // Values are stored as strings in this simplified implementation
        Assert.Equal("42", root.GetProperty("id").GetString());
        Assert.Equal("Test User", root.GetProperty("name").GetString());
    }

    [Fact]
    public async Task ParquetSinkTask_RollingModeKeepsRowsAcrossFlushes()
    {
        var outputDir = Path.Combine(_testDir, "rolling");

        var task = new ParquetSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);
        task.Start(new Dictionary<string, string>
        {
            [ParquetConnectorConfig.OutputPath] = outputDir,
            [ParquetConnectorConfig.OutputMode] = ParquetConnectorConfig.OutputModeRolling,
            [ParquetConnectorConfig.MaxRecordsPerFile] = "1000"
        });

        // The Connect runner flushes after every record — earlier flushes must survive
        await task.PutAsync([CreateSinkRecord(new { id = 1 })], CancellationToken.None);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
        await task.PutAsync([CreateSinkRecord(new { id = 2 })], CancellationToken.None);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);

        task.Stop();
        task.Dispose();

        var files = Directory.GetFiles(outputDir, "*.parquet");
        Assert.Single(files);
        Assert.Equal(2, await CountRowsAsync(files[0]));
    }

    [Fact]
    public async Task ParquetSinkTask_AppendModePreservesEarlierFlushes()
    {
        var outputPath = Path.Combine(_testDir, "append.parquet");

        var task = new ParquetSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);
        task.Start(new Dictionary<string, string>
        {
            [ParquetConnectorConfig.OutputPath] = outputPath,
            [ParquetConnectorConfig.OutputMode] = ParquetConnectorConfig.OutputModeAppend
        });

        await task.PutAsync([CreateSinkRecord(new { id = 1 })], CancellationToken.None);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
        await task.PutAsync([CreateSinkRecord(new { id = 2 })], CancellationToken.None);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);

        task.Stop();
        task.Dispose();

        Assert.Equal(2, await CountRowsAsync(outputPath));
    }

    [Fact]
    public async Task ParquetSinkTask_AppendToUnreadableFileThrows()
    {
        var outputPath = Path.Combine(_testDir, "corrupt.parquet");
        await File.WriteAllTextAsync(outputPath, "this is not a parquet file");

        var task = new ParquetSinkTask();
        var errors = new List<Exception>();
        var context = new TaskContext { RaiseError = errors.Add };
        task.Initialize(context);
        task.Start(new Dictionary<string, string>
        {
            [ParquetConnectorConfig.OutputPath] = outputPath,
            [ParquetConnectorConfig.OutputMode] = ParquetConnectorConfig.OutputModeAppend
        });

        await task.PutAsync([CreateSinkRecord(new { id = 1 })], CancellationToken.None);

        // An unreadable existing file must fail the flush loudly instead of
        // silently discarding all previously written rows
        await Assert.ThrowsAnyAsync<Exception>(() =>
            task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None));

        // Dispose surfaces the still-buffered rows via RaiseError but must not throw
        task.Dispose();
        Assert.NotEmpty(errors);
    }

    [Fact]
    public async Task ParquetSinkTask_SchemaIncludesFieldsFromAllBufferedRecords()
    {
        var outputPath = Path.Combine(_testDir, "union.parquet");

        var task = new ParquetSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);
        task.Start(new Dictionary<string, string>
        {
            [ParquetConnectorConfig.OutputPath] = outputPath,
            [ParquetConnectorConfig.OutputMode] = ParquetConnectorConfig.OutputModeOverwrite
        });

        await task.PutAsync(
        [
            CreateSinkRecord(new { id = 1 }),
            CreateSinkRecord(new { id = 2, name = "Bob" })
        ], CancellationToken.None);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);

        task.Stop();
        task.Dispose();

        using var reader = await ParquetReader.CreateAsync(outputPath);
        var fieldNames = reader.Schema.DataFields.Select(f => f.Name).ToArray();
        Assert.Contains("id", fieldNames);
        Assert.Contains("name", fieldNames);
    }

    [Fact]
    public async Task ParquetSourceTask_DeleteAfterReadDeletesOnlyAfterCommit()
    {
        var filePath = Path.Combine(_testDir, "delete-after-read.parquet");
        await CreateTestParquetFileAsync(filePath);

        var task = new ParquetSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);
        task.Start(new Dictionary<string, string>
        {
            [ParquetConnectorConfig.FilePath] = filePath,
            [ParquetConnectorConfig.Topic] = "test-topic",
            [ParquetConnectorConfig.DeleteAfterRead] = "true"
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.Equal(3, records.Count);

        // The file must survive until the records have been produced and committed
        Assert.True(File.Exists(filePath));

        await task.CommitAsync(cts.Token);

        Assert.False(File.Exists(filePath));

        task.Stop();
        task.Dispose();
    }

    private static async Task<long> CountRowsAsync(string filePath)
    {
        using var reader = await ParquetReader.CreateAsync(filePath);
        long rows = 0;
        for (var i = 0; i < reader.RowGroupCount; i++)
        {
            using var groupReader = reader.OpenRowGroupReader(i);
            rows += groupReader.RowCount;
        }
        return rows;
    }

    private static async Task CreateTestParquetFileAsync(string filePath)
    {
        var schema = new ParquetSchema(
            new DataField<long>("id"),
            new DataField<string>("name"));

        using var stream = File.Create(filePath);
        using var writer = await ParquetWriter.CreateAsync(schema, stream);
        using var groupWriter = writer.CreateRowGroup();

        await groupWriter.WriteColumnAsync(new DataColumn(
            schema.DataFields[0],
            TestIds));

        await groupWriter.WriteColumnAsync(new DataColumn(
            schema.DataFields[1],
            TestNames));
    }

    private static SinkRecord CreateSinkRecord(object data)
    {
        var json = JsonSerializer.Serialize(data);
        return new SinkRecord
        {
            Topic = "test-topic",
            Partition = 0,
            Offset = 0,
            Key = null,
            Value = Encoding.UTF8.GetBytes(json),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = null
        };
    }
}
