using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Csv;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Csv.Tests;

/// <summary>
/// Tests for CSV source and sink connectors.
/// </summary>
public sealed class CsvConnectorTests : IDisposable
{
    private readonly string _testDir;

    public CsvConnectorTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"csv-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, recursive: true);
    }

    [Fact]
    public void CsvConnectorConfig_HasExpectedConstants()
    {
        Assert.Equal("file.path", CsvConnectorConfig.FilePath);
        Assert.Equal("topic", CsvConnectorConfig.Topic);
        Assert.Equal("delimiter", CsvConnectorConfig.Delimiter);
        Assert.Equal("has.header", CsvConnectorConfig.HasHeader);
        Assert.Equal(",", CsvConnectorConfig.DefaultDelimiter);
        Assert.True(CsvConnectorConfig.DefaultHasHeader);
    }

    [Fact]
    public void CsvSourceConnector_HasCorrectConfig()
    {
        using var connector = new CsvSourceConnector();

        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(CsvSourceTask), connector.TaskClass);
        Assert.NotNull(connector.Config);

        var configKeys = connector.Config.Keys;
        Assert.Contains(configKeys, k => k.Name == CsvConnectorConfig.FilePath);
        Assert.Contains(configKeys, k => k.Name == CsvConnectorConfig.Topic);
        Assert.Contains(configKeys, k => k.Name == CsvConnectorConfig.Delimiter);
    }

    [Fact]
    public void CsvSinkConnector_HasCorrectConfig()
    {
        using var connector = new CsvSinkConnector();

        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(CsvSinkTask), connector.TaskClass);
        Assert.NotNull(connector.Config);

        var configKeys = connector.Config.Keys;
        Assert.Contains(configKeys, k => k.Name == CsvConnectorConfig.Topics);
        Assert.Contains(configKeys, k => k.Name == CsvConnectorConfig.OutputPath);
        Assert.Contains(configKeys, k => k.Name == CsvConnectorConfig.OutputMode);
    }

    [Fact]
    public void CsvSourceConnector_ThrowsOnMissingFilePath()
    {
        using var connector = new CsvSourceConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.Topic] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void CsvSourceConnector_ThrowsOnMissingTopic()
    {
        using var connector = new CsvSourceConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.FilePath] = "/path/to/file.csv"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void CsvSinkConnector_ThrowsOnMissingTopics()
    {
        using var connector = new CsvSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.OutputPath] = "/path/to/output.csv"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void CsvSinkConnector_ThrowsOnMissingOutputPath()
    {
        using var connector = new CsvSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.Topics] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public async Task CsvSourceTask_ReadsSimpleCsv()
    {
        var csvPath = Path.Combine(_testDir, "simple.csv");
        await File.WriteAllTextAsync(csvPath, "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago");

        var task = new CsvSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.FilePath] = csvPath,
            [CsvConnectorConfig.Topic] = "test-topic"
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.Equal(3, records.Count);

        var first = JsonSerializer.Deserialize<Dictionary<string, object?>>(records[0].Value!);
        Assert.NotNull(first);
        Assert.Equal("Alice", first["name"]?.ToString());
        Assert.Equal("30", first["age"]?.ToString());
        Assert.Equal("NYC", first["city"]?.ToString());

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task CsvSourceTask_ReadsWithCustomDelimiter()
    {
        var csvPath = Path.Combine(_testDir, "semicolon.csv");
        await File.WriteAllTextAsync(csvPath, "name;age;city\nAlice;30;NYC\nBob;25;LA");

        var task = new CsvSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.FilePath] = csvPath,
            [CsvConnectorConfig.Topic] = "test-topic",
            [CsvConnectorConfig.Delimiter] = ";"
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.Equal(2, records.Count);

        var first = JsonSerializer.Deserialize<Dictionary<string, object?>>(records[0].Value!);
        Assert.NotNull(first);
        Assert.Equal("Alice", first["name"]?.ToString());

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task CsvSourceTask_ReadsWithoutHeader()
    {
        var csvPath = Path.Combine(_testDir, "noheader.csv");
        await File.WriteAllTextAsync(csvPath, "Alice,30,NYC\nBob,25,LA");

        var task = new CsvSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.FilePath] = csvPath,
            [CsvConnectorConfig.Topic] = "test-topic",
            [CsvConnectorConfig.HasHeader] = "false"
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.Equal(2, records.Count);

        var first = JsonSerializer.Deserialize<Dictionary<string, object?>>(records[0].Value!);
        Assert.NotNull(first);
        Assert.Equal("Alice", first["field_0"]?.ToString());
        Assert.Equal("30", first["field_1"]?.ToString());
        Assert.Equal("NYC", first["field_2"]?.ToString());

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task CsvSourceTask_ReadsWithKeyField()
    {
        var csvPath = Path.Combine(_testDir, "keyed.csv");
        await File.WriteAllTextAsync(csvPath, "id,name,value\n1,Alice,100\n2,Bob,200");

        var task = new CsvSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.FilePath] = csvPath,
            [CsvConnectorConfig.Topic] = "test-topic",
            [CsvConnectorConfig.KeyField] = "id"
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.Equal(2, records.Count);
        Assert.NotNull(records[0].Key);
        Assert.Equal("1", Encoding.UTF8.GetString(records[0].Key!));
        Assert.Equal("2", Encoding.UTF8.GetString(records[1].Key!));

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task CsvSourceTask_ReadsQuotedFields()
    {
        var csvPath = Path.Combine(_testDir, "quoted.csv");
        await File.WriteAllTextAsync(csvPath, "name,description\nAlice,\"Hello, World\"\nBob,\"Line 1\nLine 2\"");

        var task = new CsvSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.FilePath] = csvPath,
            [CsvConnectorConfig.Topic] = "test-topic"
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.Equal(2, records.Count);

        var first = JsonSerializer.Deserialize<Dictionary<string, object?>>(records[0].Value!);
        Assert.NotNull(first);
        Assert.Equal("Hello, World", first["description"]?.ToString());

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task CsvSourceTask_AddsFileMetadataHeaders()
    {
        var csvPath = Path.Combine(_testDir, "metadata.csv");
        await File.WriteAllTextAsync(csvPath, "name\nAlice");

        var task = new CsvSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.FilePath] = csvPath,
            [CsvConnectorConfig.Topic] = "test-topic"
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.Single(records);
        Assert.NotNull(records[0].Headers);
        Assert.True(records[0].Headers!.ContainsKey("csv.file"));
        Assert.True(records[0].Headers!.ContainsKey("csv.line"));

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task CsvSinkTask_WritesSimpleCsv()
    {
        var outputPath = Path.Combine(_testDir, "output.csv");

        var task = new CsvSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.OutputPath] = outputPath
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("{\"name\":\"Alice\",\"age\":30}") },
            new() { Topic = "test", Partition = 0, Offset = 1, Value = Encoding.UTF8.GetBytes("{\"name\":\"Bob\",\"age\":25}") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        task.Stop();
        task.Dispose();

        Assert.True(File.Exists(outputPath));
        var content = await File.ReadAllTextAsync(outputPath);
        Assert.Contains("name", content);
        Assert.Contains("age", content);
        Assert.Contains("Alice", content);
        Assert.Contains("Bob", content);
    }

    [Fact]
    public async Task CsvSinkTask_WritesWithCustomDelimiter()
    {
        var outputPath = Path.Combine(_testDir, "semicolon-output.csv");

        var task = new CsvSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.OutputPath] = outputPath,
            [CsvConnectorConfig.Delimiter] = ";"
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("{\"name\":\"Alice\",\"value\":100}") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        task.Stop();
        task.Dispose();

        var content = await File.ReadAllTextAsync(outputPath);
        Assert.Contains(";", content);
        Assert.DoesNotContain(",", content.Replace("Alice", "").Replace("100", ""));
    }

    [Fact]
    public async Task CsvSinkTask_WritesWithoutHeader()
    {
        var outputPath = Path.Combine(_testDir, "no-header-output.csv");

        var task = new CsvSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.OutputPath] = outputPath,
            [CsvConnectorConfig.IncludeHeader] = "false"
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("{\"name\":\"Alice\",\"age\":30}") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        task.Stop();
        task.Dispose();

        var lines = await File.ReadAllLinesAsync(outputPath);
        Assert.Single(lines);
        Assert.DoesNotContain("name", lines[0]);
    }

    [Fact]
    public async Task CsvSinkTask_AppendsToExistingFile()
    {
        var outputPath = Path.Combine(_testDir, "append.csv");
        await File.WriteAllTextAsync(outputPath, "name,age\nExisting,99\n");

        var task = new CsvSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.OutputPath] = outputPath,
            [CsvConnectorConfig.OutputMode] = CsvConnectorConfig.OutputModeAppend
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("{\"name\":\"New\",\"age\":1}") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        task.Stop();
        task.Dispose();

        var content = await File.ReadAllTextAsync(outputPath);
        Assert.Contains("Existing", content);
        Assert.Contains("New", content);
    }

    [Fact]
    public async Task CsvSinkTask_OverwritesExistingFile()
    {
        var outputPath = Path.Combine(_testDir, "overwrite.csv");
        await File.WriteAllTextAsync(outputPath, "name,age\nOld,99\n");

        var task = new CsvSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.OutputPath] = outputPath,
            [CsvConnectorConfig.OutputMode] = CsvConnectorConfig.OutputModeOverwrite
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("{\"name\":\"New\",\"age\":1}") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        task.Stop();
        task.Dispose();

        var content = await File.ReadAllTextAsync(outputPath);
        Assert.DoesNotContain("Old", content);
        Assert.Contains("New", content);
    }

    [Fact]
    public void CsvSourceConnector_ProducesTaskConfigs()
    {
        var csvPath = Path.Combine(_testDir, "taskconfig.csv");
        File.WriteAllText(csvPath, "test");

        using var connector = new CsvSourceConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.FilePath] = csvPath,
            [CsvConnectorConfig.Topic] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal(csvPath, taskConfigs[0][CsvConnectorConfig.FilePath]);
        Assert.Equal("test-topic", taskConfigs[0][CsvConnectorConfig.Topic]);

        connector.Stop();
    }

    [Fact]
    public void CsvSinkConnector_ProducesTaskConfigs()
    {
        using var connector = new CsvSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.Topics] = "topic1,topic2",
            [CsvConnectorConfig.OutputPath] = "/path/to/output.csv"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);

        connector.Stop();
    }

    [Fact]
    public async Task CsvSourceTask_ReturnsEmptyWhenFileDoesNotExist()
    {
        var task = new CsvSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.FilePath] = "/nonexistent/file.csv",
            [CsvConnectorConfig.Topic] = "test-topic"
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var records = await task.PollAsync(cts.Token);

        Assert.Empty(records);

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task CsvSourceTask_ReadsEmptyFile()
    {
        var csvPath = Path.Combine(_testDir, "empty.csv");
        await File.WriteAllTextAsync(csvPath, "name,age\n");

        var task = new CsvSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [CsvConnectorConfig.FilePath] = csvPath,
            [CsvConnectorConfig.Topic] = "test-topic"
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var records = await task.PollAsync(cts.Token);

        Assert.Empty(records);

        task.Stop();
        task.Dispose();
    }
}
