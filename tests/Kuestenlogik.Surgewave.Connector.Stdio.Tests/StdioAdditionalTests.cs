using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.Stdio;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Stdio.Tests;

/// <summary>
/// Additional tests for Stdio source/sink connectors covering message framing,
/// format handling, edge cases, and connector/task lifecycle.
/// </summary>
public sealed class StdioAdditionalTests
{
    // ── StdioConnectorConfig defaults ──

    [Fact]
    public void Config_Defaults_AreCorrect()
    {
        Assert.Equal("line", StdioConnectorConfig.DefaultInputFormat);
        Assert.Equal("line", StdioConnectorConfig.DefaultOutputFormat);
        Assert.Equal("stdout", StdioConnectorConfig.DefaultOutputTarget);
        Assert.False(StdioConnectorConfig.DefaultIncludeKey);
        Assert.False(StdioConnectorConfig.DefaultIncludeMetadata);
        Assert.Equal("\t", StdioConnectorConfig.DefaultKeyValueSeparator);
        Assert.Equal("line_number", StdioConnectorConfig.OffsetLineNumber);
    }

    // ── Sink connector importance levels ──

    [Fact]
    public void SinkConnector_TopicsKey_HasHighImportance()
    {
        using var connector = new StdioSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == StdioConnectorConfig.Topics);
        Assert.Equal(Importance.High, key.Importance);
    }

    [Fact]
    public void SinkConnector_OutputFormatKey_HasMediumImportance()
    {
        using var connector = new StdioSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == StdioConnectorConfig.OutputFormat);
        Assert.Equal(Importance.Medium, key.Importance);
    }

    [Fact]
    public void SinkConnector_IncludeKeyKey_HasLowImportance()
    {
        using var connector = new StdioSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == StdioConnectorConfig.IncludeKey);
        Assert.Equal(Importance.Low, key.Importance);
    }

    // ── Source connector ──

    [Fact]
    public void SourceConnector_TopicKey_HasHighImportance()
    {
        using var connector = new StdioSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == StdioConnectorConfig.Topic);
        Assert.Equal(Importance.High, key.Importance);
    }

    [Fact]
    public void SourceConnector_InputFormatKey_HasMediumImportance()
    {
        using var connector = new StdioSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == StdioConnectorConfig.InputFormat);
        Assert.Equal(Importance.Medium, key.Importance);
    }

    [Fact]
    public void SourceConnector_Start_SetsDefaultFormat()
    {
        using var connector = new StdioSourceConnector();
        connector.Initialize(new ConnectorContext { RequestTaskReconfiguration = () => { }, RaiseError = _ => { } });
        connector.Start(new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topic] = "my-topic"
            // no input.format → should default to "line"
        });

        var taskConfig = connector.TaskConfigs(1)[0];
        Assert.Equal(StdioConnectorConfig.DefaultInputFormat, taskConfig[StdioConnectorConfig.InputFormat]);
    }

    [Fact]
    public void SinkConnector_Start_SetsDefaultFormatAndTarget()
    {
        using var connector = new StdioSinkConnector();
        connector.Initialize(new ConnectorContext { RequestTaskReconfiguration = () => { }, RaiseError = _ => { } });
        connector.Start(new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topics] = "topic1"
            // defaults applied
        });

        var taskConfig = connector.TaskConfigs(1)[0];
        Assert.Equal(StdioConnectorConfig.DefaultOutputFormat, taskConfig[StdioConnectorConfig.OutputFormat]);
        Assert.Equal(StdioConnectorConfig.DefaultOutputTarget, taskConfig[StdioConnectorConfig.OutputTarget]);
        Assert.Equal(StdioConnectorConfig.DefaultIncludeKey.ToString(), taskConfig[StdioConnectorConfig.IncludeKey]);
        Assert.Equal(StdioConnectorConfig.DefaultIncludeMetadata.ToString(), taskConfig[StdioConnectorConfig.IncludeMetadata]);
        Assert.Equal(StdioConnectorConfig.DefaultKeyValueSeparator, taskConfig[StdioConnectorConfig.KeyValueSeparator]);
    }

    // ── Sink task output formats ──

    [Fact]
    public async Task SinkTask_LineFormat_NullValue_WritesEmpty()
    {
        var task = CreateStartedSinkTask(new Dictionary<string, string>
        {
            [StdioConnectorConfig.OutputFormat] = StdioConnectorConfig.OutputFormatLine
        });

        var output = new StringWriter();
        task.Writer = output;

        await task.PutAsync([new SinkRecord
        {
            Topic = "t", Partition = 0, Offset = 0,
            Value = null!   // null value
        }], CancellationToken.None);

        var text = output.ToString();
        // null value should produce an empty line (just the newline separator)
        Assert.Contains(Environment.NewLine, text);
        var lineContent = text.Split(Environment.NewLine)[0];
        Assert.Equal("", lineContent);

        task.Dispose();
    }

    [Fact]
    public async Task SinkTask_LineFormat_NoKey_WritesValueOnly()
    {
        var task = CreateStartedSinkTask(new Dictionary<string, string>
        {
            [StdioConnectorConfig.OutputFormat] = StdioConnectorConfig.OutputFormatLine,
            [StdioConnectorConfig.IncludeKey] = "true"
        });

        var output = new StringWriter();
        task.Writer = output;

        await task.PutAsync([new SinkRecord
        {
            Topic = "t", Partition = 0, Offset = 0,
            Key = null,  // no key
            Value = Encoding.UTF8.GetBytes("hello")
        }], CancellationToken.None);

        var lines = output.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        Assert.Single(lines);
        Assert.Equal("hello", lines[0]);

        task.Dispose();
    }

    [Fact]
    public async Task SinkTask_LineFormat_DefaultTabSeparator()
    {
        var task = CreateStartedSinkTask(new Dictionary<string, string>
        {
            [StdioConnectorConfig.OutputFormat] = StdioConnectorConfig.OutputFormatLine,
            [StdioConnectorConfig.IncludeKey] = "true"
            // no separator → default tab
        });

        var output = new StringWriter();
        task.Writer = output;

        await task.PutAsync([new SinkRecord
        {
            Topic = "t", Partition = 0, Offset = 0,
            Key = Encoding.UTF8.GetBytes("k"),
            Value = Encoding.UTF8.GetBytes("v")
        }], CancellationToken.None);

        var text = output.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries).First();
        Assert.Equal("k\tv", text);

        task.Dispose();
    }

    [Fact]
    public async Task SinkTask_JsonFormat_NoMetadata_DoesNotIncludeTopicInfo()
    {
        var task = CreateStartedSinkTask(new Dictionary<string, string>
        {
            [StdioConnectorConfig.OutputFormat] = StdioConnectorConfig.OutputFormatJson,
            [StdioConnectorConfig.IncludeMetadata] = "false"
        });

        var output = new StringWriter();
        task.Writer = output;

        await task.PutAsync([new SinkRecord
        {
            Topic = "my-topic", Partition = 0, Offset = 0,
            Value = Encoding.UTF8.GetBytes("some text")
        }], CancellationToken.None);

        var json = output.ToString().Trim();
        Assert.DoesNotContain("\"topic\"", json);
        Assert.DoesNotContain("\"partition\"", json);
        Assert.DoesNotContain("\"offset\"", json);

        task.Dispose();
    }

    [Fact]
    public async Task SinkTask_JsonFormat_NoKey_KeyNotIncluded()
    {
        var task = CreateStartedSinkTask(new Dictionary<string, string>
        {
            [StdioConnectorConfig.OutputFormat] = StdioConnectorConfig.OutputFormatJson
        });

        var output = new StringWriter();
        task.Writer = output;

        await task.PutAsync([new SinkRecord
        {
            Topic = "t", Partition = 0, Offset = 0,
            Key = null,
            Value = Encoding.UTF8.GetBytes("{\"x\":1}")
        }], CancellationToken.None);

        var json = output.ToString().Trim();
        Assert.DoesNotContain("\"key\"", json);
        Assert.Contains("\"x\":1", json);

        task.Dispose();
    }

    [Fact]
    public async Task SinkTask_JsonFormat_TimestampNotDefaultIncluded()
    {
        var task = CreateStartedSinkTask(new Dictionary<string, string>
        {
            [StdioConnectorConfig.OutputFormat] = StdioConnectorConfig.OutputFormatJson,
            [StdioConnectorConfig.IncludeMetadata] = "true"
        });

        var output = new StringWriter();
        task.Writer = output;

        var ts = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await task.PutAsync([new SinkRecord
        {
            Topic = "t", Partition = 1, Offset = 99,
            Value = Encoding.UTF8.GetBytes("{}"),
            Timestamp = ts
        }], CancellationToken.None);

        var json = output.ToString().Trim();
        Assert.Contains("\"timestamp\":", json);
        Assert.Contains("2025", json);

        task.Dispose();
    }

    [Fact]
    public async Task SinkTask_MultipleRecords_AllWritten()
    {
        var task = CreateStartedSinkTask(new Dictionary<string, string>
        {
            [StdioConnectorConfig.OutputFormat] = StdioConnectorConfig.OutputFormatLine
        });

        var output = new StringWriter();
        task.Writer = output;

        var records = Enumerable.Range(1, 5).Select(i => new SinkRecord
        {
            Topic = "t", Partition = 0, Offset = i,
            Value = Encoding.UTF8.GetBytes($"line {i}")
        }).ToList();

        await task.PutAsync(records, CancellationToken.None);

        var lines = output.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(5, lines.Length);
        for (var i = 1; i <= 5; i++)
        {
            Assert.Equal($"line {i}", lines[i - 1]);
        }

        task.Dispose();
    }

    [Fact]
    public async Task SinkTask_Flush_DoesNotThrow()
    {
        var task = CreateStartedSinkTask(new Dictionary<string, string>
        {
            [StdioConnectorConfig.OutputFormat] = StdioConnectorConfig.OutputFormatLine
        });

        var output = new StringWriter();
        task.Writer = output;

        // Flush with no records written should not throw
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);

        task.Dispose();
    }

    // ── Source task line number tracking ──

    [Fact]
    public async Task SourceTask_LineNumbers_IncreasePerLine()
    {
        var task = new StdioSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Reader = new StringReader("a\nb\nc");
        task.Start(new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topic] = "lines"
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.Equal(3, records.Count);

        // Each record should carry a line_number offset
        for (var i = 0; i < records.Count; i++)
        {
            Assert.True(records[i].SourceOffset.ContainsKey(StdioConnectorConfig.OffsetLineNumber));
            var lineNum = Convert.ToInt64(records[i].SourceOffset[StdioConnectorConfig.OffsetLineNumber]);
            Assert.Equal(i + 1, (int)lineNum);
        }

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task SourceTask_SourcePartition_IsAlwaysStdin()
    {
        var task = new StdioSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Reader = new StringReader("hello");
        task.Start(new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topic] = "test"
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.Single(records);
        Assert.True(records[0].SourcePartition.ContainsKey("source"));
        Assert.Equal("stdin", records[0].SourcePartition["source"]);

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task SourceTask_JsonMode_AllValidJsonPasses()
    {
        var task = new StdioSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Reader = new StringReader("{}\n[]\n\"string\"\n42\ntrue");
        task.Start(new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topic] = "test",
            [StdioConnectorConfig.InputFormat] = StdioConnectorConfig.InputFormatJson
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        // All 5 are valid JSON
        Assert.Equal(5, records.Count);

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task SourceTask_JsonMode_MultipleInvalidLines_AllSkipped()
    {
        var task = new StdioSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Reader = new StringReader("bad\nalso bad\n{\"ok\":true}");
        task.Start(new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topic] = "test",
            [StdioConnectorConfig.InputFormat] = StdioConnectorConfig.InputFormatJson
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        // Only the valid JSON line survives
        Assert.Single(records);
        Assert.Equal("{\"ok\":true}", Encoding.UTF8.GetString(records[0].Value!));

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task SourceTask_LineMode_PreservesWhitespace()
    {
        var task = new StdioSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Reader = new StringReader("  spaces  \n\t\ttabs");
        task.Start(new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topic] = "test"
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.Equal(2, records.Count);
        Assert.Equal("  spaces  ", Encoding.UTF8.GetString(records[0].Value!));
        Assert.Equal("\t\ttabs", Encoding.UTF8.GetString(records[1].Value!));

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task SourceTask_TimestampIsSet()
    {
        var task = new StdioSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Reader = new StringReader("line");
        task.Start(new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topic] = "ts-test"
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var before = DateTimeOffset.UtcNow;
        var records = await task.PollAsync(cts.Token);
        var after = DateTimeOffset.UtcNow;

        Assert.Single(records);
        Assert.True(records[0].Timestamp >= before && records[0].Timestamp <= after);

        task.Stop();
        task.Dispose();
    }

    // ── Helper ──

    private static StdioSinkTask CreateStartedSinkTask(IDictionary<string, string> config)
    {
        var task = new StdioSinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);
        return task;
    }
}
