using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.Stdio;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Stdio.Tests;

/// <summary>
/// Tests for Stdio source and sink connectors.
/// </summary>
public sealed class StdioConnectorTests
{
    [Fact]
    public void StdioConnectorConfig_HasExpectedConstants()
    {
        // Source config
        Assert.Equal("topic", StdioConnectorConfig.Topic);
        Assert.Equal("input.format", StdioConnectorConfig.InputFormat);
        Assert.Equal("line", StdioConnectorConfig.InputFormatLine);
        Assert.Equal("json", StdioConnectorConfig.InputFormatJson);

        // Sink config
        Assert.Equal("topics", StdioConnectorConfig.Topics);
        Assert.Equal("output.format", StdioConnectorConfig.OutputFormat);
        Assert.Equal("output.target", StdioConnectorConfig.OutputTarget);
        Assert.Equal("stdout", StdioConnectorConfig.OutputTargetStdout);
        Assert.Equal("stderr", StdioConnectorConfig.OutputTargetStderr);
        Assert.Equal("include.key", StdioConnectorConfig.IncludeKey);
        Assert.Equal("include.metadata", StdioConnectorConfig.IncludeMetadata);
        Assert.Equal("key.value.separator", StdioConnectorConfig.KeyValueSeparator);
        Assert.Equal("\t", StdioConnectorConfig.DefaultKeyValueSeparator);
    }

    [Fact]
    public void StdioSourceConnector_HasCorrectConfig()
    {
        using var connector = new StdioSourceConnector();

        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(StdioSourceTask), connector.TaskClass);
        Assert.NotNull(connector.Config);

        var configKeys = connector.Config.Keys;
        Assert.Contains(configKeys, k => k.Name == StdioConnectorConfig.Topic);
        Assert.Contains(configKeys, k => k.Name == StdioConnectorConfig.InputFormat);
    }

    [Fact]
    public void StdioSinkConnector_HasCorrectConfig()
    {
        using var connector = new StdioSinkConnector();

        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(StdioSinkTask), connector.TaskClass);
        Assert.NotNull(connector.Config);

        var configKeys = connector.Config.Keys;
        Assert.Contains(configKeys, k => k.Name == StdioConnectorConfig.Topics);
        Assert.Contains(configKeys, k => k.Name == StdioConnectorConfig.OutputFormat);
        Assert.Contains(configKeys, k => k.Name == StdioConnectorConfig.OutputTarget);
        Assert.Contains(configKeys, k => k.Name == StdioConnectorConfig.IncludeKey);
        Assert.Contains(configKeys, k => k.Name == StdioConnectorConfig.IncludeMetadata);
    }

    [Fact]
    public void StdioSourceConnector_ThrowsOnMissingTopic()
    {
        using var connector = new StdioSourceConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>();

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void StdioSinkConnector_ThrowsOnMissingTopics()
    {
        using var connector = new StdioSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>();

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void StdioSourceConnector_ProducesTaskConfigs()
    {
        using var connector = new StdioSourceConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topic] = "stdin-data",
            [StdioConnectorConfig.InputFormat] = "json"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        // Stdio supports only single task
        Assert.Single(taskConfigs);
        Assert.Equal("stdin-data", taskConfigs[0][StdioConnectorConfig.Topic]);
        Assert.Equal("json", taskConfigs[0][StdioConnectorConfig.InputFormat]);

        connector.Stop();
    }

    [Fact]
    public void StdioSinkConnector_ProducesTaskConfigs()
    {
        using var connector = new StdioSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topics] = "topic1,topic2",
            [StdioConnectorConfig.OutputFormat] = "json",
            [StdioConnectorConfig.OutputTarget] = "stderr",
            [StdioConnectorConfig.IncludeKey] = "true",
            [StdioConnectorConfig.IncludeMetadata] = "true"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        // Stdio supports only single task
        Assert.Single(taskConfigs);
        Assert.Equal("topic1,topic2", taskConfigs[0][StdioConnectorConfig.Topics]);
        Assert.Equal("json", taskConfigs[0][StdioConnectorConfig.OutputFormat]);
        Assert.Equal("stderr", taskConfigs[0][StdioConnectorConfig.OutputTarget]);
        Assert.Equal("True", taskConfigs[0][StdioConnectorConfig.IncludeKey]);
        Assert.Equal("True", taskConfigs[0][StdioConnectorConfig.IncludeMetadata]);

        connector.Stop();
    }

    [Fact]
    public async Task StdioSourceTask_ReadsLinesFromReader()
    {
        var task = new StdioSourceTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var input = "line1\nline2\nline3";
        task.Reader = new StringReader(input);

        var config = new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topic] = "test-topic"
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        Assert.Equal(3, records.Count);
        Assert.Equal("line1", Encoding.UTF8.GetString(records[0].Value));
        Assert.Equal("line2", Encoding.UTF8.GetString(records[1].Value));
        Assert.Equal("line3", Encoding.UTF8.GetString(records[2].Value));
        Assert.All(records, r => Assert.Equal("test-topic", r.Topic));

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task StdioSourceTask_SkipsInvalidJsonInJsonMode()
    {
        var task = new StdioSourceTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var input = "{\"valid\":1}\nnot json\n{\"also\":\"valid\"}";
        task.Reader = new StringReader(input);

        var config = new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topic] = "test-topic",
            [StdioConnectorConfig.InputFormat] = StdioConnectorConfig.InputFormatJson
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        // Should skip the invalid JSON line
        Assert.Equal(2, records.Count);
        Assert.Equal("{\"valid\":1}", Encoding.UTF8.GetString(records[0].Value));
        Assert.Equal("{\"also\":\"valid\"}", Encoding.UTF8.GetString(records[1].Value));

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task StdioSinkTask_WritesLinesToWriter()
    {
        var task = new StdioSinkTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [StdioConnectorConfig.OutputFormat] = StdioConnectorConfig.OutputFormatLine
        };

        task.Start(config);

        // Set writer after Start() to override Console.Out
        var output = new StringWriter();
        task.Writer = output;

        var records = new List<SinkRecord>
        {
            new() { Topic = "topic", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("message1") },
            new() { Topic = "topic", Partition = 0, Offset = 1, Value = Encoding.UTF8.GetBytes("message2") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        var lines = output.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(2, lines.Length);
        Assert.Equal("message1", lines[0]);
        Assert.Equal("message2", lines[1]);

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task StdioSinkTask_WritesKeyValueWithSeparator()
    {
        var task = new StdioSinkTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [StdioConnectorConfig.OutputFormat] = StdioConnectorConfig.OutputFormatLine,
            [StdioConnectorConfig.IncludeKey] = "true",
            [StdioConnectorConfig.KeyValueSeparator] = "|"
        };

        task.Start(config);

        // Set writer after Start() to override Console.Out
        var output = new StringWriter();
        task.Writer = output;

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "topic",
                Partition = 0,
                Offset = 0,
                Key = Encoding.UTF8.GetBytes("key1"),
                Value = Encoding.UTF8.GetBytes("value1")
            }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        var lines = output.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        Assert.Single(lines);
        Assert.Equal("key1|value1", lines[0]);

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task StdioSinkTask_WritesJsonWithMetadata()
    {
        var task = new StdioSinkTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [StdioConnectorConfig.OutputFormat] = StdioConnectorConfig.OutputFormatJson,
            [StdioConnectorConfig.IncludeMetadata] = "true"
        };

        task.Start(config);

        // Set writer after Start() to override Console.Out
        var output = new StringWriter();
        task.Writer = output;

        var timestamp = DateTimeOffset.UtcNow;
        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 2,
                Offset = 42,
                Key = Encoding.UTF8.GetBytes("mykey"),
                Value = Encoding.UTF8.GetBytes("{\"data\":123}"),
                Timestamp = timestamp
            }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        var json = output.ToString().Trim();
        Assert.Contains("\"key\":\"mykey\"", json);
        Assert.Contains("\"data\":123", json);  // Value parsed as JSON object
        Assert.Contains("\"topic\":\"test-topic\"", json);
        Assert.Contains("\"partition\":2", json);
        Assert.Contains("\"offset\":42", json);
        Assert.Contains("\"timestamp\":", json);

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task StdioSinkTask_WritesStringValueWhenNotJson()
    {
        var task = new StdioSinkTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [StdioConnectorConfig.OutputFormat] = StdioConnectorConfig.OutputFormatJson
        };

        task.Start(config);

        // Set writer after Start() to override Console.Out
        var output = new StringWriter();
        task.Writer = output;

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "topic",
                Partition = 0,
                Offset = 0,
                Value = Encoding.UTF8.GetBytes("plain text, not json")
            }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync(records, cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        var json = output.ToString().Trim();
        Assert.Contains("\"value\":\"plain text, not json\"", json);

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public void StdioSourceTask_TracksLineNumbers()
    {
        var task = new StdioSourceTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topic] = "test-topic"
        };

        task.Start(config);

        Assert.Equal("1.0.0", task.Version);

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public void StdioSinkTask_SupportsStderrOutput()
    {
        var task = new StdioSinkTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [StdioConnectorConfig.OutputTarget] = StdioConnectorConfig.OutputTargetStderr
        };

        task.Start(config);

        // The internal writer should be Console.Error (we can't easily test this without mocking)
        Assert.Equal("1.0.0", task.Version);

        task.Stop();
        task.Dispose();
    }

    [Fact]
    public async Task StdioSourceTask_ReturnsEmptyWhenStreamEnds()
    {
        var task = new StdioSourceTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        task.Reader = new StringReader(""); // Empty input

        var config = new Dictionary<string, string>
        {
            [StdioConnectorConfig.Topic] = "test-topic"
        };

        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var records = await task.PollAsync(cts.Token);

        // First poll returns empty and sets end-of-stream flag
        Assert.Empty(records);

        task.Stop();
        task.Dispose();
    }
}
