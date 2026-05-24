using System.Text;
using Xunit;
using Kuestenlogik.Surgewave.Connector.Sequence;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sequence.Tests;

public class SequenceSourceTaskTests
{
    private static string GetTestSourceTaskTypeName() =>
        typeof(TestSourceTask).AssemblyQualifiedName!;

    private static string GetErrorSourceTaskTypeName() =>
        typeof(ErrorSourceTask).AssemblyQualifiedName!;

    private static string GetInfiniteSourceTaskTypeName() =>
        typeof(InfiniteSourceTask).AssemblyQualifiedName!;

    [Fact]
    public void Version_ReturnsExpectedVersion()
    {
        using var task = new SequenceSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithEmptySources_InitializesSuccessfully()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = "[]",
            [SequenceConnectorConfig.TopicConfig] = "test-topic"
        };

        task.Start(config);

        Assert.Equal(0, task.CurrentSourceIndex);
        Assert.Equal(0, task.SourceCount);
        Assert.False(task.AllSourcesCompleted);

        task.Stop();
    }

    [Fact]
    public void Start_WithSingleSource_InitializesCorrectly()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[{{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""5""}}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic"
        };

        task.Start(config);

        Assert.Equal(0, task.CurrentSourceIndex);
        Assert.Equal(1, task.SourceCount);
        Assert.False(task.AllSourcesCompleted);

        task.Stop();
    }

    [Fact]
    public void Start_WithMultipleSources_InitializesCorrectly()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[
            {{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""3""}},
            {{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""5""}}
        ]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic"
        };

        task.Start(config);

        Assert.Equal(0, task.CurrentSourceIndex);
        Assert.Equal(2, task.SourceCount);
        Assert.False(task.AllSourcesCompleted);

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_WithEmptySources_ReturnsEmpty()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = "[]",
            [SequenceConnectorConfig.TopicConfig] = "test-topic"
        };

        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);
        Assert.Empty(records);

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_WithSingleSource_ProducesRecords()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[{{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""3""}}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic",
            [SequenceConnectorConfig.EmptyPollsBeforeAdvanceConfig] = "1"
        };

        task.Start(config);

        // Should get 3 records
        var allRecords = new List<SourceRecord>();
        for (int i = 0; i < 3; i++)
        {
            var records = await task.PollAsync(CancellationToken.None);
            allRecords.AddRange(records);
        }

        Assert.Equal(3, allRecords.Count);

        // All records should have the output topic
        Assert.All(allRecords, r => Assert.Equal("output-topic", r.Topic));

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_TransformsTopicCorrectly()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[{{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""1""}}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "transformed-topic"
        };

        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Single(records);
        Assert.Equal("transformed-topic", records[0].Topic);

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_AddsSourceIndexHeader()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[{{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""1""}}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic",
            [SequenceConnectorConfig.IncludeSourceIndexConfig] = "true",
            [SequenceConnectorConfig.SourceIndexHeaderConfig] = "my-source-index"
        };

        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Single(records);
        Assert.NotNull(records[0].Headers);
        Assert.True(records[0].Headers!.ContainsKey("my-source-index"));
        Assert.Equal("0", Encoding.UTF8.GetString(records[0].Headers!["my-source-index"]));

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_WithIncludeSourceIndexFalse_DoesNotAddHeader()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[{{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""1""}}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic",
            [SequenceConnectorConfig.IncludeSourceIndexConfig] = "false"
        };

        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Single(records);
        // Headers should be null or not contain the source index
        if (records[0].Headers != null)
        {
            Assert.False(records[0].Headers!.ContainsKey(SequenceConnectorConfig.DefaultSourceIndexHeader));
        }

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_AdvancesToNextSourceAfterEmptyPolls()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[
            {{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""2""}},
            {{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""2""}}
        ]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic",
            [SequenceConnectorConfig.EmptyPollsBeforeAdvanceConfig] = "1",
            [SequenceConnectorConfig.EmptyPollDelayMsConfig] = "0"
        };

        task.Start(config);

        var advanceEvents = new List<(int from, int to)>();
        task.OnSourceAdvance += (from, to) => advanceEvents.Add((from, to));

        // Poll first source - 2 records
        Assert.Equal(0, task.CurrentSourceIndex);
        var r1 = await task.PollAsync(CancellationToken.None);
        Assert.Single(r1);
        var r2 = await task.PollAsync(CancellationToken.None);
        Assert.Single(r2);

        // Now source is empty, 1 empty poll should trigger advance
        var empty = await task.PollAsync(CancellationToken.None);
        Assert.Empty(empty);

        // Should have advanced to second source
        Assert.Equal(1, task.CurrentSourceIndex);
        Assert.Single(advanceEvents);
        Assert.Equal(0, advanceEvents[0].from);
        Assert.Equal(1, advanceEvents[0].to);

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_CompletesAfterAllSources()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[{{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""1""}}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic",
            [SequenceConnectorConfig.EmptyPollsBeforeAdvanceConfig] = "1",
            [SequenceConnectorConfig.EmptyPollDelayMsConfig] = "0",
            [SequenceConnectorConfig.CompletionBehaviorConfig] = SequenceConnectorConfig.CompletionBehaviorStop
        };

        task.Start(config);

        var completedEventFired = false;
        task.OnAllSourcesCompleted += () => completedEventFired = true;

        // Get the one record
        var r1 = await task.PollAsync(CancellationToken.None);
        Assert.Single(r1);
        Assert.False(task.AllSourcesCompleted);

        // Empty poll triggers completion
        var empty = await task.PollAsync(CancellationToken.None);
        Assert.Empty(empty);

        Assert.True(task.AllSourcesCompleted);
        Assert.True(completedEventFired);

        // Further polls return empty
        var r2 = await task.PollAsync(CancellationToken.None);
        Assert.Empty(r2);

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_WithRestartBehavior_RestartsSequence()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[{{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""1""}}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic",
            [SequenceConnectorConfig.EmptyPollsBeforeAdvanceConfig] = "1",
            [SequenceConnectorConfig.EmptyPollDelayMsConfig] = "0",
            [SequenceConnectorConfig.CompletionBehaviorConfig] = SequenceConnectorConfig.CompletionBehaviorRestart
        };

        task.Start(config);

        // Get the one record
        var r1 = await task.PollAsync(CancellationToken.None);
        Assert.Single(r1);

        // Empty poll triggers restart
        var empty = await task.PollAsync(CancellationToken.None);
        Assert.Empty(empty);

        Assert.False(task.AllSourcesCompleted);
        Assert.Equal(0, task.CurrentSourceIndex);

        // Should be able to get another record (source restarted)
        var r2 = await task.PollAsync(CancellationToken.None);
        Assert.Single(r2);

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_WithContinueOnError_AdvancesOnError()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[
            {{""task.class"": ""{GetErrorSourceTaskTypeName()}"", ""error.after"": ""2""}},
            {{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""1""}}
        ]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic",
            [SequenceConnectorConfig.ContinueOnErrorConfig] = "true"
        };

        task.Start(config);

        // Get 2 records before error
        var r1 = await task.PollAsync(CancellationToken.None);
        Assert.Single(r1);
        var r2 = await task.PollAsync(CancellationToken.None);
        Assert.Single(r2);

        Assert.Equal(0, task.CurrentSourceIndex);

        // Next poll triggers error, which advances to next source
        var r3 = await task.PollAsync(CancellationToken.None);
        Assert.Empty(r3); // Error returns empty and advances

        Assert.Equal(1, task.CurrentSourceIndex);

        // Should now get record from second source
        var r4 = await task.PollAsync(CancellationToken.None);
        Assert.Single(r4);

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_WithoutContinueOnError_PropagatesError()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[{{""task.class"": ""{GetErrorSourceTaskTypeName()}"", ""error.after"": ""1""}}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic",
            [SequenceConnectorConfig.ContinueOnErrorConfig] = "false"
        };

        task.Start(config);

        // Get 1 record before error
        var r1 = await task.PollAsync(CancellationToken.None);
        Assert.Single(r1);

        // Next poll should throw
        await Assert.ThrowsAsync<InvalidOperationException>(() => task.PollAsync(CancellationToken.None));

        task.Stop();
    }

    [Fact]
    public void Reset_RestartsSequence()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[
            {{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""1""}},
            {{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""1""}}
        ]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic"
        };

        task.Start(config);

        // Manually advance
        task.AdvanceToNextSource();
        Assert.Equal(1, task.CurrentSourceIndex);

        // Reset
        task.Reset();
        Assert.Equal(0, task.CurrentSourceIndex);
        Assert.False(task.AllSourcesCompleted);

        task.Stop();
    }

    [Fact]
    public void AdvanceToNextSource_ManuallyAdvances()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[
            {{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""10""}},
            {{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""10""}},
            {{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""10""}}
        ]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic"
        };

        task.Start(config);

        Assert.Equal(0, task.CurrentSourceIndex);

        task.AdvanceToNextSource();
        Assert.Equal(1, task.CurrentSourceIndex);

        task.AdvanceToNextSource();
        Assert.Equal(2, task.CurrentSourceIndex);

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_WithMultipleEmptyPollsRequired_WaitsCorrectly()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[
            {{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""1""}},
            {{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""1""}}
        ]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic",
            [SequenceConnectorConfig.EmptyPollsBeforeAdvanceConfig] = "3",
            [SequenceConnectorConfig.EmptyPollDelayMsConfig] = "0"
        };

        task.Start(config);

        // Get the one record from first source
        var r1 = await task.PollAsync(CancellationToken.None);
        Assert.Single(r1);
        Assert.Equal(0, task.CurrentSourceIndex);

        // First empty poll - should not advance yet
        await task.PollAsync(CancellationToken.None);
        Assert.Equal(0, task.CurrentSourceIndex);

        // Second empty poll - should not advance yet
        await task.PollAsync(CancellationToken.None);
        Assert.Equal(0, task.CurrentSourceIndex);

        // Third empty poll - should advance now
        await task.PollAsync(CancellationToken.None);
        Assert.Equal(1, task.CurrentSourceIndex);

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_RecordResetsEmptyPollCounter()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        // Use infinite source for this test
        var sources = $@"[{{""task.class"": ""{GetInfiniteSourceTaskTypeName()}""}}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic",
            [SequenceConnectorConfig.EmptyPollsBeforeAdvanceConfig] = "3"
        };

        task.Start(config);

        // Get several records - should always stay on source 0
        for (int i = 0; i < 10; i++)
        {
            var records = await task.PollAsync(CancellationToken.None);
            Assert.Single(records);
            Assert.Equal(0, task.CurrentSourceIndex);
        }

        task.Stop();
    }

    [Fact]
    public void Start_WithInvalidJson_HandlesGracefully()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = "not valid json",
            [SequenceConnectorConfig.TopicConfig] = "output-topic"
        };

        task.Start(config);

        // Should have 0 sources due to parse error
        Assert.Equal(0, task.SourceCount);

        task.Stop();
    }

    [Fact]
    public void Start_WithInvalidTaskClass_HandlesGracefully()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = @"[{""task.class"": ""NonExistent.Class.That.DoesNotExist"", ""key"": ""value""}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic"
        };

        task.Start(config);

        Assert.Equal(1, task.SourceCount);

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_SourcePartitionIncludesSequenceIndex()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[{{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""1""}}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic"
        };

        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Single(records);
        Assert.True(records[0].SourcePartition.ContainsKey("sequence.source.index"));
        Assert.Equal(0, records[0].SourcePartition["sequence.source.index"]);

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_SourceOffsetIncludesSequenceIndex()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[{{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""1""}}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic"
        };

        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Single(records);
        Assert.True(records[0].SourceOffset.ContainsKey("sequence.source.index"));
        Assert.Equal(0, records[0].SourceOffset["sequence.source.index"]);

        task.Stop();
    }

    [Fact]
    public void Stop_ClearsState()
    {
        using var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[{{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""10""}}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic"
        };

        task.Start(config);
        task.AdvanceToNextSource();

        task.Stop();

        Assert.Equal(0, task.CurrentSourceIndex);
        Assert.Equal(0, task.SourceCount);
        Assert.False(task.AllSourcesCompleted);
    }

    [Fact]
    public void Dispose_CleansUpResources()
    {
        var task = new SequenceSourceTask();
        var context = new TaskContext();
        task.Initialize(context);

        var sources = $@"[{{""task.class"": ""{GetTestSourceTaskTypeName()}"", ""record.count"": ""10""}}]";
        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = sources,
            [SequenceConnectorConfig.TopicConfig] = "output-topic"
        };

        task.Start(config);

        // Should not throw
        task.Dispose();
    }
}
