using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connector.Generator;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Generator.Tests;

public class GeneratorSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new GeneratorSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PollAsync_GeneratesRecords()
    {
        using var task = new GeneratorSourceTask();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.IntervalMs] = "0",
            [GeneratorConnectorConfig.BatchSize] = "5"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(5, records.Count);
        Assert.All(records, r => Assert.Equal("test-topic", r.Topic));
    }

    [Fact]
    public async Task PollAsync_WithMessageLimit_StopsAfterLimit()
    {
        using var task = new GeneratorSourceTask();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.IntervalMs] = "0",
            [GeneratorConnectorConfig.MessageCount] = "3",
            [GeneratorConnectorConfig.BatchSize] = "2"
        };
        task.Start(config);

        // First batch - 2 records
        var records1 = await task.PollAsync(CancellationToken.None);
        Assert.Equal(2, records1.Count);

        // Second batch - 1 record (hits limit)
        var records2 = await task.PollAsync(CancellationToken.None);
        Assert.Single(records2);

        // Third batch - empty (limit reached)
        var records3 = await task.PollAsync(CancellationToken.None);
        Assert.Empty(records3);
    }

    [Fact]
    public async Task PollAsync_WithSequenceTemplate_IncreasesSequence()
    {
        using var task = new GeneratorSourceTask();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.IntervalMs] = "0",
            [GeneratorConnectorConfig.BatchSize] = "3",
            [GeneratorConnectorConfig.KeyTemplate] = "${sequence}",
            [GeneratorConnectorConfig.SequenceStart] = "100",
            [GeneratorConnectorConfig.SequenceStep] = "10"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal("100", Encoding.UTF8.GetString(records[0].Key!));
        Assert.Equal("110", Encoding.UTF8.GetString(records[1].Key!));
        Assert.Equal("120", Encoding.UTF8.GetString(records[2].Key!));
    }

    [Fact]
    public async Task PollAsync_WithUuidTemplate_GeneratesUniqueUuids()
    {
        using var task = new GeneratorSourceTask();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.IntervalMs] = "0",
            [GeneratorConnectorConfig.BatchSize] = "3",
            [GeneratorConnectorConfig.KeyTemplate] = "${uuid}"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        var uuids = records.Select(r => Encoding.UTF8.GetString(r.Key!)).ToList();
        Assert.Equal(3, uuids.Distinct().Count()); // All unique
        Assert.All(uuids, u => Assert.True(Guid.TryParse(u, out _))); // Valid GUIDs
    }

    [Fact]
    public async Task PollAsync_WithTimestampTemplate_GeneratesTimestamp()
    {
        using var task = new GeneratorSourceTask();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.IntervalMs] = "0",
            [GeneratorConnectorConfig.BatchSize] = "1",
            [GeneratorConnectorConfig.KeyTemplate] = "${timestamp_ms}"
        };
        task.Start(config);

        var beforeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var records = await task.PollAsync(CancellationToken.None);
        var afterMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var timestampMs = long.Parse(Encoding.UTF8.GetString(records[0].Key!));
        Assert.InRange(timestampMs, beforeMs, afterMs);
    }

    [Fact]
    public async Task PollAsync_WithRandomSeed_ProducesReproducibleOutput()
    {
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.IntervalMs] = "0",
            [GeneratorConnectorConfig.BatchSize] = "3",
            [GeneratorConnectorConfig.KeyTemplate] = "${random_int}",
            [GeneratorConnectorConfig.RandomSeed] = "42"
        };

        // First run
        using var task1 = new GeneratorSourceTask();
        task1.Start(config);
        var records1 = await task1.PollAsync(CancellationToken.None);
        var values1 = records1.Select(r => Encoding.UTF8.GetString(r.Key!)).ToList();

        // Second run with same seed
        using var task2 = new GeneratorSourceTask();
        task2.Start(config);
        var records2 = await task2.PollAsync(CancellationToken.None);
        var values2 = records2.Select(r => Encoding.UTF8.GetString(r.Key!)).ToList();

        Assert.Equal(values1, values2);
    }

    [Fact]
    public async Task PollAsync_WithRandomIntRange_GeneratesInRange()
    {
        using var task = new GeneratorSourceTask();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.IntervalMs] = "0",
            [GeneratorConnectorConfig.BatchSize] = "10",
            [GeneratorConnectorConfig.KeyTemplate] = "${random_int}",
            [GeneratorConnectorConfig.RandomIntMin] = "50",
            [GeneratorConnectorConfig.RandomIntMax] = "100"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        foreach (var record in records)
        {
            var value = int.Parse(Encoding.UTF8.GetString(record.Key!));
            Assert.InRange(value, 50, 100);
        }
    }

    [Fact]
    public async Task PollAsync_WithRandomStringLength_GeneratesCorrectLength()
    {
        using var task = new GeneratorSourceTask();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.IntervalMs] = "0",
            [GeneratorConnectorConfig.BatchSize] = "3",
            [GeneratorConnectorConfig.KeyTemplate] = "${random_string}",
            [GeneratorConnectorConfig.RandomStringLength] = "20"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.All(records, r => Assert.Equal(20, Encoding.UTF8.GetString(r.Key!).Length));
    }

    [Fact]
    public async Task PollAsync_WithJsonTemplate_GeneratesValidJson()
    {
        using var task = new GeneratorSourceTask();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.IntervalMs] = "0",
            [GeneratorConnectorConfig.BatchSize] = "1",
            [GeneratorConnectorConfig.ValueTemplate] = "{\"id\":${sequence},\"name\":\"test\"}"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        var json = Encoding.UTF8.GetString(records[0].Value!);
        var doc = JsonDocument.Parse(json);
        Assert.Equal(1, doc.RootElement.GetProperty("id").GetInt64());
        Assert.Equal("test", doc.RootElement.GetProperty("name").GetString());
    }

    [Fact]
    public async Task PollAsync_SetsCorrectHeaders()
    {
        using var task = new GeneratorSourceTask();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.IntervalMs] = "0",
            [GeneratorConnectorConfig.BatchSize] = "1"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.NotNull(records[0].Headers);
        Assert.Contains("generator.sequence", records[0].Headers!.Keys);
        Assert.Contains("generator.count", records[0].Headers!.Keys);
    }

    [Fact]
    public async Task PollAsync_WithTopicPlaceholder_ReplacesToTopic()
    {
        using var task = new GeneratorSourceTask();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "my-test-topic",
            [GeneratorConnectorConfig.IntervalMs] = "0",
            [GeneratorConnectorConfig.BatchSize] = "1",
            [GeneratorConnectorConfig.KeyTemplate] = "${topic}"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal("my-test-topic", Encoding.UTF8.GetString(records[0].Key!));
    }

    [Fact]
    public async Task PollAsync_WithRandomBool_GeneratesTrueOrFalse()
    {
        using var task = new GeneratorSourceTask();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.IntervalMs] = "0",
            [GeneratorConnectorConfig.BatchSize] = "50",
            [GeneratorConnectorConfig.KeyTemplate] = "${random_bool}"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        var values = records.Select(r => Encoding.UTF8.GetString(r.Key!)).ToList();
        Assert.Contains("true", values);
        Assert.Contains("false", values);
    }

    [Fact]
    public async Task PollAsync_WithRandomDouble_GeneratesInRange()
    {
        using var task = new GeneratorSourceTask();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.IntervalMs] = "0",
            [GeneratorConnectorConfig.BatchSize] = "10",
            [GeneratorConnectorConfig.KeyTemplate] = "${random_double}",
            [GeneratorConnectorConfig.RandomDoubleMin] = "1.5",
            [GeneratorConnectorConfig.RandomDoubleMax] = "2.5"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        foreach (var record in records)
        {
            var value = double.Parse(Encoding.UTF8.GetString(record.Key!));
            Assert.InRange(value, 1.5, 2.5);
        }
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        using var task = new GeneratorSourceTask();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic"
        };
        task.Start(config);

        var exception = Record.Exception(() =>
        {
            task.Stop();
            task.Stop();
        });

        Assert.Null(exception);
    }
}
