using System.Text;
using Kuestenlogik.Surgewave.Connector.ICal;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.ICal.Tests;

public class ICalSinkTaskOutputModeTests
{
    private static SinkRecord EventRecord(string uid, long offset) => new()
    {
        Topic = "events",
        Partition = 0,
        Offset = offset,
        Key = Encoding.UTF8.GetBytes(uid),
        Value = Encoding.UTF8.GetBytes(
            $$"""{"uid":"{{uid}}","summary":"Standup","start":"2026-01-01T09:00:00Z","end":"2026-01-01T09:15:00Z"}""")
    };

    [Fact]
    public void Start_RecordMode_ThrowsWithoutOutputTopic()
    {
        using var task = new ICalSinkTask();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.OutputModeConfig] = ICalConnectorConfig.OutputModeRecord
        };

        var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
        Assert.Contains(ICalConnectorConfig.OutputTopicConfig, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_ThrowsOnUnknownOutputMode()
    {
        using var task = new ICalSinkTask();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.OutputModeConfig] = "carrier-pigeon"
        };

        var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
        Assert.Contains(ICalConnectorConfig.OutputModeConfig, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_RecordMode_ProducesIcsPerRecord()
    {
        var producer = new RecordingProducer();

        using var task = new ICalSinkTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(new Dictionary<string, string>
        {
            [ICalConnectorConfig.OutputModeConfig] = ICalConnectorConfig.OutputModeRecord,
            [ICalConnectorConfig.OutputTopicConfig] = "calendar.ics"
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync([EventRecord("evt-1", 0), EventRecord("evt-2", 1)], cts.Token);

        Assert.Equal(2, producer.Produced.Count);
        Assert.All(producer.Produced, p => Assert.Equal("calendar.ics", p.Topic));
        Assert.Equal("evt-1", Encoding.UTF8.GetString(producer.Produced[0].Key!));

        var ics = Encoding.UTF8.GetString(producer.Produced[0].Value!);
        Assert.Contains("BEGIN:VCALENDAR", ics, StringComparison.Ordinal);
        Assert.Contains("UID:evt-1", ics, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_RecordMode_SkipsInvalidJsonAndRaisesError()
    {
        var producer = new RecordingProducer();
        var errors = new List<Exception>();

        using var task = new ICalSinkTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = errors.Add });
        task.Start(new Dictionary<string, string>
        {
            [ICalConnectorConfig.OutputModeConfig] = ICalConnectorConfig.OutputModeRecord,
            [ICalConnectorConfig.OutputTopicConfig] = "calendar.ics"
        });

        var poison = new SinkRecord
        {
            Topic = "events",
            Partition = 0,
            Offset = 0,
            Value = Encoding.UTF8.GetBytes("not json")
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await task.PutAsync([poison, EventRecord("evt-1", 1)], cts.Token);

        Assert.Single(producer.Produced);
        Assert.Single(errors);
    }

    [Fact]
    public async Task PutAsync_FileMode_RotationKeepsPreviouslyFlushedEvents()
    {
        var directory = Path.Combine(Path.GetTempPath(), "ical-sink-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);

        try
        {
            // No ${timestamp} placeholder: every rotation used to overwrite the same file.
            var outputPath = Path.Combine(directory, "events.ics");

            using var task = new ICalSinkTask();
            task.Initialize(new TaskContext { RaiseError = _ => { } });
            task.Start(new Dictionary<string, string>
            {
                [ICalConnectorConfig.OutputModeConfig] = ICalConnectorConfig.OutputModeFile,
                [ICalConnectorConfig.OutputPathConfig] = outputPath,
                [ICalConnectorConfig.MaxEventsPerFileConfig] = "1"
            });

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await task.PutAsync([EventRecord("evt-1", 0)], cts.Token);
            await task.PutAsync([EventRecord("evt-2", 1)], cts.Token);

            var files = Directory.GetFiles(directory, "*.ics");
            Assert.Equal(2, files.Length);

            var contents = files.Select(File.ReadAllText).ToList();
            Assert.Contains(contents, c => c.Contains("UID:evt-1", StringComparison.Ordinal));
            Assert.Contains(contents, c => c.Contains("UID:evt-2", StringComparison.Ordinal));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task PutAsync_FileMode_HonorsFlushInterval()
    {
        var directory = Path.Combine(Path.GetTempPath(), "ical-sink-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);

        try
        {
            var outputPath = Path.Combine(directory, "events.ics");

            using var task = new ICalSinkTask();
            task.Initialize(new TaskContext { RaiseError = _ => { } });
            task.Start(new Dictionary<string, string>
            {
                [ICalConnectorConfig.OutputModeConfig] = ICalConnectorConfig.OutputModeFile,
                [ICalConnectorConfig.OutputPathConfig] = outputPath,
                [ICalConnectorConfig.MaxEventsPerFileConfig] = "1000",
                [ICalConnectorConfig.FlushIntervalMsConfig] = "0"
            });

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await task.PutAsync([EventRecord("evt-1", 0)], cts.Token);

            // The batch is far below max.events.per.file, so only the interval can flush it.
            Assert.True(File.Exists(outputPath));
            Assert.Contains("UID:evt-1", await File.ReadAllTextAsync(outputPath, cts.Token), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private sealed class RecordingProducer : IConnectProducer
    {
        public List<(string Topic, byte[]? Key, byte[]? Value)> Produced { get; } = [];

        public Task ProduceAsync(string topic, byte[]? key, byte[]? value, CancellationToken cancellationToken = default)
        {
            Produced.Add((topic, key, value));
            return Task.CompletedTask;
        }

        public Task ProduceAsync(string topic, byte[]? key, byte[]? value, IDictionary<string, byte[]>? headers,
            CancellationToken cancellationToken = default)
            => ProduceAsync(topic, key, value, cancellationToken);
    }
}
