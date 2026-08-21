using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.InfluxDB.Tests;

public class InfluxDBSourceTaskCursorTests
{
    private static Dictionary<string, string> CreateConfig() => new()
    {
        [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
        [InfluxDBConnectorConfig.TokenConfig] = "test-token",
        [InfluxDBConnectorConfig.OrgConfig] = "test-org",
        [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
        [InfluxDBConnectorConfig.MeasurementConfig] = "cpu"
    };

    [Fact]
    public void BuildQuery_WithoutStoredOffset_UsesConfiguredTimeRange()
    {
        using var task = new InfluxDBSourceTask();
        task.Initialize(new TaskContext());
        task.Start(CreateConfig());

        var query = task.BuildQuery();

        Assert.Contains("range(start: -1h)", query);
    }

    [Fact]
    public void BuildQuery_WithStoredOffset_StartsOneTickAfterLastEmittedPoint()
    {
        using var task = new InfluxDBSourceTask();
        task.Initialize(new TaskContext
        {
            OffsetStorageReader = new StubOffsetStorageReader(new Dictionary<string, object>
            {
                [InfluxDBConnectorConfig.OffsetTimestamp] = "2026-08-20T12:00:00.0000000Z"
            })
        });
        task.Start(CreateConfig());

        var query = task.BuildQuery();

        // Flux 'range' start is inclusive: starting one tick later keeps the newest row from
        // being re-fetched and re-emitted as a duplicate on every poll cycle.
        Assert.Contains("range(start: 2026-08-20T12:00:00.0000001Z)", query);
        Assert.DoesNotContain("range(start: -1h)", query);
    }

    [Fact]
    public void BuildQuery_WithoutOffsetStorage_FallsBackToConfiguredTimeRange()
    {
        using var task = new InfluxDBSourceTask();
        task.Initialize(new TaskContext { OffsetStorageReader = new StubOffsetStorageReader(null) });

        var config = CreateConfig();
        config[InfluxDBConnectorConfig.TimeRangeConfig] = "-24h";
        task.Start(config);

        var query = task.BuildQuery();

        Assert.Contains("range(start: -24h)", query);
    }

    [Fact]
    public void BuildQuery_WithExplicitStartAndStop_IgnoresStoredOffset()
    {
        using var task = new InfluxDBSourceTask();
        task.Initialize(new TaskContext
        {
            OffsetStorageReader = new StubOffsetStorageReader(new Dictionary<string, object>
            {
                [InfluxDBConnectorConfig.OffsetTimestamp] = "2026-08-20T12:00:00.0000000Z"
            })
        });

        var config = CreateConfig();
        config[InfluxDBConnectorConfig.StartTimeConfig] = "2026-08-01T00:00:00Z";
        config[InfluxDBConnectorConfig.StopTimeConfig] = "2026-08-02T00:00:00Z";
        task.Start(config);

        var query = task.BuildQuery();

        Assert.Contains("range(start: 2026-08-01T00:00:00Z, stop: 2026-08-02T00:00:00Z)", query);
    }

    private sealed class StubOffsetStorageReader(IDictionary<string, object>? offset) : IOffsetStorageReader
    {
        public IDictionary<string, object>? Offset(IDictionary<string, object> partition) => offset;

        public IDictionary<IDictionary<string, object>, IDictionary<string, object>> Offsets(
            IReadOnlyCollection<IDictionary<string, object>> partitions)
            => new Dictionary<IDictionary<string, object>, IDictionary<string, object>>();
    }
}
