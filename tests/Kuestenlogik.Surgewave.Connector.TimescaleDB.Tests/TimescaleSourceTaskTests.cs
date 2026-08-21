using System.Globalization;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.TimescaleDB.Tests;

/// <summary>
/// Tests for the incremental read of <see cref="TimescaleSourceTask"/>: the SQL it builds around
/// the time filter, and the cursor it starts from. A cursor that lives only in memory replays the
/// whole lookback window after every restart, which duplicates every row in that window.
/// </summary>
public class TimescaleSourceTaskTests
{
    private const string ConnectionString = "Host=127.0.0.1;Port=5432;Database=metrics;Username=u;Password=p";

    [Fact]
    public void BuildQuery_FromATable_ReadsEverythingAfterTheCursorInTimeOrder()
    {
        using var task = StartTask((TimescaleConnectorConfig.Table, "readings"));

        var sql = task.BuildQuery();

        Assert.Contains("SELECT *", sql, StringComparison.Ordinal);
        Assert.Contains("FROM readings", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE time > @lastTime", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY time ASC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT @limit", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_FromATable_ProjectsOnlyTheConfiguredColumns()
    {
        using var task = StartTask(
            (TimescaleConnectorConfig.Table, "readings"),
            (TimescaleConnectorConfig.Columns, "ts, device, value"));

        var sql = task.BuildQuery();

        Assert.Contains("SELECT ts, device, value", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT *", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_FromATable_UsesTheConfiguredTimeColumn()
    {
        using var task = StartTask(
            (TimescaleConnectorConfig.Table, "readings"),
            (TimescaleConnectorConfig.TimeColumn, "observed_at"));

        var sql = task.BuildQuery();

        Assert.Contains("WHERE observed_at > @lastTime", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY observed_at ASC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_FromABareCustomQuery_AddsTheFilterOrderAndLimit()
    {
        using var task = StartTask((TimescaleConnectorConfig.Query, "SELECT * FROM readings"));

        Assert.Equal(
            "SELECT * FROM readings WHERE time > @lastTime ORDER BY time ASC LIMIT @limit",
            task.BuildQuery());
    }

    [Fact]
    public void BuildQuery_FromAFilteredCustomQuery_KeepsTheOperatorsOwnPredicate()
    {
        using var task = StartTask(
            (TimescaleConnectorConfig.Query, "SELECT * FROM readings WHERE device = 'a'"));

        Assert.Equal(
            "SELECT * FROM readings WHERE time > @lastTime AND device = 'a' ORDER BY time ASC LIMIT @limit",
            task.BuildQuery());
    }

    [Fact]
    public void BuildQuery_FromASortedCustomQuery_PutsTheFilterBeforeTheOrdering()
    {
        using var task = StartTask(
            (TimescaleConnectorConfig.Query, "SELECT * FROM readings ORDER BY time DESC"));

        Assert.Equal(
            "SELECT * FROM readings WHERE time > @lastTime ORDER BY time DESC LIMIT @limit",
            task.BuildQuery());
    }

    [Fact]
    public void BuildQuery_FromACustomQueryThatAlreadyOrdersAndLimits_DoesNotAddASecondOne()
    {
        using var task = StartTask(
            (TimescaleConnectorConfig.Query, "SELECT * FROM readings WHERE device = 'a' ORDER BY time ASC LIMIT 10"));

        Assert.Equal(
            "SELECT * FROM readings WHERE time > @lastTime AND device = 'a' ORDER BY time ASC LIMIT 10",
            task.BuildQuery());
    }

    [Fact]
    public void InitializeCursor_ResumesFromTheTimestampThePreviousRunStored()
    {
        var reader = new FakeOffsetStorageReader("2026-01-02T03:04:05.0000000Z");
        using var task = StartTask(reader, (TimescaleConnectorConfig.Table, "readings"));

        task.InitializeCursor();

        Assert.Equal(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc), task.LastTimestamp);
        Assert.Equal("readings", Assert.Single(reader.RequestedTables));
    }

    [Fact]
    public void InitializeCursor_WithoutAStoredOffset_FallsBackToTheLookbackWindow()
    {
        var before = DateTime.UtcNow;
        using var task = StartTask(
            new FakeOffsetStorageReader(storedLastTime: null),
            (TimescaleConnectorConfig.Table, "readings"),
            (TimescaleConnectorConfig.LookbackSeconds, "60"));

        task.InitializeCursor();

        Assert.InRange(task.LastTimestamp, before.AddSeconds(-61), DateTime.UtcNow.AddSeconds(-59));
    }

    [Fact]
    public void InitializeCursor_OnlyConsultsTheOffsetStoreOnce()
    {
        var reader = new FakeOffsetStorageReader("2026-01-02T03:04:05.0000000Z");
        using var task = StartTask(reader, (TimescaleConnectorConfig.Table, "readings"));

        task.InitializeCursor();
        task.InitializeCursor();

        Assert.Single(reader.RequestedTables);
    }

    [Fact]
    public void InitializeCursor_IgnoresAStoredValueThatIsNotATimestamp()
    {
        var before = DateTime.UtcNow;
        using var task = StartTask(
            new FakeOffsetStorageReader("not-a-timestamp"),
            (TimescaleConnectorConfig.Table, "readings"),
            (TimescaleConnectorConfig.LookbackSeconds, "60"));

        task.InitializeCursor();

        Assert.InRange(task.LastTimestamp, before.AddSeconds(-61), DateTime.UtcNow.AddSeconds(-59));
    }

    private static TimescaleSourceTask StartTask(params (string Key, string Value)[] settings) =>
        StartTask(null, settings);

    private static TimescaleSourceTask StartTask(
        IOffsetStorageReader? reader,
        params (string Key, string Value)[] settings)
    {
        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TimescaleConnectorConfig.Topic] = "metrics",
            [TimescaleConnectorConfig.ConnectionString] = ConnectionString
        };

        foreach (var (key, value) in settings)
        {
            config[key] = value;
        }

        var task = new TimescaleSourceTask();
        task.Initialize(new TaskContext { OffsetStorageReader = reader, RaiseError = _ => { } });
        task.Start(config);
        return task;
    }

    private sealed class FakeOffsetStorageReader(string? storedLastTime) : IOffsetStorageReader
    {
        public List<string> RequestedTables { get; } = [];

        public IDictionary<string, object>? Offset(IDictionary<string, object> partition)
        {
            Assert.Equal("timescale", partition["source"]);
            RequestedTables.Add(Convert.ToString(partition["table"], CultureInfo.InvariantCulture)!);

            return storedLastTime == null
                ? null
                : new Dictionary<string, object>(StringComparer.Ordinal) { ["last_time"] = storedLastTime };
        }

        public IDictionary<IDictionary<string, object>, IDictionary<string, object>> Offsets(
            IReadOnlyCollection<IDictionary<string, object>> partitions) => throw new NotSupportedException();
    }
}
