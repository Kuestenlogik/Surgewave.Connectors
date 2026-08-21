using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.TimescaleDB.Tests;

/// <summary>
/// Tests for how <see cref="TimescaleSinkTask"/> turns a JSON record into a hypertable row. The
/// time column decides which chunk a row lands in, so a timestamp that arrives as text instead of
/// a <see cref="DateTime"/> is a write that either fails or lands in the wrong place.
/// </summary>
public class TimescaleSinkTaskTests
{
    private const string ConnectionString = "Host=127.0.0.1;Port=5432;Database=metrics;Username=u;Password=p";

    [Fact]
    public void ConvertJsonValue_ParsesTheTimeColumnFromAnIsoString()
    {
        using var task = StartTask();
        using var document = JsonDocument.Parse("""{"time":"2026-01-02T03:04:05Z"}""");

        var value = task.ConvertJsonValue(document.RootElement.GetProperty("time"), "time");

        var timestamp = Assert.IsType<DateTime>(value);
        Assert.Equal(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc), timestamp);
        Assert.Equal(DateTimeKind.Utc, timestamp.Kind);
    }

    [Fact]
    public void ConvertJsonValue_ReadsTheTimeColumnFromAUnixMillisecondNumber()
    {
        using var task = StartTask();
        using var document = JsonDocument.Parse("""{"time":1767322245000}""");

        var value = task.ConvertJsonValue(document.RootElement.GetProperty("time"), "time");

        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1767322245000).UtcDateTime, Assert.IsType<DateTime>(value));
    }

    [Fact]
    public void ConvertJsonValue_LeavesAnUnparsableTimestampAsText()
    {
        // Better handed to PostgreSQL as-is than silently turned into "now".
        using var task = StartTask();
        using var document = JsonDocument.Parse("""{"time":"whenever"}""");

        var value = task.ConvertJsonValue(document.RootElement.GetProperty("time"), "time");

        Assert.Equal("whenever", Assert.IsType<string>(value));
    }

    [Fact]
    public void ConvertJsonValue_OnlyTreatsTheConfiguredFieldAsTheTimeColumn()
    {
        using var task = StartTask((TimescaleConnectorConfig.TimeColumnField, "observed_at"));
        using var document = JsonDocument.Parse("""{"time":"2026-01-02T03:04:05Z"}""");

        var value = task.ConvertJsonValue(document.RootElement.GetProperty("time"), "time");

        Assert.Equal("2026-01-02T03:04:05Z", Assert.IsType<string>(value));
    }

    [Fact]
    public void ConvertJsonValue_MapsWholeNumbersToBigint()
    {
        using var task = StartTask();
        using var document = JsonDocument.Parse("""{"count":9007199254740993}""");

        var value = task.ConvertJsonValue(document.RootElement.GetProperty("count"), "count");

        Assert.Equal(9007199254740993L, Assert.IsType<long>(value));
    }

    [Fact]
    public void ConvertJsonValue_MapsFractionalNumbersToDouble()
    {
        using var task = StartTask();
        using var document = JsonDocument.Parse("""{"value":1.5}""");

        var value = task.ConvertJsonValue(document.RootElement.GetProperty("value"), "value");

        Assert.Equal(1.5d, Assert.IsType<double>(value), 6);
    }

    [Fact]
    public void ConvertJsonValue_MapsBooleansAndNulls()
    {
        using var task = StartTask();
        using var document = JsonDocument.Parse("""{"ok":true,"bad":false,"missing":null}""");

        var root = document.RootElement;
        Assert.True(Assert.IsType<bool>(task.ConvertJsonValue(root.GetProperty("ok"), "ok")));
        Assert.False(Assert.IsType<bool>(task.ConvertJsonValue(root.GetProperty("bad"), "bad")));
        Assert.Null(task.ConvertJsonValue(root.GetProperty("missing"), "missing"));
    }

    [Fact]
    public void ConvertJsonValue_KeepsNestedStructuresAsJsonForJsonbColumns()
    {
        using var task = StartTask();
        using var document = JsonDocument.Parse("""{"tags":["a","b"],"meta":{"unit":"C"}}""");

        var root = document.RootElement;
        Assert.Equal("""["a","b"]""", Assert.IsType<string>(task.ConvertJsonValue(root.GetProperty("tags"), "tags")));
        Assert.Equal("""{"unit":"C"}""", Assert.IsType<string>(task.ConvertJsonValue(root.GetProperty("meta"), "meta")));
    }

    [Fact]
    public async Task PutAsync_WithAPoisonRecord_SurfacesItInsteadOfDroppingItSilently()
    {
        var errors = new List<Exception>();
        using var task = StartTask(errors.Add);

        await task.PutAsync([Record("this is not json")], CancellationToken.None);

        Assert.IsType<JsonException>(Assert.Single(errors), exactMatch: false);
    }

    [Fact]
    public async Task PutAsync_KeepsGoingAfterAPoisonRecord()
    {
        var errors = new List<Exception>();
        using var task = StartTask(errors.Add);

        await task.PutAsync([Record("{"), Record("]")], CancellationToken.None);

        Assert.Equal(2, errors.Count);
    }

    private static TimescaleSinkTask StartTask(params (string Key, string Value)[] settings) =>
        StartTask(null, settings);

    private static TimescaleSinkTask StartTask(
        Action<Exception>? raiseError,
        params (string Key, string Value)[] settings)
    {
        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TimescaleConnectorConfig.Topics] = "metrics",
            [TimescaleConnectorConfig.TargetTable] = "readings",
            [TimescaleConnectorConfig.ConnectionString] = ConnectionString
        };

        foreach (var (key, value) in settings)
        {
            config[key] = value;
        }

        var task = new TimescaleSinkTask();
        task.Initialize(new TaskContext { RaiseError = raiseError ?? (_ => { }) });
        task.Start(config);
        return task;
    }

    private static SinkRecord Record(string json) => new()
    {
        Topic = "metrics",
        Partition = 0,
        Offset = 1,
        Value = Encoding.UTF8.GetBytes(json)
    };
}
