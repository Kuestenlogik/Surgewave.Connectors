using System.Globalization;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Connector.InfluxDB.Tests;

public class InfluxDBSinkTaskTimestampTests
{
    private const long EpochMillis = 1755772800000L;

    private static Dictionary<string, string> CreateConfig(string precision) => new()
    {
        [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
        [InfluxDBConnectorConfig.TokenConfig] = "test-token",
        [InfluxDBConnectorConfig.OrgConfig] = "test-org",
        [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
        [InfluxDBConnectorConfig.PrecisionConfig] = precision
    };

    [Fact]
    public void ParseTimestamp_NumericValue_IsUnixEpochInConfiguredPrecision()
    {
        using var task = new InfluxDBSinkTask();
        task.Start(CreateConfig("ms"));

        var parsed = task.ParseTimestamp(EpochMillis);

        // Not a Windows FILETIME - that reading would place this point in January 1601
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(EpochMillis).UtcDateTime, parsed);
        Assert.Equal(2025, parsed!.Value.Year);
    }

    [Fact]
    public void ParseTimestamp_JsonNumber_IsUnixEpochInConfiguredPrecision()
    {
        using var task = new InfluxDBSinkTask();
        task.Start(CreateConfig("ns"));

        using var doc = JsonDocument.Parse((EpochMillis * 1_000_000L).ToString(CultureInfo.InvariantCulture));
        var parsed = task.ParseTimestamp(doc.RootElement);

        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(EpochMillis).UtcDateTime, parsed);
    }

    [Fact]
    public void ParseTimestamp_NumericValue_HonorsSecondsPrecision()
    {
        using var task = new InfluxDBSinkTask();
        task.Start(CreateConfig("s"));

        var parsed = task.ParseTimestamp(EpochMillis / 1000L);

        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(EpochMillis).UtcDateTime, parsed);
    }

    [Fact]
    public void ParseTimestamp_IsoString_IsParsedAsUtc()
    {
        using var task = new InfluxDBSinkTask();
        task.Start(CreateConfig("ms"));

        var parsed = task.ParseTimestamp("2026-08-20T12:00:00Z");

        Assert.Equal(new DateTime(2026, 8, 20, 12, 0, 0, DateTimeKind.Utc), parsed);
    }

    [Fact]
    public void ParseTimestamp_UnusableValue_ReturnsNull()
    {
        using var task = new InfluxDBSinkTask();
        task.Start(CreateConfig("ms"));

        Assert.Null(task.ParseTimestamp("not-a-timestamp"));
        Assert.Null(task.ParseTimestamp(long.MaxValue));
    }
}
