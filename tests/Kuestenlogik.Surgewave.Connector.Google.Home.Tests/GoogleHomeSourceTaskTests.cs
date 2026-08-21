using System.Diagnostics;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Google.Home.Tests;

/// <summary>
/// Exercises the poll side without Home Graph credentials: which device types survive the
/// include switches, what a device record carries, and how failures and idle time behave.
/// </summary>
public class GoogleHomeSourceTaskTests
{
    [Theory]
    [InlineData("action.devices.types.LIGHT")]
    [InlineData("action.devices.types.SWITCH")]
    [InlineData("action.devices.types.OUTLET")]
    [InlineData("action.devices.types.THERMOSTAT")]
    [InlineData("action.devices.types.LOCK")]
    [InlineData("action.devices.types.SENSOR")]
    public void ShouldIncludeDeviceType_WithEverythingEnabled_IncludesTheKnownTypes(string deviceType)
    {
        using var task = ConfiguredTask(_ => { });

        Assert.True(task.ShouldIncludeDeviceType(deviceType));
    }

    [Theory]
    [InlineData(GoogleHomeConnectorConfig.IncludeLights, "action.devices.types.LIGHT")]
    [InlineData(GoogleHomeConnectorConfig.IncludeSwitches, "action.devices.types.SWITCH")]
    [InlineData(GoogleHomeConnectorConfig.IncludeSwitches, "action.devices.types.OUTLET")]
    [InlineData(GoogleHomeConnectorConfig.IncludeThermostats, "action.devices.types.THERMOSTAT")]
    [InlineData(GoogleHomeConnectorConfig.IncludeLocks, "action.devices.types.LOCK")]
    [InlineData(GoogleHomeConnectorConfig.IncludeSensors, "action.devices.types.SENSOR")]
    public void ShouldIncludeDeviceType_WithThatSwitchTurnedOff_ExcludesTheType(string setting, string deviceType)
    {
        using var task = ConfiguredTask(c => c[setting] = "false");

        Assert.False(task.ShouldIncludeDeviceType(deviceType));
    }

    [Fact]
    public void ShouldIncludeDeviceType_ForATypeWithoutASwitch_IsIncluded()
    {
        using var task = ConfiguredTask(c =>
        {
            c[GoogleHomeConnectorConfig.IncludeLights] = "false";
            c[GoogleHomeConnectorConfig.IncludeSensors] = "false";
        });

        Assert.True(task.ShouldIncludeDeviceType("action.devices.types.CAMERA"));
    }

    [Fact]
    public void CreateDeviceRecord_ShortensTheDeviceTypeAndCarriesTheIdEverywhereItIsNeeded()
    {
        using var task = ConfiguredTask(_ => { });

        var record = task.CreateDeviceRecord(
            "dev-1",
            "action.devices.types.LIGHT",
            new Dictionary<string, object> { ["on"] = true });

        Assert.Equal("google-home", record.Topic);
        Assert.Equal("google-home:dev-1", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("light", Encoding.UTF8.GetString(record.Headers!["google.type"]));
        Assert.Equal("dev-1", Encoding.UTF8.GetString(record.Headers!["google.device.id"]));
        Assert.Equal("google-home", record.SourcePartition["source"]);
        Assert.Equal("light", record.SourcePartition["type"]);
        Assert.Equal("dev-1", record.SourceOffset["device_id"]);
        Assert.Equal(1L, record.SourceOffset["message_id"]);

        var payload = Encoding.UTF8.GetString(record.Value);
        Assert.Contains("\"deviceId\":\"dev-1\"", payload, StringComparison.Ordinal);
        Assert.Contains("\"deviceType\":\"action.devices.types.LIGHT\"", payload, StringComparison.Ordinal);
        Assert.Contains("\"state\":{\"on\":true}", payload, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateDeviceRecord_AssignsAscendingMessageIds()
    {
        using var task = ConfiguredTask(_ => { });

        var first = task.CreateDeviceRecord("dev-1", "action.devices.types.LIGHT", null);
        var second = task.CreateDeviceRecord("dev-2", "action.devices.types.LOCK", null);

        Assert.Equal(1L, first.SourceOffset["message_id"]);
        Assert.Equal(2L, second.SourceOffset["message_id"]);
        Assert.Equal("lock", Encoding.UTF8.GetString(second.Headers!["google.type"]));
    }

    [Fact]
    public async Task PollAsync_WhenHomeGraphCannotBeReached_SurfacesTheErrorInsteadOfProducingNothingForever()
    {
        // The empty catch made wrong credentials or a dead endpoint look like an empty house.
        var errors = new List<Exception>();
        using var task = new GoogleHomeSourceTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfig(SourceConfig());

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Empty(records);
        Assert.Single(errors);
    }

    [Fact]
    public async Task PollAsync_BeforeTheIntervalElapsed_WaitsInsteadOfBusySpinning()
    {
        // Returning [] immediately made the worker's poll loop spin between intervals.
        using var task = new GoogleHomeSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        var config = SourceConfig();
        config[GoogleHomeConnectorConfig.PollIntervalMs] = "250";
        task.ApplyConfig(config);

        await task.PollAsync(TestContext.Current.CancellationToken);

        var stopwatch = Stopwatch.StartNew();
        await task.PollAsync(TestContext.Current.CancellationToken);
        stopwatch.Stop();

        Assert.True(
            stopwatch.ElapsedMilliseconds >= 100,
            $"expected the second poll to wait out the interval, it returned after {stopwatch.ElapsedMilliseconds} ms");
    }

    private static GoogleHomeSourceTask ConfiguredTask(Action<Dictionary<string, string>> configure)
    {
        var config = SourceConfig();
        configure(config);

        var task = new GoogleHomeSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.ApplyConfig(config);
        return task;
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [GoogleHomeConnectorConfig.Topic] = "google-home",
        [GoogleHomeConnectorConfig.AgentUserId] = "agent-1",
        [GoogleHomeConnectorConfig.PollIntervalMs] = "0"
    };
}
