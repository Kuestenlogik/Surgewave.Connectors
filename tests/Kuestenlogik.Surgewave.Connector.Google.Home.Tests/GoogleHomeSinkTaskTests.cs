using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Google.Home.Tests;

/// <summary>
/// Exercises the report side without Home Graph credentials: how a record payload becomes a
/// device state, where the device id comes from, and what happens to a failed report.
/// </summary>
public class GoogleHomeSinkTaskTests
{
    [Fact]
    public void BuildDeviceState_WithNothingButAnEmptyPayload_StillReportsTheDeviceAsOnline()
    {
        var state = GoogleHomeSinkTask.BuildDeviceState(Element("{}"));

        var entry = Assert.Single(state);
        Assert.Equal("online", entry.Key);
        Assert.True(Assert.IsType<bool>(entry.Value));
    }

    [Fact]
    public void BuildDeviceState_WithAnExplicitOnlineFlag_DoesNotOverrideIt()
    {
        var state = GoogleHomeSinkTask.BuildDeviceState(Element("""{"online":false}"""));

        Assert.False(Assert.IsType<bool>(state["online"]));
    }

    [Fact]
    public void BuildDeviceState_OnOffAndBrightness_AreCarriedThrough()
    {
        var state = GoogleHomeSinkTask.BuildDeviceState(Element("""{"on":true,"brightness":42}"""));

        Assert.True(Assert.IsType<bool>(state["on"]));
        Assert.Equal(42, Assert.IsType<int>(state["brightness"]));
    }

    [Fact]
    public void BuildDeviceState_SpectrumHsv_IsNestedUnderColor()
    {
        var state = GoogleHomeSinkTask.BuildDeviceState(
            Element("""{"color":{"spectrumHsv":{"hue":120.0,"saturation":0.5,"value":0.9}}}"""));

        var color = Assert.IsType<Dictionary<string, object>>(state["color"]);
        var hsv = Assert.IsType<Dictionary<string, object>>(color["spectrumHsv"]);
        Assert.Equal(120.0, Assert.IsType<double>(hsv["hue"]));
        Assert.Equal(0.5, Assert.IsType<double>(hsv["saturation"]));
        Assert.Equal(0.9, Assert.IsType<double>(hsv["value"]));
    }

    [Fact]
    public void BuildDeviceState_SpectrumHsvWithMissingComponents_FallsBackToFullSaturationAndValue()
    {
        var state = GoogleHomeSinkTask.BuildDeviceState(Element("""{"color":{"spectrumHsv":{}}}"""));

        var color = Assert.IsType<Dictionary<string, object>>(state["color"]);
        var hsv = Assert.IsType<Dictionary<string, object>>(color["spectrumHsv"]);
        Assert.Equal(0.0, Assert.IsType<double>(hsv["hue"]));
        Assert.Equal(1.0, Assert.IsType<double>(hsv["saturation"]));
        Assert.Equal(1.0, Assert.IsType<double>(hsv["value"]));
    }

    [Fact]
    public void BuildDeviceState_ColorTemperatureShorthand_WinsOverAColorBlock()
    {
        var state = GoogleHomeSinkTask.BuildDeviceState(
            Element("""{"color":{"temperatureK":4000},"colorTemperature":2700}"""));

        var color = Assert.IsType<Dictionary<string, object>>(state["color"]);
        Assert.Equal(2700, Assert.IsType<int>(color["temperatureK"]));
    }

    [Fact]
    public void BuildDeviceState_ThermostatFields_AreCarriedThrough()
    {
        var state = GoogleHomeSinkTask.BuildDeviceState(
            Element("""{"thermostatMode":"heat","thermostatTemperatureSetpoint":21.5,"thermostatTemperatureAmbient":19.25}"""));

        Assert.Equal("heat", Assert.IsType<string>(state["thermostatMode"]));
        Assert.Equal(21.5, Assert.IsType<double>(state["thermostatTemperatureSetpoint"]));
        Assert.Equal(19.25, Assert.IsType<double>(state["thermostatTemperatureAmbient"]));
    }

    [Fact]
    public void BuildDeviceState_LockAndOpenCloseFields_AreCarriedThrough()
    {
        var state = GoogleHomeSinkTask.BuildDeviceState(
            Element("""{"isLocked":true,"isJammed":false,"openPercent":40,"currentFanSpeedSetting":"low"}"""));

        Assert.True(Assert.IsType<bool>(state["isLocked"]));
        Assert.False(Assert.IsType<bool>(state["isJammed"]));
        Assert.Equal(40, Assert.IsType<int>(state["openPercent"]));
        Assert.Equal("low", Assert.IsType<string>(state["currentFanSpeedSetting"]));
    }

    [Fact]
    public void GetString_PrefersThePayloadPropertyOverTheHeader()
    {
        var headers = new Dictionary<string, byte[]> { ["google.deviceId"] = Encoding.UTF8.GetBytes("from-header") };

        Assert.Equal("from-payload",
            GoogleHomeSinkTask.GetString(Element("""{"deviceId":"from-payload"}"""), "deviceId", headers));
    }

    [Fact]
    public void GetString_FallsBackToTheGooglePrefixedHeaderAndThenToNothing()
    {
        var headers = new Dictionary<string, byte[]> { ["google.deviceId"] = Encoding.UTF8.GetBytes("from-header") };

        Assert.Equal("from-header", GoogleHomeSinkTask.GetString(Element("{}"), "deviceId", headers));
        Assert.Null(GoogleHomeSinkTask.GetString(Element("{}"), "deviceId", null));
    }

    [Fact]
    public async Task PutAsync_WithoutADeviceIdAnywhere_SkipsTheRecordWithoutCallingHomeGraph()
    {
        var errors = new List<Exception>();
        using var task = new GoogleHomeSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfig(SinkConfig());

        await task.PutAsync([Record("""{"on":true}""")], TestContext.Current.CancellationToken);

        Assert.Empty(errors);
    }

    [Fact]
    public async Task PutAsync_WithAMalformedPayload_RaisesTheErrorAndKeepsGoing()
    {
        // The empty catch acknowledged unparseable records as if they had been reported.
        var errors = new List<Exception>();
        using var task = new GoogleHomeSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfig(SinkConfig());

        await task.PutAsync([Record("this is not json"), Record("""{"on":true}""")],
            TestContext.Current.CancellationToken);

        Assert.IsType<JsonException>(Assert.Single(errors), exactMatch: false);
    }

    [Fact]
    public async Task PutAsync_WhenTheReportCallFails_RaisesAndRethrowsInsteadOfAcknowledging()
    {
        // Start was deliberately not called, so reporting fails on the missing Home Graph
        // service. What is pinned is where the failure goes: it used to be swallowed while
        // the worker committed the offset of a state change Google never received.
        var errors = new List<Exception>();
        using var task = new GoogleHomeSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        var config = SinkConfig();
        config[GoogleHomeConnectorConfig.DefaultDeviceId] = "dev-1";
        task.ApplyConfig(config);

        var thrown = await Assert.ThrowsAnyAsync<Exception>(() =>
            task.PutAsync([Record("""{"on":true}""")], TestContext.Current.CancellationToken));

        Assert.Same(thrown, Assert.Single(errors));
    }

    private static JsonElement Element(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }

    private static SinkRecord Record(string json) => new()
    {
        Topic = "home-commands",
        Partition = 0,
        Offset = 0,
        Value = Encoding.UTF8.GetBytes(json)
    };

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [GoogleHomeConnectorConfig.Topics] = "home-commands",
        [GoogleHomeConnectorConfig.AgentUserId] = "agent-1"
    };
}
