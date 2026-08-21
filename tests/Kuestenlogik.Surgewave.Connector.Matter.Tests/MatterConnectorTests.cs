namespace Kuestenlogik.Surgewave.Connector.Matter.Tests;

/// <summary>
/// Validation of the Matter connectors before any task is handed a configuration.
/// </summary>
public class MatterConnectorTests
{
    [Fact]
    public void SourceConnector_Start_RequiresControllerUrl()
    {
        using var connector = new MatterSourceConnector();
        var config = new Dictionary<string, string>
        {
            [MatterConnectorConfig.Topic] = "matter-events"
        };

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains(MatterConnectorConfig.ControllerUrl, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_RequiresTopic()
    {
        using var connector = new MatterSourceConnector();
        var config = new Dictionary<string, string>
        {
            [MatterConnectorConfig.ControllerUrl] = "http://matter.local:5580"
        };

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains(MatterConnectorConfig.Topic, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_Start_RequiresControllerUrl()
    {
        using var connector = new MatterSinkConnector();
        var config = new Dictionary<string, string>
        {
            [MatterConnectorConfig.Topics] = "matter-commands"
        };

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains(MatterConnectorConfig.ControllerUrl, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_TaskConfigs_HandsTheTaskAnIndependentCopy()
    {
        using var connector = new MatterSinkConnector();
        var config = new Dictionary<string, string>
        {
            [MatterConnectorConfig.Topics] = "matter-commands",
            [MatterConnectorConfig.ControllerUrl] = "http://matter.local:5580"
        };

        connector.Start(config);
        var taskConfig = Assert.Single(connector.TaskConfigs(2));
        config[MatterConnectorConfig.ControllerUrl] = "http://changed:1";

        Assert.Equal(typeof(MatterSinkTask), connector.TaskClass);
        Assert.Equal("http://matter.local:5580", taskConfig[MatterConnectorConfig.ControllerUrl]);
    }
}
