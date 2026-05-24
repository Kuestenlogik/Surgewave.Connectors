using Kuestenlogik.Surgewave.Connector.Generator;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Generator.Tests;

public class GeneratorSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new GeneratorSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsGeneratorSourceTask()
    {
        var connector = new GeneratorSourceConnector();
        Assert.Equal(typeof(GeneratorSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new GeneratorSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == GeneratorConnectorConfig.Topic);
        Assert.Contains(connector.Config.Keys, k => k.Name == GeneratorConnectorConfig.MessageCount);
        Assert.Contains(connector.Config.Keys, k => k.Name == GeneratorConnectorConfig.IntervalMs);
        Assert.Contains(connector.Config.Keys, k => k.Name == GeneratorConnectorConfig.BatchSize);
        Assert.Contains(connector.Config.Keys, k => k.Name == GeneratorConnectorConfig.KeyTemplate);
        Assert.Contains(connector.Config.Keys, k => k.Name == GeneratorConnectorConfig.ValueTemplate);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        var connector = new GeneratorSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingTopic_ThrowsArgumentException()
    {
        var connector = new GeneratorSourceConnector();
        var config = new Dictionary<string, string>();

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithInvalidMessageFormat_ThrowsArgumentException()
    {
        var connector = new GeneratorSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.MessageFormat] = "invalid"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Theory]
    [InlineData("json")]
    [InlineData("string")]
    [InlineData("bytes")]
    public void Start_WithValidMessageFormat_Succeeds(string format)
    {
        var connector = new GeneratorSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic",
            [GeneratorConnectorConfig.MessageFormat] = format
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new GeneratorSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal("test-topic", taskConfigs[0][GeneratorConnectorConfig.Topic]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new GeneratorSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GeneratorConnectorConfig.Topic] = "test-topic"
        };
        connector.Start(config);

        var exception = Record.Exception(() =>
        {
            connector.Stop();
            connector.Stop();
        });

        Assert.Null(exception);
    }
}
