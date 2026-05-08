using Kuestenlogik.Surgewave.Connector.Excel;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Excel.Tests;

public class ExcelSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new ExcelSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsExcelSinkTask()
    {
        var connector = new ExcelSinkConnector();
        Assert.Equal(typeof(ExcelSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new ExcelSinkConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == ExcelConnectorConfig.Topics);
        Assert.Contains(connector.Config.Keys, k => k.Name == ExcelConnectorConfig.OutputPath);
        Assert.Contains(connector.Config.Keys, k => k.Name == ExcelConnectorConfig.OutputMode);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        var connector = new ExcelSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.Topics] = "test-topic",
            [ExcelConnectorConfig.OutputPath] = "output.xlsx"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingTopics_ThrowsArgumentException()
    {
        var connector = new ExcelSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.OutputPath] = "output.xlsx"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingOutputPath_ThrowsArgumentException()
    {
        var connector = new ExcelSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.Topics] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithInvalidOutputMode_ThrowsArgumentException()
    {
        var connector = new ExcelSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.Topics] = "test-topic",
            [ExcelConnectorConfig.OutputPath] = "output.xlsx",
            [ExcelConnectorConfig.OutputMode] = "invalid"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Theory]
    [InlineData("append")]
    [InlineData("overwrite")]
    [InlineData("rolling")]
    public void Start_WithValidOutputMode_Succeeds(string mode)
    {
        var connector = new ExcelSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.Topics] = "test-topic",
            [ExcelConnectorConfig.OutputPath] = "output.xlsx",
            [ExcelConnectorConfig.OutputMode] = mode
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new ExcelSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.Topics] = "test-topic",
            [ExcelConnectorConfig.OutputPath] = "output.xlsx"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs); // Single task for file writing
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new ExcelSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.Topics] = "test-topic",
            [ExcelConnectorConfig.OutputPath] = "output.xlsx"
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
