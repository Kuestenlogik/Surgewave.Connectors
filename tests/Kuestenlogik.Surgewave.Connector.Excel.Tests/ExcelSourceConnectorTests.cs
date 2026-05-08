using Kuestenlogik.Surgewave.Connector.Excel;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Excel.Tests;

public class ExcelSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new ExcelSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsExcelSourceTask()
    {
        var connector = new ExcelSourceConnector();
        Assert.Equal(typeof(ExcelSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new ExcelSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == ExcelConnectorConfig.FilePath);
        Assert.Contains(connector.Config.Keys, k => k.Name == ExcelConnectorConfig.Topic);
        Assert.Contains(connector.Config.Keys, k => k.Name == ExcelConnectorConfig.SheetName);
        Assert.Contains(connector.Config.Keys, k => k.Name == ExcelConnectorConfig.HasHeader);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        var connector = new ExcelSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.FilePath] = "test.xlsx",
            [ExcelConnectorConfig.Topic] = "excel-topic"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingFilePath_ThrowsArgumentException()
    {
        var connector = new ExcelSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.Topic] = "excel-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingTopic_ThrowsArgumentException()
    {
        var connector = new ExcelSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.FilePath] = "test.xlsx"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TaskConfigs_SingleFile_ReturnsSingleConfig()
    {
        var connector = new ExcelSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.FilePath] = "test.xlsx",
            [ExcelConnectorConfig.Topic] = "excel-topic"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal("test.xlsx", taskConfigs[0][ExcelConnectorConfig.FilePath]);
    }

    [Fact]
    public void TaskConfigs_MultipleFiles_DistributesAcrossTasks()
    {
        var connector = new ExcelSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.FilePath] = "file1.xlsx;file2.xlsx;file3.xlsx;file4.xlsx",
            [ExcelConnectorConfig.Topic] = "excel-topic"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(2);

        Assert.Equal(2, taskConfigs.Count);
        // Files are distributed round-robin
        Assert.Contains("file1.xlsx", taskConfigs[0][ExcelConnectorConfig.FilePath]);
        Assert.Contains("file2.xlsx", taskConfigs[1][ExcelConnectorConfig.FilePath]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new ExcelSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.FilePath] = "test.xlsx",
            [ExcelConnectorConfig.Topic] = "excel-topic"
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
