namespace Kuestenlogik.Surgewave.Connector.Flink.Tests;

/// <summary>
/// Configuration contract of the Flink source and sink connectors: both tasks read the base URL
/// and their topic with an indexer, so a missing value has to be rejected here.
/// </summary>
public class FlinkConnectorTests
{
    [Fact]
    public void SinkConnector_Start_RejectsAMissingBaseUrl()
    {
        using var connector = new FlinkSinkConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [FlinkConnectorConfig.Topics] = "flink-commands"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(FlinkConnectorConfig.BaseUrl, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_Start_RejectsAnEmptyTopicList()
    {
        using var connector = new FlinkSinkConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [FlinkConnectorConfig.BaseUrl] = "http://localhost:8081",
            [FlinkConnectorConfig.Topics] = string.Empty
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(FlinkConnectorConfig.Topics, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_RejectsAMissingOutputTopic()
    {
        using var connector = new FlinkSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [FlinkConnectorConfig.BaseUrl] = "http://localhost:8081"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(FlinkConnectorConfig.OutputTopic, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_TaskConfigs_ReturnsOneSnapshotOfTheStartConfig()
    {
        using var connector = new FlinkSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [FlinkConnectorConfig.BaseUrl] = "http://localhost:8081",
            [FlinkConnectorConfig.OutputTopic] = "flink-metrics"
        };
        connector.Start(config);

        // One cluster means one poller, whatever maxTasks says.
        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("flink-metrics", taskConfig[FlinkConnectorConfig.OutputTopic]);

        config[FlinkConnectorConfig.OutputTopic] = "changed-after-start";
        Assert.Equal("flink-metrics", taskConfig[FlinkConnectorConfig.OutputTopic]);
    }
}
