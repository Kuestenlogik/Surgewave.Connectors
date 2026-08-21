namespace Kuestenlogik.Surgewave.Connector.FileStream.Tests;

/// <summary>
/// Configuration contract of the file source and sink connectors: the tasks read every value
/// with an indexer, so a missing key has to be rejected here with a readable message.
/// </summary>
public class FileStreamConnectorTests
{
    [Fact]
    public void SourceConnector_Start_RejectsAMissingFile()
    {
        using var connector = new FileStreamSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal) { ["topic"] = "lines" };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("file", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_RejectsAMissingTopic()
    {
        using var connector = new FileStreamSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal) { ["file"] = "input.txt" };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topic", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_TaskConfigs_CarriesFileAndTopicToASingleTask()
    {
        using var connector = new FileStreamSourceConnector();
        connector.Start(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["file"] = "input.txt",
            ["topic"] = "lines"
        });

        // One file means one reader - more tasks would read the same bytes twice.
        var taskConfig = Assert.Single(connector.TaskConfigs(8));

        Assert.Equal("input.txt", taskConfig["file"]);
        Assert.Equal("lines", taskConfig["topic"]);
    }

    [Fact]
    public void SinkConnector_Start_RejectsMissingTopics()
    {
        using var connector = new FileStreamSinkConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal) { ["file"] = "out.txt" };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message, StringComparison.Ordinal);
    }
}
