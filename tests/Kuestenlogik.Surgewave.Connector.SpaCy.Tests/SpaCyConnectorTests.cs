namespace Kuestenlogik.Surgewave.Connector.SpaCy.Tests;

/// <summary>
/// Tests for <see cref="SpaCySinkConnector"/>: an NLP node that starts without a server or an
/// output topic only reveals the gap once records are already flowing through it.
/// </summary>
public class SpaCyConnectorTests
{
    [Theory]
    [InlineData(SpaCyConnectorConfig.Topics)]
    [InlineData(SpaCyConnectorConfig.OutputTopic)]
    [InlineData(SpaCyConnectorConfig.ServerUrl)]
    public void SinkConnector_RefusesToStartWithoutARequiredKey(string missingKey)
    {
        using var connector = new SpaCySinkConnector();

        var config = SinkConfig();
        config.Remove(missingKey);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(missingKey, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_HandsTheWholeConfigurationToItsSingleTask()
    {
        using var connector = new SpaCySinkConnector();
        var config = SinkConfig();
        config[SpaCyConnectorConfig.Operations] = "ner,lemma";
        connector.Start(config);

        var taskConfig = Assert.Single(connector.TaskConfigs(4));

        // The task reads operations, model and text field itself - the connector validates
        // none of them, so dropping any of them here loses them silently.
        Assert.Equal("ner,lemma", taskConfig[SpaCyConnectorConfig.Operations]);
        Assert.Equal(config.Count, taskConfig.Count);
        Assert.Equal(typeof(SpaCySinkTask), connector.TaskClass);
    }

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [SpaCyConnectorConfig.Topics] = "documents",
        [SpaCyConnectorConfig.OutputTopic] = "documents-nlp",
        [SpaCyConnectorConfig.ServerUrl] = "http://spacy.invalid:8080"
    };
}
