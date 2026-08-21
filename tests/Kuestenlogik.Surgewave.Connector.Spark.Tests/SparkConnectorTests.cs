namespace Kuestenlogik.Surgewave.Connector.Spark.Tests;

/// <summary>
/// Tests for the Spark connectors. Both talk to two different clusters' APIs (the Spark
/// master and Livy) and need at least one of them, so the "either one will do" rule is the
/// part worth pinning down.
/// </summary>
public class SparkConnectorTests
{
    private const string SparkUrl = "http://spark.invalid:8080";
    private const string LivyUrl = "http://livy.invalid:8998";

    [Fact]
    public void SinkConnector_RefusesToStartWithoutAnyClusterUrl()
    {
        using var connector = new SparkSinkConnector();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(
            new Dictionary<string, string> { [SparkConnectorConfig.Topics] = "spark-commands" }));

        Assert.Contains(SparkConnectorConfig.LivyUrl, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_RefusesToStartWithoutTopics()
    {
        using var connector = new SparkSinkConnector();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(
            new Dictionary<string, string> { [SparkConnectorConfig.LivyUrl] = LivyUrl }));

        Assert.Contains(SparkConnectorConfig.Topics, ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(SparkConnectorConfig.BaseUrl)]
    [InlineData(SparkConnectorConfig.LivyUrl)]
    public void SinkConnector_StartsWithEitherClusterUrlOnItsOwn(string urlKey)
    {
        using var connector = new SparkSinkConnector();

        // Livy-only and master-only deployments are both normal; requiring both would rule
        // out most real clusters.
        connector.Start(new Dictionary<string, string>
        {
            [SparkConnectorConfig.Topics] = "spark-commands",
            [urlKey] = SparkUrl
        });

        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal(SparkUrl, taskConfig[urlKey]);
        Assert.Equal(typeof(SparkSinkTask), connector.TaskClass);
    }

    [Fact]
    public void SinkConnector_TreatsAnEmptyUrlAsNoUrl()
    {
        using var connector = new SparkSinkConnector();

        // An unset editor field arrives as "" rather than as a missing key.
        Assert.Throws<ArgumentException>(() => connector.Start(new Dictionary<string, string>
        {
            [SparkConnectorConfig.Topics] = "spark-commands",
            [SparkConnectorConfig.BaseUrl] = "",
            [SparkConnectorConfig.LivyUrl] = ""
        }));
    }

    [Fact]
    public void SourceConnector_RefusesToStartWithoutAnyClusterUrl()
    {
        using var connector = new SparkSourceConnector();

        Assert.Throws<ArgumentException>(() => connector.Start(
            new Dictionary<string, string> { [SparkConnectorConfig.OutputTopic] = "spark-metrics" }));
    }

    [Fact]
    public void SourceConnector_RefusesToStartWithoutAnOutputTopic()
    {
        using var connector = new SparkSourceConnector();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(
            new Dictionary<string, string> { [SparkConnectorConfig.LivyUrl] = LivyUrl }));

        Assert.Contains(SparkConnectorConfig.OutputTopic, ex.Message, StringComparison.Ordinal);
    }
}
