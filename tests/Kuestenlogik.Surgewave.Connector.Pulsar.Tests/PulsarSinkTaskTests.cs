namespace Kuestenlogik.Surgewave.Connector.Pulsar.Tests;

/// <summary>
/// Covers the topic routing the sink applies before it hands a record to Pulsar: the
/// '${surgewave.topic}' placeholder, the optional mapping prefix and the qualification of bare
/// topic names into 'persistent://public/default/...'.
/// </summary>
public class PulsarSinkTaskTests
{
    [Theory]
    [InlineData("${surgewave.topic}", "orders", "persistent://public/default/orders")]
    [InlineData("persistent://acme/eu/${surgewave.topic}", "orders", "persistent://acme/eu/orders")]
    [InlineData("mirror", "orders", "persistent://public/default/mirror")]
    [InlineData("non-persistent://public/default/mirror", "orders", "non-persistent://public/default/mirror")]
    public void GetPulsarTopic_QualifiesBareTopicNames(string template, string surgewaveTopic, string expected)
    {
        using var task = new PulsarSinkTask();

        var config = SinkConfig();
        config[PulsarConnectorConfig.Topic] = template;
        task.Configure(config);

        Assert.Equal(expected, task.GetPulsarTopic(surgewaveTopic));
    }

    [Fact]
    public void GetPulsarTopic_AppliesTheMappingPrefix_OnlyWhenMappingIsEnabled()
    {
        var config = SinkConfig();
        config[PulsarConnectorConfig.Topic] = "${surgewave.topic}";
        config[PulsarConnectorConfig.TopicMappingPrefix] = "eu-";

        using var disabled = new PulsarSinkTask();
        disabled.Configure(config);
        Assert.Equal("persistent://public/default/orders", disabled.GetPulsarTopic("orders"));

        using var enabled = new PulsarSinkTask();
        config[PulsarConnectorConfig.TopicMappingEnabled] = "true";
        enabled.Configure(config);

        // The prefix is applied to the mapped name before it is qualified.
        Assert.Equal("persistent://public/default/eu-orders", enabled.GetPulsarTopic("orders"));
    }

    private static Dictionary<string, string> SinkConfig() => new(StringComparer.Ordinal)
    {
        [PulsarConnectorConfig.ServiceUrl] = "pulsar://localhost:6650",
        [PulsarConnectorConfig.Topic] = "${surgewave.topic}",
        [PulsarConnectorConfig.Topics] = "orders"
    };
}
