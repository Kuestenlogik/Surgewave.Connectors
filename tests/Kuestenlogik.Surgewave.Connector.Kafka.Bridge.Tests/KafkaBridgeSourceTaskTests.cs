using System.Text;
using Confluent.Kafka;

namespace Kuestenlogik.Surgewave.Connector.Kafka.Bridge.Tests;

/// <summary>
/// Kafka-to-Surgewave mapping rules: destination topic resolution and the header envelope that
/// carries the Kafka coordinates over to Surgewave.
/// </summary>
public class KafkaBridgeSourceTaskTests
{
    [Fact]
    public void MapTopic_SubstitutesKafkaTopicPlaceholder()
    {
        Assert.Equal("mirror.orders", KafkaBridgeSourceTask.MapTopic("mirror.${kafka.topic}", "orders", false, "", ""));
    }

    [Fact]
    public void MapTopic_WithoutPlaceholder_FunnelsEverythingIntoOneTopic()
    {
        Assert.Equal("all-events", KafkaBridgeSourceTask.MapTopic("all-events", "orders", false, "", ""));
    }

    [Fact]
    public void MapTopic_AppliesPrefixAndSuffix_WhenMappingEnabled()
    {
        Assert.Equal("sw.orders.v1", KafkaBridgeSourceTask.MapTopic("${kafka.topic}", "orders", true, "sw.", ".v1"));
    }

    [Fact]
    public void MapTopic_IgnoresPrefixAndSuffix_WhenMappingDisabled()
    {
        Assert.Equal("orders", KafkaBridgeSourceTask.MapTopic("${kafka.topic}", "orders", false, "sw.", ".v1"));
    }

    [Fact]
    public void ConvertHeaders_AlwaysCarriesTheKafkaCoordinates()
    {
        var headers = KafkaBridgeSourceTask.ConvertHeaders(null, "orders", 3, 4711);

        Assert.Equal(3, headers.Count);
        Assert.Equal("orders", Encoding.UTF8.GetString(headers["kafka.source.topic"]));
        Assert.Equal("3", Encoding.UTF8.GetString(headers["kafka.source.partition"]));
        Assert.Equal("4711", Encoding.UTF8.GetString(headers["kafka.source.offset"]));
    }

    [Fact]
    public void ConvertHeaders_NamespacesTheOriginalKafkaHeaders()
    {
        var kafkaHeaders = new Headers
        {
            { "trace-id", Encoding.UTF8.GetBytes("abc") }
        };

        var headers = KafkaBridgeSourceTask.ConvertHeaders(kafkaHeaders, "orders", 0, 0);

        Assert.Equal("abc", Encoding.UTF8.GetString(headers["kafka.header.trace-id"]));
        Assert.Equal(4, headers.Count);
    }
}
