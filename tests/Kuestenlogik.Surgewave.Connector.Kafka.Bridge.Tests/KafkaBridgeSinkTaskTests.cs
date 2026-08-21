using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Kafka.Bridge.Tests;

/// <summary>
/// Surgewave-to-Kafka mapping rules: destination topic resolution and the header envelope that
/// carries the Surgewave coordinates over to Kafka.
/// </summary>
public class KafkaBridgeSinkTaskTests
{
    [Fact]
    public void MapTopic_WithoutOverride_KeepsTheSurgewaveTopicName()
    {
        Assert.Equal("orders", KafkaBridgeSinkTask.MapTopic(null, "orders", false, "", ""));
    }

    [Fact]
    public void MapTopic_SubstitutesSurgewaveTopicPlaceholder()
    {
        Assert.Equal("legacy-orders", KafkaBridgeSinkTask.MapTopic("legacy-${surgewave.topic}", "orders", false, "", ""));
    }

    [Fact]
    public void MapTopic_AppliesPrefixAndSuffix_WhenMappingEnabled()
    {
        Assert.Equal("kafka.orders.v2", KafkaBridgeSinkTask.MapTopic("", "orders", true, "kafka.", ".v2"));
    }

    [Fact]
    public void ConvertHeaders_CarriesSurgewaveCoordinatesAndOriginalHeaders()
    {
        var record = new SinkRecord
        {
            Topic = "orders",
            Partition = 2,
            Offset = 99,
            Value = Encoding.UTF8.GetBytes("{}"),
            Headers = new Dictionary<string, byte[]>
            {
                ["trace-id"] = Encoding.UTF8.GetBytes("abc")
            }
        };

        var headers = KafkaBridgeSinkTask.ConvertHeaders(record);

        Assert.Equal("orders", Encoding.UTF8.GetString(headers.GetLastBytes("surgewave.source.topic")));
        Assert.Equal("2", Encoding.UTF8.GetString(headers.GetLastBytes("surgewave.source.partition")));
        Assert.Equal("99", Encoding.UTF8.GetString(headers.GetLastBytes("surgewave.source.offset")));
        Assert.Equal("abc", Encoding.UTF8.GetString(headers.GetLastBytes("surgewave.header.trace-id")));
    }
}
