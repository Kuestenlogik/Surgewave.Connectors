using System.Text.Json;
using Kuestenlogik.Surgewave.Connector.Mirror.Models;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Tests.Models;

public class HeartbeatTests
{
    [Fact]
    public void Heartbeat_ShouldSerializeToJson()
    {
        var heartbeat = new Heartbeat
        {
            SourceCluster = "dc1",
            TargetCluster = "dc2",
            Timestamp = 1234567890123
        };

        var json = JsonSerializer.Serialize(heartbeat);
        var parsed = JsonDocument.Parse(json);

        Assert.Equal("dc1", parsed.RootElement.GetProperty("sourceCluster").GetString());
        Assert.Equal("dc2", parsed.RootElement.GetProperty("targetCluster").GetString());
        Assert.Equal(1234567890123, parsed.RootElement.GetProperty("timestamp").GetInt64());
    }

    [Fact]
    public void Heartbeat_ShouldDeserializeFromJson()
    {
        var json = """{"sourceCluster":"dc1","targetCluster":"dc2","timestamp":1234567890123}""";

        var heartbeat = JsonSerializer.Deserialize<Heartbeat>(json);

        Assert.NotNull(heartbeat);
        Assert.Equal("dc1", heartbeat.SourceCluster);
        Assert.Equal("dc2", heartbeat.TargetCluster);
        Assert.Equal(1234567890123, heartbeat.Timestamp);
    }

    [Fact]
    public void CheckpointRecord_ShouldSerializeToJson()
    {
        var checkpoint = new CheckpointRecord
        {
            ConsumerGroup = "my-app",
            Topic = "orders",
            Partition = 0,
            SourceOffset = 100,
            TargetOffset = 95,
            Metadata = "dc1->dc2",
            Timestamp = 1234567890123
        };

        var json = JsonSerializer.Serialize(checkpoint);
        var parsed = JsonDocument.Parse(json);

        Assert.Equal("my-app", parsed.RootElement.GetProperty("consumerGroup").GetString());
        Assert.Equal("orders", parsed.RootElement.GetProperty("topic").GetString());
        Assert.Equal(0, parsed.RootElement.GetProperty("partition").GetInt32());
        Assert.Equal(100, parsed.RootElement.GetProperty("sourceOffset").GetInt64());
        Assert.Equal(95, parsed.RootElement.GetProperty("targetOffset").GetInt64());
        Assert.Equal("dc1->dc2", parsed.RootElement.GetProperty("metadata").GetString());
    }

    [Fact]
    public void CheckpointRecord_ShouldDeserializeFromJson()
    {
        var json = """
        {
            "consumerGroup":"my-app",
            "topic":"orders",
            "partition":0,
            "sourceOffset":100,
            "targetOffset":95,
            "metadata":"dc1->dc2",
            "timestamp":1234567890123
        }
        """;

        var checkpoint = JsonSerializer.Deserialize<CheckpointRecord>(json);

        Assert.NotNull(checkpoint);
        Assert.Equal("my-app", checkpoint.ConsumerGroup);
        Assert.Equal("orders", checkpoint.Topic);
        Assert.Equal(0, checkpoint.Partition);
        Assert.Equal(100, checkpoint.SourceOffset);
        Assert.Equal(95, checkpoint.TargetOffset);
        Assert.Equal("dc1->dc2", checkpoint.Metadata);
    }

    [Fact]
    public void CheckpointRecord_MetadataShouldBeOptional()
    {
        var json = """
        {
            "consumerGroup":"my-app",
            "topic":"orders",
            "partition":0,
            "sourceOffset":100,
            "targetOffset":95,
            "timestamp":1234567890123
        }
        """;

        var checkpoint = JsonSerializer.Deserialize<CheckpointRecord>(json);

        Assert.NotNull(checkpoint);
        Assert.Null(checkpoint.Metadata);
    }

    [Fact]
    public void OffsetSyncRecord_ShouldSerializeToJson()
    {
        var offsetSync = new OffsetSyncRecord
        {
            Topic = "orders",
            Partition = 0,
            SourceOffset = 100,
            TargetOffset = 95,
            Timestamp = 1234567890123
        };

        var json = JsonSerializer.Serialize(offsetSync);
        var parsed = JsonDocument.Parse(json);

        Assert.Equal("orders", parsed.RootElement.GetProperty("topic").GetString());
        Assert.Equal(0, parsed.RootElement.GetProperty("partition").GetInt32());
        Assert.Equal(100, parsed.RootElement.GetProperty("sourceOffset").GetInt64());
        Assert.Equal(95, parsed.RootElement.GetProperty("targetOffset").GetInt64());
    }

    [Fact]
    public void OffsetSyncRecord_ShouldDeserializeFromJson()
    {
        var json = """
        {
            "topic":"orders",
            "partition":0,
            "sourceOffset":100,
            "targetOffset":95,
            "timestamp":1234567890123
        }
        """;

        var offsetSync = JsonSerializer.Deserialize<OffsetSyncRecord>(json);

        Assert.NotNull(offsetSync);
        Assert.Equal("orders", offsetSync.Topic);
        Assert.Equal(0, offsetSync.Partition);
        Assert.Equal(100, offsetSync.SourceOffset);
        Assert.Equal(95, offsetSync.TargetOffset);
    }
}
