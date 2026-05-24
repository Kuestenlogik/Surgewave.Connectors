namespace Kuestenlogik.Surgewave.Connector.Akka.Tests;

public class AkkaConnectorConfigTests
{
    [Fact]
    public void ActorSystemNameConfig_HasExpectedValue()
    {
        Assert.Equal("akka.system.name", AkkaConnectorConfig.ActorSystemNameConfig);
    }

    [Fact]
    public void ActorSystemConfigConfig_HasExpectedValue()
    {
        Assert.Equal("akka.system.config", AkkaConnectorConfig.ActorSystemConfigConfig);
    }

    [Fact]
    public void ActorPathConfig_HasExpectedValue()
    {
        Assert.Equal("akka.actor.path", AkkaConnectorConfig.ActorPathConfig);
    }

    [Fact]
    public void RemoteAddressConfig_HasExpectedValue()
    {
        Assert.Equal("akka.remote.address", AkkaConnectorConfig.RemoteAddressConfig);
    }

    [Fact]
    public void ModeConfig_HasExpectedValue()
    {
        Assert.Equal("akka.mode", AkkaConnectorConfig.ModeConfig);
    }

    [Fact]
    public void ClusterSeedNodesConfig_HasExpectedValue()
    {
        Assert.Equal("akka.cluster.seed.nodes", AkkaConnectorConfig.ClusterSeedNodesConfig);
    }

    [Fact]
    public void ClusterPublishTopicConfig_HasExpectedValue()
    {
        Assert.Equal("akka.cluster.publish.topic", AkkaConnectorConfig.ClusterPublishTopicConfig);
    }

    [Fact]
    public void ClusterSubscribeTopicConfig_HasExpectedValue()
    {
        Assert.Equal("akka.cluster.subscribe.topic", AkkaConnectorConfig.ClusterSubscribeTopicConfig);
    }

    [Fact]
    public void StreamBufferSizeConfig_HasExpectedValue()
    {
        Assert.Equal("akka.stream.buffer.size", AkkaConnectorConfig.StreamBufferSizeConfig);
    }

    [Fact]
    public void StreamOverflowStrategyConfig_HasExpectedValue()
    {
        Assert.Equal("akka.stream.overflow.strategy", AkkaConnectorConfig.StreamOverflowStrategyConfig);
    }

    [Fact]
    public void StreamParallelismConfig_HasExpectedValue()
    {
        Assert.Equal("akka.stream.parallelism", AkkaConnectorConfig.StreamParallelismConfig);
    }

    [Fact]
    public void TopicPatternConfig_HasExpectedValue()
    {
        Assert.Equal("akka.topic.pattern", AkkaConnectorConfig.TopicPatternConfig);
    }

    [Fact]
    public void PollTimeoutMsConfig_HasExpectedValue()
    {
        Assert.Equal("akka.poll.timeout.ms", AkkaConnectorConfig.PollTimeoutMsConfig);
    }

    [Fact]
    public void MaxMessagesPerPollConfig_HasExpectedValue()
    {
        Assert.Equal("akka.max.messages.per.poll", AkkaConnectorConfig.MaxMessagesPerPollConfig);
    }

    [Fact]
    public void DefaultActorSystemName_HasExpectedValue()
    {
        Assert.Equal("surgewave-connect", AkkaConnectorConfig.DefaultActorSystemName);
    }

    [Fact]
    public void DefaultMode_HasExpectedValue()
    {
        Assert.Equal("actor", AkkaConnectorConfig.DefaultMode);
    }

    [Fact]
    public void DefaultTopicPattern_HasExpectedValue()
    {
        Assert.Equal("akka.${path}", AkkaConnectorConfig.DefaultTopicPattern);
    }

    [Fact]
    public void DefaultPollTimeoutMs_HasExpectedValue()
    {
        Assert.Equal(1000L, AkkaConnectorConfig.DefaultPollTimeoutMs);
    }

    [Fact]
    public void DefaultMaxMessagesPerPoll_HasExpectedValue()
    {
        Assert.Equal(100, AkkaConnectorConfig.DefaultMaxMessagesPerPoll);
    }

    [Fact]
    public void DefaultAskTimeoutMs_HasExpectedValue()
    {
        Assert.Equal(5000L, AkkaConnectorConfig.DefaultAskTimeoutMs);
    }

    [Fact]
    public void DefaultBatchSize_HasExpectedValue()
    {
        Assert.Equal(32, AkkaConnectorConfig.DefaultBatchSize);
    }

    [Fact]
    public void DefaultMaxRetryCount_HasExpectedValue()
    {
        Assert.Equal(3, AkkaConnectorConfig.DefaultMaxRetryCount);
    }

    [Fact]
    public void DefaultRetryDelayMs_HasExpectedValue()
    {
        Assert.Equal(1000L, AkkaConnectorConfig.DefaultRetryDelayMs);
    }

    [Fact]
    public void DefaultStreamBufferSize_HasExpectedValue()
    {
        Assert.Equal(1024, AkkaConnectorConfig.DefaultStreamBufferSize);
    }

    [Fact]
    public void DefaultOverflowStrategy_HasExpectedValue()
    {
        Assert.Equal("backpressure", AkkaConnectorConfig.DefaultOverflowStrategy);
    }

    [Fact]
    public void DefaultStreamParallelism_HasExpectedValue()
    {
        Assert.Equal(4, AkkaConnectorConfig.DefaultStreamParallelism);
    }

    [Fact]
    public void HeaderActorPath_HasExpectedValue()
    {
        Assert.Equal("akka.actor.path", AkkaConnectorConfig.HeaderActorPath);
    }

    [Fact]
    public void HeaderSenderPath_HasExpectedValue()
    {
        Assert.Equal("akka.sender.path", AkkaConnectorConfig.HeaderSenderPath);
    }

    [Fact]
    public void HeaderMessageType_HasExpectedValue()
    {
        Assert.Equal("akka.message.type", AkkaConnectorConfig.HeaderMessageType);
    }

    [Fact]
    public void HeaderTimestamp_HasExpectedValue()
    {
        Assert.Equal("akka.timestamp", AkkaConnectorConfig.HeaderTimestamp);
    }

    [Fact]
    public void OffsetSequence_HasExpectedValue()
    {
        Assert.Equal("sequence", AkkaConnectorConfig.OffsetSequence);
    }

    [Fact]
    public void OffsetActorPath_HasExpectedValue()
    {
        Assert.Equal("actor_path", AkkaConnectorConfig.OffsetActorPath);
    }
}
