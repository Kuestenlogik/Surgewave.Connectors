namespace Kuestenlogik.Surgewave.Connector.Akka;

/// <summary>
/// Configuration constants for Akka.NET connectors.
/// </summary>
public static class AkkaConnectorConfig
{
    // Actor system configs
    public const string ActorSystemNameConfig = "akka.system.name";
    public const string ActorSystemConfigConfig = "akka.system.config";
    public const string ActorPathConfig = "akka.actor.path";
    public const string RemoteAddressConfig = "akka.remote.address";
    public const string ModeConfig = "akka.mode"; // actor, cluster, streams

    // Cluster Client configs
    public const string ClusterSeedNodesConfig = "akka.cluster.seed.nodes";
    public const string ClusterClientReceptionist = "akka.cluster.receptionist.path";
    public const string ClusterPublishTopicConfig = "akka.cluster.publish.topic";
    public const string ClusterSubscribeTopicConfig = "akka.cluster.subscribe.topic";

    // Streams configs
    public const string StreamBufferSizeConfig = "akka.stream.buffer.size";
    public const string StreamOverflowStrategyConfig = "akka.stream.overflow.strategy"; // dropHead, dropTail, dropBuffer, backpressure, fail
    public const string StreamParallelismConfig = "akka.stream.parallelism";

    // Source configs
    public const string TopicPatternConfig = "akka.topic.pattern";
    public const string PollTimeoutMsConfig = "akka.poll.timeout.ms";
    public const string MaxMessagesPerPollConfig = "akka.max.messages.per.poll";
    public const string IncludeMetadataConfig = "akka.include.metadata";
    public const string MessageTypeConfig = "akka.message.type";

    // Sink configs
    public const string TopicsConfig = "topics";
    public const string AskTimeoutMsConfig = "akka.ask.timeout.ms";
    public const string TellOnlyConfig = "akka.tell.only";
    public const string BatchSizeConfig = "akka.batch.size";
    public const string MaxRetryCountConfig = "akka.max.retry.count";
    public const string RetryDelayMsConfig = "akka.retry.delay.ms";

    // Default values
    public const string DefaultActorSystemName = "surgewave-connect";
    public const string DefaultMode = "actor";
    public const string DefaultTopicPattern = "akka.${path}";
    public const long DefaultPollTimeoutMs = 1000;
    public const int DefaultMaxMessagesPerPoll = 100;
    public const long DefaultAskTimeoutMs = 5000;
    public const int DefaultBatchSize = 32;
    public const int DefaultMaxRetryCount = 3;
    public const long DefaultRetryDelayMs = 1000;
    public const int DefaultStreamBufferSize = 1024;
    public const string DefaultOverflowStrategy = "backpressure";
    public const int DefaultStreamParallelism = 4;
    public const string DefaultReceptionistPath = "/system/receptionist";

    // Header names
    public const string HeaderActorPath = "akka.actor.path";
    public const string HeaderSenderPath = "akka.sender.path";
    public const string HeaderMessageType = "akka.message.type";
    public const string HeaderTimestamp = "akka.timestamp";
    public const string HeaderClusterTopic = "akka.cluster.topic";

    // Offset tracking
    public const string OffsetSequence = "sequence";
    public const string OffsetActorPath = "actor_path";
    public const string OffsetClusterTopic = "cluster_topic";
}
