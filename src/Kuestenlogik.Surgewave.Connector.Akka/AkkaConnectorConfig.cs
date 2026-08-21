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
    public const string DefaultTopicPattern = "akka.${path}";
    public const long DefaultPollTimeoutMs = 1000;
    public const int DefaultMaxMessagesPerPoll = 100;
    public const long DefaultAskTimeoutMs = 5000;
    public const int DefaultBatchSize = 32;
    public const int DefaultMaxRetryCount = 3;
    public const long DefaultRetryDelayMs = 1000;

    // Header names
    public const string HeaderActorPath = "akka.actor.path";
    public const string HeaderSenderPath = "akka.sender.path";
    public const string HeaderMessageType = "akka.message.type";
    public const string HeaderTimestamp = "akka.timestamp";

    // Offset tracking
    public const string OffsetSequence = "sequence";
    public const string OffsetActorPath = "actor_path";
}
