namespace Kuestenlogik.Surgewave.Connector.Gcp.PubSub;

/// <summary>
/// Configuration constants for Google Cloud Pub/Sub connectors.
/// </summary>
internal static class PubSubConnectorConfig
{
    // Connection configs
    public const string ProjectIdConfig = "gcp.pubsub.project.id";
    public const string CredentialsJsonConfig = "gcp.pubsub.credentials.json";
    public const string CredentialsFileConfig = "gcp.pubsub.credentials.file";
    public const string EmulatorHostConfig = "gcp.pubsub.emulator.host";

    // Source configs (pull from subscription)
    public const string SubscriptionIdConfig = "gcp.pubsub.subscription.id";
    public const string MaxMessagesConfig = "gcp.pubsub.max.messages";
    public const string AckDeadlineSecondsConfig = "gcp.pubsub.ack.deadline.seconds";
    public const string SurgewaveTopicConfig = "surgewave.topic";
    public const string AutoAckConfig = "gcp.pubsub.auto.ack";

    // Sink configs (publish to topic)
    public const string PubSubTopicIdConfig = "gcp.pubsub.topic.id";
    public const string TopicsConfig = "topics";
    public const string OrderingKeyFieldConfig = "gcp.pubsub.ordering.key.field";
    public const string BatchSizeConfig = "gcp.pubsub.batch.size";
    public const string BatchDelayMsConfig = "gcp.pubsub.batch.delay.ms";

    // Header mapping
    public const string HeaderPrefixConfig = "gcp.pubsub.header.prefix";
    public const string IncludeMetadataConfig = "gcp.pubsub.include.metadata";

    // Default values
    public const int DefaultMaxMessages = 100;
    public const int DefaultAckDeadlineSeconds = 10;
    public const bool DefaultAutoAck = true;
    public const int DefaultBatchSize = 100;
    public const int DefaultBatchDelayMs = 100;
    public const string DefaultHeaderPrefix = "pubsub.";
    public const bool DefaultIncludeMetadata = true;
}
