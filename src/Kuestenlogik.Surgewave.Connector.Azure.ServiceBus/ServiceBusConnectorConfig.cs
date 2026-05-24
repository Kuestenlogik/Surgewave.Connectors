namespace Kuestenlogik.Surgewave.Connector.Azure.ServiceBus;

/// <summary>
/// Configuration constants for Azure Service Bus connectors.
/// </summary>
internal static class ServiceBusConnectorConfig
{
    // Connection configs
    public const string ConnectionStringConfig = "azure.servicebus.connection.string";
    public const string NamespaceConfig = "azure.servicebus.namespace";

    // Queue/Topic configs
    public const string QueueNameConfig = "azure.servicebus.queue.name";
    public const string TopicNameConfig = "azure.servicebus.topic.name";
    public const string SubscriptionNameConfig = "azure.servicebus.subscription.name";

    // Surgewave configs
    public const string SurgewaveTopicConfig = "surgewave.topic";
    public const string TopicsConfig = "topics";

    // Receive configs
    public const string ReceiveModeConfig = "azure.servicebus.receive.mode";
    public const string PrefetchCountConfig = "azure.servicebus.prefetch.count";
    public const string MaxConcurrentCallsConfig = "azure.servicebus.max.concurrent.calls";
    public const string MaxMessagesConfig = "azure.servicebus.max.messages";

    // Send configs
    public const string SessionIdFieldConfig = "azure.servicebus.session.id.field";
    public const string PartitionKeyFieldConfig = "azure.servicebus.partition.key.field";
    public const string BatchSizeConfig = "azure.servicebus.batch.size";

    // Header mapping
    public const string HeaderPrefixConfig = "azure.servicebus.header.prefix";
    public const string IncludeMetadataConfig = "azure.servicebus.include.metadata";

    // Default values
    public const string DefaultReceiveMode = "PeekLock"; // or "ReceiveAndDelete"
    public const int DefaultPrefetchCount = 0;
    public const int DefaultMaxConcurrentCalls = 1;
    public const int DefaultMaxMessages = 10;
    public const int DefaultBatchSize = 100;
    public const string DefaultHeaderPrefix = "servicebus.";
    public const bool DefaultIncludeMetadata = true;
}
