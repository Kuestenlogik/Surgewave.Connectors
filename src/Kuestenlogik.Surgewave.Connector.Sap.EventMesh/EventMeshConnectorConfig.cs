namespace Kuestenlogik.Surgewave.Connector.Sap.EventMesh;

/// <summary>
/// Configuration constants for SAP Event Mesh connector.
/// </summary>
public static class EventMeshConnectorConfig
{
    // Connection settings
    public const string ServiceUrl = "eventmesh.service.url";  // Messaging endpoint
    public const string TokenUrl = "eventmesh.token.url";  // OAuth token endpoint
    public const string ClientId = "eventmesh.client.id";
    public const string ClientSecret = "eventmesh.client.secret";
    public const string Protocol = "eventmesh.protocol";  // amqp, mqtt, rest

    // Queue/Topic settings
    public const string QueueName = "eventmesh.queue.name";
    public const string TopicPattern = "eventmesh.topic.pattern";  // Topic subscription pattern
    public const string Namespace = "eventmesh.namespace";

    // Source settings
    public const string Topic = "topic";  // Surgewave topic to produce to
    public const string PollIntervalMs = "poll.interval.ms";
    public const string MaxMessages = "eventmesh.max.messages";  // Per poll
    public const string AckMode = "eventmesh.ack.mode";  // auto, manual
    public const string PrefetchCount = "eventmesh.prefetch.count";

    // Sink settings
    public const string Topics = "topics";  // Surgewave topics to consume from
    public const string TargetTopic = "eventmesh.target.topic";  // Event Mesh topic to publish to
    public const string ContentType = "eventmesh.content.type";
    public const string CloudEventSource = "eventmesh.cloudevent.source";
    public const string CloudEventType = "eventmesh.cloudevent.type";
    public const string BatchSize = "eventmesh.batch.size";

    // Defaults
    public const int DefaultPollIntervalMs = 100;
    public const int DefaultMaxMessages = 100;
    public const int DefaultPrefetchCount = 100;
    public const int DefaultBatchSize = 100;
    public const string DefaultProtocol = "rest";
    public const string DefaultAckMode = "manual";
    public const string DefaultContentType = "application/json";
}
