namespace Kuestenlogik.Surgewave.Connector.Amqp;

/// <summary>
/// Configuration constants for AMQP connector.
/// </summary>
public static class AmqpConnectorConfig
{
    // Connection settings
    public const string Uri = "amqp.uri";  // amqp://user:pass@host:port/vhost
    public const string Host = "amqp.host";
    public const string Port = "amqp.port";
    public const string VirtualHost = "amqp.vhost";
    public const string Username = "amqp.username";
    public const string Password = "amqp.password";
    public const string UseSsl = "amqp.ssl";
    public const string RequestedHeartbeat = "amqp.heartbeat.seconds";

    // Source settings
    public const string Topic = "topic";
    public const string SourceQueue = "amqp.source.queue";
    public const string SourceExchange = "amqp.source.exchange";
    public const string SourceRoutingKey = "amqp.source.routing.key";
    public const string AutoAck = "amqp.auto.ack";
    public const string PrefetchCount = "amqp.prefetch.count";
    public const string DeclareQueue = "amqp.declare.queue";
    public const string QueueDurable = "amqp.queue.durable";
    public const string QueueExclusive = "amqp.queue.exclusive";
    public const string QueueAutoDelete = "amqp.queue.auto.delete";

    // Sink settings
    public const string Topics = "topics";
    public const string TargetExchange = "amqp.target.exchange";
    public const string TargetRoutingKey = "amqp.target.routing.key";
    public const string ExchangeType = "amqp.exchange.type";  // direct, fanout, topic, headers
    public const string DeclareExchange = "amqp.declare.exchange";
    public const string ExchangeDurable = "amqp.exchange.durable";
    public const string Persistent = "amqp.persistent";
    public const string BatchSize = "amqp.batch.size";

    // Defaults
    public const int DefaultPort = 5672;
    public const int DefaultSslPort = 5671;
    public const string DefaultVirtualHost = "/";
    public const int DefaultHeartbeatSeconds = 60;
    public const int DefaultPrefetchCount = 100;
    public const int DefaultBatchSize = 100;
    public const string DefaultExchangeType = "direct";
}
