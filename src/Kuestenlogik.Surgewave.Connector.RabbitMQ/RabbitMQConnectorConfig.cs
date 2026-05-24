namespace Kuestenlogik.Surgewave.Connector.RabbitMQ;

/// <summary>
/// Configuration constants for RabbitMQ connector.
/// </summary>
public static class RabbitMQConnectorConfig
{
    // Connection settings
    public const string Host = "rabbitmq.host";
    public const string Port = "rabbitmq.port";
    public const string VirtualHost = "rabbitmq.virtual.host";
    public const string Username = "rabbitmq.username";
    public const string Password = "rabbitmq.password";
    public const string ConnectionName = "rabbitmq.connection.name";

    // TLS settings
    public const string TlsEnabled = "rabbitmq.tls.enabled";
    public const string TlsCertFile = "rabbitmq.tls.cert.file";
    public const string TlsKeyFile = "rabbitmq.tls.key.file";
    public const string TlsCaFile = "rabbitmq.tls.ca.file";

    // Source settings
    public const string Queue = "rabbitmq.queue";
    public const string QueueDurable = "rabbitmq.queue.durable";
    public const string QueueExclusive = "rabbitmq.queue.exclusive";
    public const string QueueAutoDelete = "rabbitmq.queue.auto.delete";
    public const string Topic = "topic";
    public const string PrefetchCount = "rabbitmq.prefetch.count";
    public const string AutoAck = "rabbitmq.auto.ack";
    public const string BatchSize = "rabbitmq.batch.size";
    public const string PollTimeoutMs = "rabbitmq.poll.timeout.ms";

    // Sink settings
    public const string Topics = "topics";
    public const string Exchange = "rabbitmq.exchange";
    public const string ExchangeType = "rabbitmq.exchange.type";
    public const string ExchangeDurable = "rabbitmq.exchange.durable";
    public const string ExchangeAutoDelete = "rabbitmq.exchange.auto.delete";
    public const string RoutingKeyTemplate = "rabbitmq.routing.key.template";
    public const string Persistent = "rabbitmq.persistent";
    public const string Mandatory = "rabbitmq.mandatory";

    // Dead letter settings
    public const string DeadLetterExchange = "rabbitmq.dead.letter.exchange";
    public const string DeadLetterRoutingKey = "rabbitmq.dead.letter.routing.key";

    // Message properties
    public const string ContentType = "rabbitmq.content.type";
    public const string ContentEncoding = "rabbitmq.content.encoding";
    public const string MessageTtlMs = "rabbitmq.message.ttl.ms";

    // Offset tracking keys
    public const string OffsetDeliveryTag = "delivery.tag";
    public const string OffsetTimestamp = "timestamp";

    // Defaults
    public const string DefaultHost = "localhost";
    public const int DefaultPort = 5672;
    public const string DefaultVirtualHost = "/";
    public const string DefaultUsername = "guest";
    public const string DefaultPassword = "guest";
    public const ushort DefaultPrefetchCount = 100;
    public const int DefaultBatchSize = 100;
    public const int DefaultPollTimeoutMs = 1000;
    public const string DefaultExchangeType = "direct";
    public const string DefaultRoutingKeyTemplate = "${topic}";
    public const string DefaultContentType = "application/octet-stream";
}
