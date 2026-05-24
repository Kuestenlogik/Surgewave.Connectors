using System.Text;
using RabbitMQ.Client;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Amqp;

/// <summary>
/// Task that writes messages to AMQP exchanges.
/// </summary>
public sealed class AmqpSinkTask : SinkTask
{
    private IConnection? _connection;
    private IChannel? _channel;
    private string _exchange = null!;
    private string _routingKey = null!;
    private bool _persistent;
    private int _batchSize;
    private int _pendingConfirms;

    public override string Version => "1.0.0";

    public override async void Start(IDictionary<string, string> config)
    {
        _exchange = config.GetValueOrDefault(AmqpConnectorConfig.TargetExchange, "")!;
        _routingKey = config.GetValueOrDefault(AmqpConnectorConfig.TargetRoutingKey, "")!;
        _persistent = config.GetValueOrDefault(AmqpConnectorConfig.Persistent, "true") == "true";
        _batchSize = int.Parse(config.GetValueOrDefault(AmqpConnectorConfig.BatchSize,
            AmqpConnectorConfig.DefaultBatchSize.ToString())!);

        var factory = CreateConnectionFactory(config);
        _connection = await factory.CreateConnectionAsync();
        // Publisher confirms are now enabled via CreateChannelOptions in RabbitMQ.Client 7.0
        var channelOptions = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
        _channel = await _connection.CreateChannelAsync(channelOptions);

        // Declare exchange if requested
        var declareExchange = config.GetValueOrDefault(AmqpConnectorConfig.DeclareExchange, "true") == "true";
        if (declareExchange && !string.IsNullOrEmpty(_exchange))
        {
            var exchangeType = config.GetValueOrDefault(AmqpConnectorConfig.ExchangeType,
                AmqpConnectorConfig.DefaultExchangeType)!;
            var durable = config.GetValueOrDefault(AmqpConnectorConfig.ExchangeDurable, "true") == "true";

            await _channel.ExchangeDeclareAsync(_exchange, exchangeType, durable);
        }
    }

    private ConnectionFactory CreateConnectionFactory(IDictionary<string, string> config)
    {
        var uri = config.GetValueOrDefault(AmqpConnectorConfig.Uri, "");

        if (!string.IsNullOrWhiteSpace(uri))
        {
            return new ConnectionFactory { Uri = new Uri(uri) };
        }

        var host = config.GetValueOrDefault(AmqpConnectorConfig.Host, "localhost")!;
        var useSsl = config.GetValueOrDefault(AmqpConnectorConfig.UseSsl, "false") == "true";
        var port = int.Parse(config.GetValueOrDefault(AmqpConnectorConfig.Port,
            (useSsl ? AmqpConnectorConfig.DefaultSslPort : AmqpConnectorConfig.DefaultPort).ToString())!);
        var vhost = config.GetValueOrDefault(AmqpConnectorConfig.VirtualHost,
            AmqpConnectorConfig.DefaultVirtualHost)!;
        var username = config.GetValueOrDefault(AmqpConnectorConfig.Username, "guest")!;
        var password = config.GetValueOrDefault(AmqpConnectorConfig.Password, "guest")!;
        var heartbeat = int.Parse(config.GetValueOrDefault(AmqpConnectorConfig.RequestedHeartbeat,
            AmqpConnectorConfig.DefaultHeartbeatSeconds.ToString())!);

        var factory = new ConnectionFactory
        {
            HostName = host,
            Port = port,
            VirtualHost = vhost,
            UserName = username,
            Password = password,
            RequestedHeartbeat = TimeSpan.FromSeconds(heartbeat)
        };

        if (useSsl)
        {
            factory.Ssl = new SslOption { Enabled = true, ServerName = host };
        }

        return factory;
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                // Determine routing key from record or config
                var routingKey = _routingKey;
                if (record.Headers?.TryGetValue("amqp.routing_key", out var rkBytes) == true)
                {
                    routingKey = Encoding.UTF8.GetString(rkBytes);
                }
                else if (record.Key != null)
                {
                    routingKey = Encoding.UTF8.GetString(record.Key);
                }

                // Build properties
                var properties = new BasicProperties
                {
                    Persistent = _persistent,
                    Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
                };

                // Copy headers from record
                if (record.Headers != null)
                {
                    var amqpHeaders = new Dictionary<string, object?>();
                    foreach (var (key, value) in record.Headers)
                    {
                        if (key.StartsWith("amqp.header.", StringComparison.Ordinal))
                        {
                            amqpHeaders[key[12..]] = value;
                        }
                    }
                    if (amqpHeaders.Count > 0)
                    {
                        properties.Headers = amqpHeaders;
                    }

                    // Restore content type if present
                    if (record.Headers.TryGetValue("amqp.content_type", out var ctBytes))
                    {
                        properties.ContentType = Encoding.UTF8.GetString(ctBytes);
                    }
                    if (record.Headers.TryGetValue("amqp.correlation_id", out var corrBytes))
                    {
                        properties.CorrelationId = Encoding.UTF8.GetString(corrBytes);
                    }
                    if (record.Headers.TryGetValue("amqp.message_id", out var midBytes))
                    {
                        properties.MessageId = Encoding.UTF8.GetString(midBytes);
                    }
                }

                await _channel!.BasicPublishAsync(_exchange, routingKey, false, properties, record.Value, cancellationToken);
                _pendingConfirms++;

                // Wait for confirms in batches
                if (_pendingConfirms >= _batchSize)
                {
                    await WaitForConfirmsAsync(cancellationToken);
                }
            }
            catch (Exception)
            {
                // Log and continue
            }
        }

        // Wait for remaining confirms
        if (_pendingConfirms > 0)
        {
            await WaitForConfirmsAsync(cancellationToken);
        }
    }

    private Task WaitForConfirmsAsync(CancellationToken ct)
    {
        // In RabbitMQ.Client 7.0, awaiting BasicPublishAsync already waits for confirmation
        // when PublisherConfirmationsEnabled is true
        _pendingConfirms = 0;
        return Task.CompletedTask;
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public override async void Stop()
    {
        if (_channel != null)
        {
            await _channel.CloseAsync();
        }
        if (_connection != null)
        {
            await _connection.CloseAsync();
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _channel?.Dispose();
            _connection?.Dispose();
        }
        base.Dispose(disposing);
    }
}
