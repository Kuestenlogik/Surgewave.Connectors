using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Amqp;

/// <summary>
/// Task that reads messages from AMQP queues.
/// </summary>
public sealed class AmqpSourceTask : SourceTask
{
    private IConnection? _connection;
    private IChannel? _channel;
    private string _topic = null!;
    private string _queue = null!;
    private bool _autoAck;
    private long _messageId;
    private readonly ConcurrentQueue<(BasicDeliverEventArgs Args, byte[] Body)> _messages = new();
    private readonly List<ulong> _pendingAcks = new();

    public override string Version => "1.0.0";

    public override async void Start(IDictionary<string, string> config)
    {
        _topic = config[AmqpConnectorConfig.Topic];
        _queue = config[AmqpConnectorConfig.SourceQueue];
        _autoAck = config.GetValueOrDefault(AmqpConnectorConfig.AutoAck, "false") == "true";

        var prefetchCount = ushort.Parse(config.GetValueOrDefault(AmqpConnectorConfig.PrefetchCount,
            AmqpConnectorConfig.DefaultPrefetchCount.ToString())!);

        var factory = CreateConnectionFactory(config);
        _connection = await factory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();

        // Set QoS
        await _channel.BasicQosAsync(0, prefetchCount, false);

        // Declare queue if requested
        var declareQueue = config.GetValueOrDefault(AmqpConnectorConfig.DeclareQueue, "true") == "true";
        if (declareQueue)
        {
            var durable = config.GetValueOrDefault(AmqpConnectorConfig.QueueDurable, "true") == "true";
            var exclusive = config.GetValueOrDefault(AmqpConnectorConfig.QueueExclusive, "false") == "true";
            var autoDelete = config.GetValueOrDefault(AmqpConnectorConfig.QueueAutoDelete, "false") == "true";

            await _channel.QueueDeclareAsync(_queue, durable, exclusive, autoDelete);

            // Bind to exchange if specified
            var exchange = config.GetValueOrDefault(AmqpConnectorConfig.SourceExchange, "");
            if (!string.IsNullOrWhiteSpace(exchange))
            {
                var routingKey = config.GetValueOrDefault(AmqpConnectorConfig.SourceRoutingKey, "#")!;
                await _channel.QueueBindAsync(_queue, exchange, routingKey);
            }
        }

        // Set up consumer
        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += OnMessageReceivedAsync;

        await _channel.BasicConsumeAsync(_queue, _autoAck, consumer);
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

    private Task OnMessageReceivedAsync(object sender, BasicDeliverEventArgs args)
    {
        _messages.Enqueue((args, args.Body.ToArray()));
        return Task.CompletedTask;
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        while (_messages.TryDequeue(out var item))
        {
            var record = CreateRecord(item.Args, item.Body);
            records.Add(record);

            if (!_autoAck)
            {
                _pendingAcks.Add(item.Args.DeliveryTag);
            }
        }

        return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
    }

    private SourceRecord CreateRecord(BasicDeliverEventArgs args, byte[] body)
    {
        var msgId = Interlocked.Increment(ref _messageId);

        // Extract headers
        var headers = new Dictionary<string, byte[]>
        {
            ["amqp.exchange"] = Encoding.UTF8.GetBytes(args.Exchange ?? ""),
            ["amqp.routing_key"] = Encoding.UTF8.GetBytes(args.RoutingKey ?? ""),
            ["amqp.delivery_tag"] = Encoding.UTF8.GetBytes(args.DeliveryTag.ToString()),
            ["amqp.redelivered"] = Encoding.UTF8.GetBytes(args.Redelivered.ToString())
        };

        // Copy AMQP headers
        if (args.BasicProperties?.Headers != null)
        {
            foreach (var (headerKey, headerValue) in args.BasicProperties.Headers)
            {
                if (headerValue is byte[] bytes)
                {
                    headers[$"amqp.header.{headerKey}"] = bytes;
                }
                else if (headerValue != null)
                {
                    headers[$"amqp.header.{headerKey}"] = Encoding.UTF8.GetBytes(headerValue.ToString()!);
                }
            }
        }

        // Add basic properties
        if (args.BasicProperties != null)
        {
            if (!string.IsNullOrEmpty(args.BasicProperties.ContentType))
            {
                headers["amqp.content_type"] = Encoding.UTF8.GetBytes(args.BasicProperties.ContentType);
            }
            if (!string.IsNullOrEmpty(args.BasicProperties.CorrelationId))
            {
                headers["amqp.correlation_id"] = Encoding.UTF8.GetBytes(args.BasicProperties.CorrelationId);
            }
            if (!string.IsNullOrEmpty(args.BasicProperties.MessageId))
            {
                headers["amqp.message_id"] = Encoding.UTF8.GetBytes(args.BasicProperties.MessageId);
            }
        }

        // Use routing key or message ID as key
        byte[]? key = null;
        if (!string.IsNullOrEmpty(args.RoutingKey))
        {
            key = Encoding.UTF8.GetBytes(args.RoutingKey);
        }
        else if (args.BasicProperties?.MessageId != null)
        {
            key = Encoding.UTF8.GetBytes(args.BasicProperties.MessageId);
        }

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["source"] = "amqp",
                ["queue"] = _queue
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["delivery_tag"] = args.DeliveryTag
            },
            Topic = _topic,
            Key = key,
            Value = body,
            Headers = headers
        };
    }

    public override async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_autoAck || _pendingAcks.Count == 0) return;

        try
        {
            // Ack all pending messages (use multiple ack for efficiency)
            var maxTag = _pendingAcks.Max();
            await _channel!.BasicAckAsync(maxTag, true);
            _pendingAcks.Clear();
        }
        catch (Exception)
        {
            // Log and continue
        }
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
