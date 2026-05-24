using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Threading.Channels;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.RabbitMQ;

/// <summary>
/// Task that consumes messages from a RabbitMQ queue and produces source records.
/// Supports manual acknowledgment for exactly-once semantics.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class RabbitMQSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _host = RabbitMQConnectorConfig.DefaultHost;
    private int _port = RabbitMQConnectorConfig.DefaultPort;
    private string _virtualHost = RabbitMQConnectorConfig.DefaultVirtualHost;
    private string _username = RabbitMQConnectorConfig.DefaultUsername;
    private string _password = RabbitMQConnectorConfig.DefaultPassword;
    private string _queue = "";
    private string _topic = "";
    private bool _queueDurable = true;
    private bool _queueExclusive = false;
    private bool _queueAutoDelete = false;
    private ushort _prefetchCount = RabbitMQConnectorConfig.DefaultPrefetchCount;
    private bool _autoAck = false;
    private int _batchSize = RabbitMQConnectorConfig.DefaultBatchSize;
    private int _pollTimeoutMs = RabbitMQConnectorConfig.DefaultPollTimeoutMs;

    private IConnection? _connection;
    private IChannel? _channel;
    private string? _consumerTag;
    private CancellationTokenSource? _cts;
    private readonly Channel<(BasicDeliverEventArgs EventArgs, byte[] Body)> _messageChannel =
        Channel.CreateBounded<(BasicDeliverEventArgs, byte[])>(1000);

    private readonly Dictionary<string, object> _sourcePartition = new();
    private readonly List<ulong> _pendingDeliveryTags = [];

    public override void Start(IDictionary<string, string> config)
    {
        _host = config.TryGetValue(RabbitMQConnectorConfig.Host, out var host) ? host : RabbitMQConnectorConfig.DefaultHost;
        _port = config.TryGetValue(RabbitMQConnectorConfig.Port, out var port) ? int.Parse(port) : RabbitMQConnectorConfig.DefaultPort;
        _virtualHost = config.TryGetValue(RabbitMQConnectorConfig.VirtualHost, out var vh) ? vh : RabbitMQConnectorConfig.DefaultVirtualHost;
        _username = config.TryGetValue(RabbitMQConnectorConfig.Username, out var user) ? user : RabbitMQConnectorConfig.DefaultUsername;
        _password = config.TryGetValue(RabbitMQConnectorConfig.Password, out var pwd) ? pwd : RabbitMQConnectorConfig.DefaultPassword;
        _queue = config.TryGetValue(RabbitMQConnectorConfig.Queue, out var q) ? q : "";
        _topic = config.TryGetValue(RabbitMQConnectorConfig.Topic, out var t) ? t : "";

        if (config.TryGetValue(RabbitMQConnectorConfig.QueueDurable, out var durable))
            _queueDurable = bool.Parse(durable);
        if (config.TryGetValue(RabbitMQConnectorConfig.QueueExclusive, out var exclusive))
            _queueExclusive = bool.Parse(exclusive);
        if (config.TryGetValue(RabbitMQConnectorConfig.QueueAutoDelete, out var autoDelete))
            _queueAutoDelete = bool.Parse(autoDelete);
        if (config.TryGetValue(RabbitMQConnectorConfig.PrefetchCount, out var pc))
            _prefetchCount = ushort.Parse(pc);
        if (config.TryGetValue(RabbitMQConnectorConfig.AutoAck, out var aa))
            _autoAck = bool.Parse(aa);
        if (config.TryGetValue(RabbitMQConnectorConfig.BatchSize, out var bs))
            _batchSize = int.Parse(bs);
        if (config.TryGetValue(RabbitMQConnectorConfig.PollTimeoutMs, out var pt))
            _pollTimeoutMs = int.Parse(pt);

        _sourcePartition["connector"] = "rabbitmq";
        _sourcePartition["host"] = _host;
        _sourcePartition["queue"] = _queue;

        // Build connection factory
        var factory = new ConnectionFactory
        {
            HostName = _host,
            Port = _port,
            VirtualHost = _virtualHost,
            UserName = _username,
            Password = _password
        };

        // TLS
        if (config.TryGetValue(RabbitMQConnectorConfig.TlsEnabled, out var tlsEnabled) && bool.Parse(tlsEnabled))
        {
            factory.Ssl = new SslOption
            {
                Enabled = true,
                ServerName = _host
            };

            if (config.TryGetValue(RabbitMQConnectorConfig.TlsCertFile, out var certFile) && !string.IsNullOrEmpty(certFile))
            {
                factory.Ssl.CertPath = certFile;
            }
        }

        // Connection name
        if (config.TryGetValue(RabbitMQConnectorConfig.ConnectionName, out var connName) && !string.IsNullOrEmpty(connName))
        {
            factory.ClientProvidedName = connName;
        }

        // Connect (sync wrapper for async API)
        _cts = new CancellationTokenSource();
        ConnectAsync(factory, config).GetAwaiter().GetResult();
    }

    private async Task ConnectAsync(ConnectionFactory factory, IDictionary<string, string> config)
    {
        _connection = await factory.CreateConnectionAsync(_cts!.Token);
        _channel = await _connection.CreateChannelAsync(cancellationToken: _cts.Token);

        // Set QoS (prefetch)
        await _channel.BasicQosAsync(0, _prefetchCount, false, _cts.Token);

        // Dead letter exchange setup
        var queueArgs = new Dictionary<string, object?>();
        if (config.TryGetValue(RabbitMQConnectorConfig.DeadLetterExchange, out var dlx) && !string.IsNullOrEmpty(dlx))
        {
            queueArgs["x-dead-letter-exchange"] = dlx;
            if (config.TryGetValue(RabbitMQConnectorConfig.DeadLetterRoutingKey, out var dlrk) && !string.IsNullOrEmpty(dlrk))
            {
                queueArgs["x-dead-letter-routing-key"] = dlrk;
            }
        }

        // Declare queue
        await _channel.QueueDeclareAsync(
            queue: _queue,
            durable: _queueDurable,
            exclusive: _queueExclusive,
            autoDelete: _queueAutoDelete,
            arguments: queueArgs,
            cancellationToken: _cts.Token);

        // Create consumer
        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            var body = ea.Body.ToArray();
            await _messageChannel.Writer.WriteAsync((ea, body), _cts.Token);
        };

        _consumerTag = await _channel.BasicConsumeAsync(
            queue: _queue,
            autoAck: _autoAck,
            consumer: consumer,
            cancellationToken: _cts.Token);
    }

    public override void Stop()
    {
        _cts?.Cancel();

        try
        {
            if (_channel != null && _consumerTag != null)
            {
                _channel.BasicCancelAsync(_consumerTag).GetAwaiter().GetResult();
            }
        }
        catch
        {
            // Ignore cancellation errors
        }

        _channel?.CloseAsync().GetAwaiter().GetResult();
        _channel?.Dispose();
        _channel = null;

        _connection?.CloseAsync().GetAwaiter().GetResult();
        _connection?.Dispose();
        _connection = null;

        _cts?.Dispose();
        _cts = null;
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        using var timeoutCts = new CancellationTokenSource(_pollTimeoutMs);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        try
        {
            while (records.Count < _batchSize)
            {
                if (_messageChannel.Reader.TryRead(out var item))
                {
                    var record = CreateSourceRecord(item.EventArgs, item.Body);
                    records.Add(record);

                    if (!_autoAck)
                    {
                        _pendingDeliveryTags.Add(item.EventArgs.DeliveryTag);
                    }
                }
                else
                {
                    var hasMore = await _messageChannel.Reader.WaitToReadAsync(linkedCts.Token);
                    if (!hasMore)
                        break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout or cancellation - return what we have
        }

        return records;
    }

    public override async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_channel == null || _autoAck || _pendingDeliveryTags.Count == 0)
            return;

        // Acknowledge all pending messages up to the highest delivery tag
        var maxTag = _pendingDeliveryTags.Max();
        await _channel.BasicAckAsync(maxTag, multiple: true, cancellationToken);
        _pendingDeliveryTags.Clear();
    }

    private SourceRecord CreateSourceRecord(BasicDeliverEventArgs ea, byte[] body)
    {
        var sourceOffset = new Dictionary<string, object>
        {
            [RabbitMQConnectorConfig.OffsetDeliveryTag] = ea.DeliveryTag,
            [RabbitMQConnectorConfig.OffsetTimestamp] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        // Use routing key as the message key
        byte[]? key = null;
        if (!string.IsNullOrEmpty(ea.RoutingKey))
        {
            key = Encoding.UTF8.GetBytes(ea.RoutingKey);
        }

        var headers = new Dictionary<string, byte[]>
        {
            ["rabbitmq.exchange"] = Encoding.UTF8.GetBytes(ea.Exchange),
            ["rabbitmq.routing.key"] = Encoding.UTF8.GetBytes(ea.RoutingKey),
            ["rabbitmq.delivery.tag"] = BitConverter.GetBytes(ea.DeliveryTag),
            ["rabbitmq.redelivered"] = BitConverter.GetBytes(ea.Redelivered)
        };

        // Copy RabbitMQ message headers
        if (ea.BasicProperties?.Headers != null)
        {
            foreach (var header in ea.BasicProperties.Headers)
            {
                if (header.Value is byte[] bytes)
                {
                    headers[$"rabbitmq.header.{header.Key}"] = bytes;
                }
                else if (header.Value != null)
                {
                    headers[$"rabbitmq.header.{header.Key}"] = Encoding.UTF8.GetBytes(header.Value.ToString() ?? "");
                }
            }
        }

        // Message properties
        if (ea.BasicProperties != null)
        {
            if (!string.IsNullOrEmpty(ea.BasicProperties.ContentType))
                headers["rabbitmq.content.type"] = Encoding.UTF8.GetBytes(ea.BasicProperties.ContentType);
            if (!string.IsNullOrEmpty(ea.BasicProperties.ContentEncoding))
                headers["rabbitmq.content.encoding"] = Encoding.UTF8.GetBytes(ea.BasicProperties.ContentEncoding);
            if (!string.IsNullOrEmpty(ea.BasicProperties.CorrelationId))
                headers["rabbitmq.correlation.id"] = Encoding.UTF8.GetBytes(ea.BasicProperties.CorrelationId);
            if (!string.IsNullOrEmpty(ea.BasicProperties.MessageId))
                headers["rabbitmq.message.id"] = Encoding.UTF8.GetBytes(ea.BasicProperties.MessageId);
            if (!string.IsNullOrEmpty(ea.BasicProperties.ReplyTo))
                headers["rabbitmq.reply.to"] = Encoding.UTF8.GetBytes(ea.BasicProperties.ReplyTo);
        }

        return new SourceRecord
        {
            Topic = _topic,
            Key = key,
            Value = body,
            SourcePartition = _sourcePartition,
            SourceOffset = sourceOffset,
            Timestamp = DateTimeOffset.UtcNow,
            Headers = headers
        };
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }
}
