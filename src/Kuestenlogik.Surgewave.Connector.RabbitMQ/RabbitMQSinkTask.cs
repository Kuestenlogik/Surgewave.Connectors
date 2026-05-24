using System.Diagnostics.CodeAnalysis;
using System.Text;
using RabbitMQ.Client;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.RabbitMQ;

/// <summary>
/// Task that publishes sink records to RabbitMQ exchanges.
/// Supports routing key templating with topic, key, and partition placeholders.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class RabbitMQSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _host = RabbitMQConnectorConfig.DefaultHost;
    private int _port = RabbitMQConnectorConfig.DefaultPort;
    private string _virtualHost = RabbitMQConnectorConfig.DefaultVirtualHost;
    private string _username = RabbitMQConnectorConfig.DefaultUsername;
    private string _password = RabbitMQConnectorConfig.DefaultPassword;
    private string _exchange = "";
    private string _exchangeType = RabbitMQConnectorConfig.DefaultExchangeType;
    private bool _exchangeDurable = true;
    private bool _exchangeAutoDelete = false;
    private string _routingKeyTemplate = RabbitMQConnectorConfig.DefaultRoutingKeyTemplate;
    private bool _persistent = true;
    private bool _mandatory = false;
    private string _contentType = RabbitMQConnectorConfig.DefaultContentType;
    private int _messageTtlMs = 0;

    private IConnection? _connection;
    private IChannel? _channel;
    private CancellationTokenSource? _cts;
    private readonly object _lock = new();

    public override void Start(IDictionary<string, string> config)
    {
        _host = config.TryGetValue(RabbitMQConnectorConfig.Host, out var host) ? host : RabbitMQConnectorConfig.DefaultHost;
        _port = config.TryGetValue(RabbitMQConnectorConfig.Port, out var port) ? int.Parse(port) : RabbitMQConnectorConfig.DefaultPort;
        _virtualHost = config.TryGetValue(RabbitMQConnectorConfig.VirtualHost, out var vh) ? vh : RabbitMQConnectorConfig.DefaultVirtualHost;
        _username = config.TryGetValue(RabbitMQConnectorConfig.Username, out var user) ? user : RabbitMQConnectorConfig.DefaultUsername;
        _password = config.TryGetValue(RabbitMQConnectorConfig.Password, out var pwd) ? pwd : RabbitMQConnectorConfig.DefaultPassword;
        _exchange = config.TryGetValue(RabbitMQConnectorConfig.Exchange, out var ex) ? ex : "";
        _exchangeType = config.TryGetValue(RabbitMQConnectorConfig.ExchangeType, out var et) ? et : RabbitMQConnectorConfig.DefaultExchangeType;
        _routingKeyTemplate = config.TryGetValue(RabbitMQConnectorConfig.RoutingKeyTemplate, out var rkt)
            ? rkt : RabbitMQConnectorConfig.DefaultRoutingKeyTemplate;

        if (config.TryGetValue(RabbitMQConnectorConfig.ExchangeDurable, out var ed))
            _exchangeDurable = bool.Parse(ed);
        if (config.TryGetValue(RabbitMQConnectorConfig.ExchangeAutoDelete, out var ead))
            _exchangeAutoDelete = bool.Parse(ead);
        if (config.TryGetValue(RabbitMQConnectorConfig.Persistent, out var pers))
            _persistent = bool.Parse(pers);
        if (config.TryGetValue(RabbitMQConnectorConfig.Mandatory, out var mand))
            _mandatory = bool.Parse(mand);
        if (config.TryGetValue(RabbitMQConnectorConfig.ContentType, out var ct))
            _contentType = ct;
        if (config.TryGetValue(RabbitMQConnectorConfig.MessageTtlMs, out var ttl))
            _messageTtlMs = int.Parse(ttl);

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
        ConnectAsync(factory).GetAwaiter().GetResult();
    }

    private async Task ConnectAsync(ConnectionFactory factory)
    {
        _connection = await factory.CreateConnectionAsync(_cts!.Token);
        _channel = await _connection.CreateChannelAsync(cancellationToken: _cts.Token);

        // Declare exchange if specified
        if (!string.IsNullOrEmpty(_exchange))
        {
            await _channel.ExchangeDeclareAsync(
                exchange: _exchange,
                type: _exchangeType,
                durable: _exchangeDurable,
                autoDelete: _exchangeAutoDelete,
                cancellationToken: _cts.Token);
        }
    }

    public override void Stop()
    {
        lock (_lock)
        {
            _cts?.Cancel();

            _channel?.CloseAsync().GetAwaiter().GetResult();
            _channel?.Dispose();
            _channel = null;

            _connection?.CloseAsync().GetAwaiter().GetResult();
            _connection?.Dispose();
            _connection = null;

            _cts?.Dispose();
            _cts = null;
        }
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0 || _channel == null)
            return;

        foreach (var record in records)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var routingKey = BuildRoutingKey(record);
            var body = record.Value ?? [];

            // Build basic properties
            var props = new BasicProperties
            {
                ContentType = _contentType,
                DeliveryMode = _persistent ? DeliveryModes.Persistent : DeliveryModes.Transient,
                Timestamp = new AmqpTimestamp(record.Timestamp.ToUnixTimeSeconds())
            };

            // Message TTL
            if (_messageTtlMs > 0)
            {
                props.Expiration = _messageTtlMs.ToString();
            }

            // Copy headers from record
            if (record.Headers != null && record.Headers.Count > 0)
            {
                var headers = new Dictionary<string, object?>();
                foreach (var header in record.Headers)
                {
                    // Skip internal headers
                    if (header.Key.StartsWith("rabbitmq.", StringComparison.OrdinalIgnoreCase))
                        continue;

                    headers[header.Key] = header.Value;
                }

                // Add Surgewave metadata
                headers["surgewave.topic"] = Encoding.UTF8.GetBytes(record.Topic);
                headers["surgewave.partition"] = BitConverter.GetBytes(record.Partition);
                headers["surgewave.offset"] = BitConverter.GetBytes(record.Offset);

                props.Headers = headers;
            }
            else
            {
                // Add Surgewave metadata
                props.Headers = new Dictionary<string, object?>
                {
                    ["surgewave.topic"] = Encoding.UTF8.GetBytes(record.Topic),
                    ["surgewave.partition"] = BitConverter.GetBytes(record.Partition),
                    ["surgewave.offset"] = BitConverter.GetBytes(record.Offset)
                };
            }

            await _channel.BasicPublishAsync(
                exchange: _exchange,
                routingKey: routingKey,
                mandatory: _mandatory,
                basicProperties: props,
                body: body,
                cancellationToken: cancellationToken);
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // RabbitMQ publishes are immediately sent
        // Could enable publisher confirms for guaranteed delivery
        return Task.CompletedTask;
    }

    private string BuildRoutingKey(SinkRecord record)
    {
        var routingKey = _routingKeyTemplate;

        // Replace topic placeholder
        routingKey = routingKey.Replace("${topic}", record.Topic);

        // Replace key placeholder if present
        if (record.Key != null)
        {
            var keyString = Encoding.UTF8.GetString(record.Key);
            routingKey = routingKey.Replace("${key}", keyString);
        }
        else
        {
            routingKey = routingKey.Replace("${key}", "");
        }

        // Replace partition placeholder
        routingKey = routingKey.Replace("${partition}", record.Partition.ToString());

        return routingKey;
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
