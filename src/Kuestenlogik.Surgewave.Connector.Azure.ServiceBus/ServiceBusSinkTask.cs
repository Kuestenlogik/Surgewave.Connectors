namespace Kuestenlogik.Surgewave.Connector.Azure.ServiceBus;

using System.Text;
using global::Azure.Messaging.ServiceBus;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that sends messages to an Azure Service Bus queue or topic.
/// </summary>
public sealed class ServiceBusSinkTask : SinkTask
{
    private ServiceBusClient? _client;
    private ServiceBusSender? _sender;
    private string _sessionIdField = "";
    private string _partitionKeyField = "";
    private string _headerPrefix = ServiceBusConnectorConfig.DefaultHeaderPrefix;
    private int _batchSize = ServiceBusConnectorConfig.DefaultBatchSize;

    private readonly List<ServiceBusMessage> _messageBuffer = new();
    private readonly object _bufferLock = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var connectionString = config[ServiceBusConnectorConfig.ConnectionStringConfig];
        var queueName = GetConfigValue(config, ServiceBusConnectorConfig.QueueNameConfig, "");
        var topicName = GetConfigValue(config, ServiceBusConnectorConfig.TopicNameConfig, "");

        _sessionIdField = GetConfigValue(config, ServiceBusConnectorConfig.SessionIdFieldConfig, "");
        _partitionKeyField = GetConfigValue(config, ServiceBusConnectorConfig.PartitionKeyFieldConfig, "");
        _headerPrefix = GetConfigValue(config, ServiceBusConnectorConfig.HeaderPrefixConfig, ServiceBusConnectorConfig.DefaultHeaderPrefix);
        _batchSize = GetConfigInt(config, ServiceBusConnectorConfig.BatchSizeConfig, ServiceBusConnectorConfig.DefaultBatchSize);

        _client = new ServiceBusClient(connectionString);

        var entityPath = !string.IsNullOrEmpty(queueName) ? queueName : topicName;
        _sender = _client.CreateSender(entityPath);
    }

    public override void Stop()
    {
        // Flush remaining messages
        FlushBufferAsync(CancellationToken.None).GetAwaiter().GetResult();

        _sender?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _client?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _sender = null;
        _client = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            try
            {
                FlushBufferAsync(CancellationToken.None).GetAwaiter().GetResult();
            }
            catch
            {
                // Ignore disposal errors
            }
            _sender?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _client?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _sender = null;
            _client = null;
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_sender == null || records.Count == 0)
            return;

        foreach (var record in records)
        {
            var message = CreateServiceBusMessage(record);

            lock (_bufferLock)
            {
                _messageBuffer.Add(message);
            }

            // Flush when batch size is reached
            if (_messageBuffer.Count >= _batchSize)
            {
                await FlushBufferAsync(cancellationToken);
            }
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        await FlushBufferAsync(cancellationToken);
    }

    private async Task FlushBufferAsync(CancellationToken cancellationToken)
    {
        if (_sender == null)
            return;

        List<ServiceBusMessage> messages;
        lock (_bufferLock)
        {
            if (_messageBuffer.Count == 0)
                return;

            messages = [.. _messageBuffer];
            _messageBuffer.Clear();
        }

        // Send in batches
        using var batch = await _sender.CreateMessageBatchAsync(cancellationToken);

        foreach (var message in messages)
        {
            if (!batch.TryAddMessage(message))
            {
                // Batch is full, send it and start a new one
                if (batch.Count > 0)
                {
                    await _sender.SendMessagesAsync(batch, cancellationToken);
                }

                // Send the message that didn't fit individually
                await _sender.SendMessageAsync(message, cancellationToken);
            }
        }

        // Send remaining messages in batch
        if (batch.Count > 0)
        {
            await _sender.SendMessagesAsync(batch, cancellationToken);
        }
    }

    private ServiceBusMessage CreateServiceBusMessage(SinkRecord record)
    {
        var message = new ServiceBusMessage(record.Value ?? []);

        // Set session id if configured
        var sessionId = GetSessionId(record);
        if (!string.IsNullOrEmpty(sessionId))
        {
            message.SessionId = sessionId;
        }

        // Set partition key if configured
        var partitionKey = GetPartitionKey(record);
        if (!string.IsNullOrEmpty(partitionKey))
        {
            message.PartitionKey = partitionKey;
        }

        // Map Surgewave headers to Service Bus application properties
        if (record.Headers != null)
        {
            foreach (var header in record.Headers)
            {
                // Skip metadata headers, only map custom headers
                if (header.Key.StartsWith(_headerPrefix + "prop.", StringComparison.Ordinal))
                {
                    var propKey = header.Key[(_headerPrefix.Length + 5)..];
                    message.ApplicationProperties[propKey] = Encoding.UTF8.GetString(header.Value);
                }
                else if (!header.Key.StartsWith(_headerPrefix, StringComparison.Ordinal))
                {
                    // Map non-prefixed headers as application properties
                    message.ApplicationProperties[header.Key] = Encoding.UTF8.GetString(header.Value);
                }
            }
        }

        // Add Surgewave metadata as application properties
        if (record.Topic != null)
        {
            message.ApplicationProperties["surgewave.topic"] = record.Topic;
        }
        message.ApplicationProperties["surgewave.partition"] = record.Partition;
        message.ApplicationProperties["surgewave.offset"] = record.Offset;

        return message;
    }

    private string? GetSessionId(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_sessionIdField))
            return null;

        // Check headers
        if (record.Headers != null && record.Headers.TryGetValue(_sessionIdField, out var headerValue))
        {
            return Encoding.UTF8.GetString(headerValue);
        }

        // Use record key as session id if field matches "key"
        if (_sessionIdField.Equals("key", StringComparison.OrdinalIgnoreCase) && record.Key != null)
        {
            return Encoding.UTF8.GetString(record.Key);
        }

        return null;
    }

    private string? GetPartitionKey(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_partitionKeyField))
            return null;

        // Check headers
        if (record.Headers != null && record.Headers.TryGetValue(_partitionKeyField, out var headerValue))
        {
            return Encoding.UTF8.GetString(headerValue);
        }

        // Use record key as partition key if field matches "key"
        if (_partitionKeyField.Equals("key", StringComparison.OrdinalIgnoreCase) && record.Key != null)
        {
            return Encoding.UTF8.GetString(record.Key);
        }

        return null;
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static int GetConfigInt(IDictionary<string, string> config, string key, int defaultValue)
        => config.TryGetValue(key, out var value) && int.TryParse(value, out var intValue) ? intValue : defaultValue;
}
