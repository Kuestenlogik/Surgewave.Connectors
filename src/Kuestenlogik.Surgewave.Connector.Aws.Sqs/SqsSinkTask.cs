namespace Kuestenlogik.Surgewave.Connector.Aws.Sqs;

using System.Text;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that sends messages to an AWS SQS queue.
/// </summary>
public sealed class SqsSinkTask : SinkTask
{
    private AmazonSQSClient? _sqsClient;
    private string _queueUrl = "";
    private string _messageGroupIdField = "";
    private string _deduplicationIdField = "";
    private string _headerPrefix = SqsConnectorConfig.DefaultHeaderPrefix;
    private bool _isFifoQueue;

    private readonly List<SendMessageBatchRequestEntry> _messageBuffer = new();
    private readonly object _bufferLock = new();
    private int _batchIdCounter;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _queueUrl = config[SqsConnectorConfig.QueueUrlConfig];
        _messageGroupIdField = GetConfigValue(config, SqsConnectorConfig.MessageGroupIdFieldConfig, "");
        _deduplicationIdField = GetConfigValue(config, SqsConnectorConfig.DeduplicationIdFieldConfig, "");
        _headerPrefix = GetConfigValue(config, SqsConnectorConfig.HeaderPrefixConfig, SqsConnectorConfig.DefaultHeaderPrefix);

        // Detect FIFO queue from URL
        _isFifoQueue = _queueUrl.EndsWith(".fifo", StringComparison.OrdinalIgnoreCase);

        var region = GetConfigValue(config, SqsConnectorConfig.RegionConfig, SqsConnectorConfig.DefaultRegion);
        var accessKey = GetConfigValue(config, SqsConnectorConfig.AccessKeyConfig, "");
        var secretKey = GetConfigValue(config, SqsConnectorConfig.SecretKeyConfig, "");
        var endpoint = GetConfigValue(config, SqsConnectorConfig.EndpointConfig, "");

        _sqsClient = CreateSqsClient(accessKey, secretKey, region, endpoint);
    }

    private static AmazonSQSClient CreateSqsClient(
        string accessKey,
        string secretKey,
        string region,
        string endpoint)
    {
        var config = new AmazonSQSConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(region)
        };

        if (!string.IsNullOrEmpty(endpoint))
        {
            config.ServiceURL = endpoint;
        }

        if (!string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey))
        {
            return new AmazonSQSClient(new BasicAWSCredentials(accessKey, secretKey), config);
        }

        return new AmazonSQSClient(config);
    }

    public override void Stop()
    {
        // Flush remaining messages
        FlushBufferAsync(CancellationToken.None).GetAwaiter().GetResult();
        _sqsClient?.Dispose();
        _sqsClient = null;
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
            _sqsClient?.Dispose();
            _sqsClient = null;
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_sqsClient == null || records.Count == 0)
            return;

        foreach (var record in records)
        {
            var entry = CreateMessageEntry(record);

            lock (_bufferLock)
            {
                _messageBuffer.Add(entry);
            }

            // Flush when batch size is reached (SQS max is 10)
            if (_messageBuffer.Count >= 10)
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
        if (_sqsClient == null)
            return;

        List<SendMessageBatchRequestEntry> entries;
        lock (_bufferLock)
        {
            if (_messageBuffer.Count == 0)
                return;

            entries = [.. _messageBuffer];
            _messageBuffer.Clear();
        }

        // Send in batches of 10 (SQS limit)
        const int batchSize = 10;
        for (var i = 0; i < entries.Count; i += batchSize)
        {
            var batch = entries.Skip(i).Take(batchSize).ToList();
            await _sqsClient.SendMessageBatchAsync(new SendMessageBatchRequest
            {
                QueueUrl = _queueUrl,
                Entries = batch
            }, cancellationToken);
        }
    }

    private SendMessageBatchRequestEntry CreateMessageEntry(SinkRecord record)
    {
        var entry = new SendMessageBatchRequestEntry
        {
            Id = Interlocked.Increment(ref _batchIdCounter).ToString(),
            MessageBody = record.Value != null ? Encoding.UTF8.GetString(record.Value) : ""
        };

        // Set FIFO queue properties if applicable
        if (_isFifoQueue)
        {
            entry.MessageGroupId = GetMessageGroupId(record);
            var dedupId = GetDeduplicationId(record);
            if (!string.IsNullOrEmpty(dedupId))
            {
                entry.MessageDeduplicationId = dedupId;
            }
        }

        // Map Surgewave headers to SQS message attributes
        if (record.Headers != null)
        {
            foreach (var header in record.Headers)
            {
                // Skip metadata headers, only map custom headers
                if (header.Key.StartsWith(_headerPrefix + "attr.", StringComparison.Ordinal))
                {
                    var attrKey = header.Key[(_headerPrefix.Length + 5)..]; // Remove prefix + "attr."
                    entry.MessageAttributes[attrKey] = new MessageAttributeValue
                    {
                        DataType = "String",
                        StringValue = Encoding.UTF8.GetString(header.Value)
                    };
                }
                else if (!header.Key.StartsWith(_headerPrefix, StringComparison.Ordinal))
                {
                    // Map non-prefixed headers as attributes
                    entry.MessageAttributes[header.Key] = new MessageAttributeValue
                    {
                        DataType = "String",
                        StringValue = Encoding.UTF8.GetString(header.Value)
                    };
                }
            }
        }

        // Add Surgewave metadata as attributes
        if (record.Topic != null)
        {
            entry.MessageAttributes["surgewave.topic"] = new MessageAttributeValue
            {
                DataType = "String",
                StringValue = record.Topic
            };
        }
        entry.MessageAttributes["surgewave.partition"] = new MessageAttributeValue
        {
            DataType = "String",
            StringValue = record.Partition.ToString()
        };
        entry.MessageAttributes["surgewave.offset"] = new MessageAttributeValue
        {
            DataType = "String",
            StringValue = record.Offset.ToString()
        };

        return entry;
    }

    private string GetMessageGroupId(SinkRecord record)
    {
        // First, check if the group id is in headers
        if (!string.IsNullOrEmpty(_messageGroupIdField) && record.Headers != null &&
            record.Headers.TryGetValue(_messageGroupIdField, out var headerValue))
        {
            return Encoding.UTF8.GetString(headerValue);
        }

        // Use record key as group id if field matches "key"
        if (_messageGroupIdField.Equals("key", StringComparison.OrdinalIgnoreCase) && record.Key != null)
        {
            return Encoding.UTF8.GetString(record.Key);
        }

        // Use topic and partition as default group id
        return $"{record.Topic ?? "default"}-{record.Partition}";
    }

    private string? GetDeduplicationId(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_deduplicationIdField))
            return null;

        // Check headers
        if (record.Headers != null && record.Headers.TryGetValue(_deduplicationIdField, out var headerValue))
        {
            return Encoding.UTF8.GetString(headerValue);
        }

        // Use offset as deduplication id if field matches "offset"
        if (_deduplicationIdField.Equals("offset", StringComparison.OrdinalIgnoreCase))
        {
            return $"{record.Topic}-{record.Partition}-{record.Offset}";
        }

        return null;
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;
}
