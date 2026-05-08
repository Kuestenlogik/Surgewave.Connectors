namespace Kuestenlogik.Surgewave.Connector.Aws.Sns;

using System.Text;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that publishes messages to an AWS SNS topic.
/// </summary>
public sealed class SnsSinkTask : SinkTask
{
    private AmazonSimpleNotificationServiceClient? _snsClient;
    private string _topicArn = "";
    private string _subject = "";
    private string _messageGroupId = "";
    private string _headerPrefix = SnsConnectorConfig.DefaultHeaderPrefix;
    private bool _isFifoTopic;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topicArn = config[SnsConnectorConfig.TopicArnConfig];
        _subject = GetConfigValue(config, SnsConnectorConfig.SubjectConfig, "");
        _messageGroupId = GetConfigValue(config, SnsConnectorConfig.MessageGroupIdConfig, "");
        _headerPrefix = GetConfigValue(config, SnsConnectorConfig.HeaderPrefixConfig, SnsConnectorConfig.DefaultHeaderPrefix);

        // Detect FIFO topic from ARN
        _isFifoTopic = _topicArn.EndsWith(".fifo", StringComparison.OrdinalIgnoreCase);

        var region = GetConfigValue(config, SnsConnectorConfig.RegionConfig, SnsConnectorConfig.DefaultRegion);
        var accessKey = GetConfigValue(config, SnsConnectorConfig.AccessKeyConfig, "");
        var secretKey = GetConfigValue(config, SnsConnectorConfig.SecretKeyConfig, "");
        var endpoint = GetConfigValue(config, SnsConnectorConfig.EndpointConfig, "");

        _snsClient = CreateSnsClient(accessKey, secretKey, region, endpoint);
    }

    private static AmazonSimpleNotificationServiceClient CreateSnsClient(
        string accessKey,
        string secretKey,
        string region,
        string endpoint)
    {
        var config = new AmazonSimpleNotificationServiceConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(region)
        };

        if (!string.IsNullOrEmpty(endpoint))
        {
            config.ServiceURL = endpoint;
        }

        if (!string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey))
        {
            return new AmazonSimpleNotificationServiceClient(new BasicAWSCredentials(accessKey, secretKey), config);
        }

        return new AmazonSimpleNotificationServiceClient(config);
    }

    public override void Stop()
    {
        _snsClient?.Dispose();
        _snsClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _snsClient?.Dispose();
            _snsClient = null;
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_snsClient == null || records.Count == 0)
            return;

        // SNS doesn't support batch publishing to regular topics, only FIFO topics via PublishBatch
        // For simplicity, publish individually (consider batching for FIFO topics in future)
        foreach (var record in records)
        {
            var request = CreatePublishRequest(record);
            await _snsClient.PublishAsync(request, cancellationToken);
        }
    }

    private PublishRequest CreatePublishRequest(SinkRecord record)
    {
        var request = new PublishRequest
        {
            TopicArn = _topicArn,
            Message = record.Value != null ? Encoding.UTF8.GetString(record.Value) : ""
        };

        // Set subject if configured
        if (!string.IsNullOrEmpty(_subject))
        {
            request.Subject = _subject;
        }

        // Set FIFO topic properties if applicable
        if (_isFifoTopic)
        {
            request.MessageGroupId = GetMessageGroupId(record);
            // MessageDeduplicationId is optional if content-based deduplication is enabled
        }

        // Map Surgewave headers to SNS message attributes
        if (record.Headers != null)
        {
            foreach (var header in record.Headers)
            {
                // Skip metadata headers, only map custom headers
                if (header.Key.StartsWith(_headerPrefix + "attr.", StringComparison.Ordinal))
                {
                    var attrKey = header.Key[(_headerPrefix.Length + 5)..];
                    request.MessageAttributes[attrKey] = new MessageAttributeValue
                    {
                        DataType = "String",
                        StringValue = Encoding.UTF8.GetString(header.Value)
                    };
                }
                else if (!header.Key.StartsWith(_headerPrefix, StringComparison.Ordinal))
                {
                    request.MessageAttributes[header.Key] = new MessageAttributeValue
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
            request.MessageAttributes["surgewave.topic"] = new MessageAttributeValue
            {
                DataType = "String",
                StringValue = record.Topic
            };
        }
        request.MessageAttributes["surgewave.partition"] = new MessageAttributeValue
        {
            DataType = "String",
            StringValue = record.Partition.ToString()
        };
        request.MessageAttributes["surgewave.offset"] = new MessageAttributeValue
        {
            DataType = "String",
            StringValue = record.Offset.ToString()
        };

        return request;
    }

    private string GetMessageGroupId(SinkRecord record)
    {
        // Use configured message group id if set
        if (!string.IsNullOrEmpty(_messageGroupId))
        {
            if (_messageGroupId.Equals("key", StringComparison.OrdinalIgnoreCase) && record.Key != null)
            {
                return Encoding.UTF8.GetString(record.Key);
            }
            // Check headers
            if (record.Headers != null && record.Headers.TryGetValue(_messageGroupId, out var headerValue))
            {
                return Encoding.UTF8.GetString(headerValue);
            }
            return _messageGroupId;
        }

        // Use topic and partition as default group id
        return $"{record.Topic ?? "default"}-{record.Partition}";
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;
}
