namespace Kuestenlogik.Surgewave.Connector.Aws.Sqs;

using System.Text;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that receives messages from an AWS SQS queue
/// and produces them to Surgewave topics.
/// </summary>
public sealed class SqsSourceTask : SourceTask
{
    private AmazonSQSClient? _sqsClient;
    private string _queueUrl = "";
    private string _surgewaveTopic = "";
    private int _waitTimeSeconds = SqsConnectorConfig.DefaultWaitTimeSeconds;
    private int _visibilityTimeout = SqsConnectorConfig.DefaultVisibilityTimeout;
    private int _maxMessages = SqsConnectorConfig.DefaultMaxMessages;
    private string _headerPrefix = SqsConnectorConfig.DefaultHeaderPrefix;
    private bool _includeMetadata = SqsConnectorConfig.DefaultIncludeMetadata;

    private readonly Dictionary<string, object> _sourcePartition = new();
    private readonly List<string> _pendingReceiptHandles = new();
    private readonly object _handleLock = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _queueUrl = config[SqsConnectorConfig.QueueUrlConfig];
        _surgewaveTopic = config[SqsConnectorConfig.SurgewaveTopicConfig];

        _waitTimeSeconds = GetConfigInt(config, SqsConnectorConfig.WaitTimeSecondsConfig, SqsConnectorConfig.DefaultWaitTimeSeconds);
        _visibilityTimeout = GetConfigInt(config, SqsConnectorConfig.VisibilityTimeoutConfig, SqsConnectorConfig.DefaultVisibilityTimeout);
        _maxMessages = GetConfigInt(config, SqsConnectorConfig.MaxMessagesConfig, SqsConnectorConfig.DefaultMaxMessages);
        _headerPrefix = GetConfigValue(config, SqsConnectorConfig.HeaderPrefixConfig, SqsConnectorConfig.DefaultHeaderPrefix);
        _includeMetadata = GetConfigBool(config, SqsConnectorConfig.IncludeMetadataConfig, SqsConnectorConfig.DefaultIncludeMetadata);

        _sourcePartition["sqs.queue.url"] = _queueUrl;

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

        // Use default credential chain (environment variables, IAM role, etc.)
        return new AmazonSQSClient(config);
    }

    public override void Stop()
    {
        _sqsClient?.Dispose();
        _sqsClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _sqsClient?.Dispose();
            _sqsClient = null;
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_sqsClient == null)
            return [];

        var request = new ReceiveMessageRequest
        {
            QueueUrl = _queueUrl,
            MaxNumberOfMessages = _maxMessages,
            WaitTimeSeconds = _waitTimeSeconds,
            VisibilityTimeout = _visibilityTimeout,
            MessageAttributeNames = ["All"],
            MessageSystemAttributeNames = ["All"]
        };

        var response = await _sqsClient.ReceiveMessageAsync(request, cancellationToken);

        if (response.Messages.Count == 0)
            return [];

        var records = new List<SourceRecord>(response.Messages.Count);

        foreach (var message in response.Messages)
        {
            var record = CreateSourceRecord(message);
            records.Add(record);

            // Track receipt handle for commit-based deletion
            lock (_handleLock)
            {
                _pendingReceiptHandles.Add(message.ReceiptHandle);
            }
        }

        return records;
    }

    public override async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_sqsClient == null)
            return;

        await DeletePendingMessagesAsync(cancellationToken);
    }

    private async Task DeletePendingMessagesAsync(CancellationToken cancellationToken)
    {
        if (_sqsClient == null)
            return;

        List<string> handles;
        lock (_handleLock)
        {
            if (_pendingReceiptHandles.Count == 0)
                return;

            handles = [.. _pendingReceiptHandles];
            _pendingReceiptHandles.Clear();
        }

        // Delete messages in batches of 10 (SQS limit)
        const int batchSize = 10;
        for (var i = 0; i < handles.Count; i += batchSize)
        {
            var batch = handles.Skip(i).Take(batchSize).ToList();
            var entries = batch.Select((handle, index) => new DeleteMessageBatchRequestEntry
            {
                Id = index.ToString(),
                ReceiptHandle = handle
            }).ToList();

            await _sqsClient.DeleteMessageBatchAsync(new DeleteMessageBatchRequest
            {
                QueueUrl = _queueUrl,
                Entries = entries
            }, cancellationToken);
        }
    }

    private SourceRecord CreateSourceRecord(Message message)
    {
        var headers = new Dictionary<string, byte[]>();

        // Add message attributes as headers
        foreach (var attr in message.MessageAttributes)
        {
            if (attr.Value.DataType == "String")
            {
                headers[$"{_headerPrefix}attr.{attr.Key}"] = Encoding.UTF8.GetBytes(attr.Value.StringValue ?? "");
            }
            else if (attr.Value.DataType == "Binary")
            {
                headers[$"{_headerPrefix}attr.{attr.Key}"] = attr.Value.BinaryValue.ToArray();
            }
        }

        // Add system attributes as headers
        foreach (var attr in message.Attributes)
        {
            headers[$"{_headerPrefix}sys.{attr.Key}"] = Encoding.UTF8.GetBytes(attr.Value);
        }

        // Add metadata headers if configured
        if (_includeMetadata)
        {
            headers[$"{_headerPrefix}messageId"] = Encoding.UTF8.GetBytes(message.MessageId);
            headers[$"{_headerPrefix}receiptHandle"] = Encoding.UTF8.GetBytes(message.ReceiptHandle);

            if (!string.IsNullOrEmpty(message.MD5OfBody))
            {
                headers[$"{_headerPrefix}md5OfBody"] = Encoding.UTF8.GetBytes(message.MD5OfBody);
            }
        }

        // Use MessageGroupId as key for FIFO queues
        byte[]? key = null;
        if (message.Attributes.TryGetValue("MessageGroupId", out var groupId))
        {
            key = Encoding.UTF8.GetBytes(groupId);
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = new Dictionary<string, object>
            {
                ["messageId"] = message.MessageId,
                ["receiptHandle"] = message.ReceiptHandle
            },
            Topic = _surgewaveTopic,
            Key = key,
            Value = Encoding.UTF8.GetBytes(message.Body),
            Timestamp = message.Attributes.TryGetValue("SentTimestamp", out var ts)
                ? DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(ts))
                : DateTimeOffset.UtcNow,
            Headers = headers
        };
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static int GetConfigInt(IDictionary<string, string> config, string key, int defaultValue)
        => config.TryGetValue(key, out var value) && int.TryParse(value, out var intValue) ? intValue : defaultValue;

    private static bool GetConfigBool(IDictionary<string, string> config, string key, bool defaultValue)
        => config.TryGetValue(key, out var value) && bool.TryParse(value, out var boolValue) ? boolValue : defaultValue;
}
