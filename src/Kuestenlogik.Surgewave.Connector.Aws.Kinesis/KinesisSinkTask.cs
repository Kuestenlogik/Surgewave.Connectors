using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Kinesis;

/// <summary>
/// Task that writes records to AWS Kinesis Data Streams.
/// Uses PutRecords for batch writes with automatic retry for failed records.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class KinesisSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private AmazonKinesisClient? _kinesisClient;
    private string _streamName = "";
    private string _partitionKeyField = "";
    private string _explicitHashKeyField = "";
    private int _batchSize = KinesisConnectorConfig.DefaultBatchSize;
    private int _retryCount = KinesisConnectorConfig.DefaultRetryCount;
    private long _retryDelayMs = KinesisConnectorConfig.DefaultRetryDelayMs;

    public override void Start(IDictionary<string, string> config)
    {
        _streamName = config[KinesisConnectorConfig.StreamNameConfig];
        _partitionKeyField = GetConfigValue(config, KinesisConnectorConfig.PartitionKeyFieldConfig, "");
        _explicitHashKeyField = GetConfigValue(config, KinesisConnectorConfig.ExplicitHashKeyFieldConfig, "");
        _batchSize = Math.Min(500, int.Parse(GetConfigValue(config, KinesisConnectorConfig.BatchSizeConfig, KinesisConnectorConfig.DefaultBatchSize.ToString())));
        _retryCount = int.Parse(GetConfigValue(config, KinesisConnectorConfig.RetryCountConfig, KinesisConnectorConfig.DefaultRetryCount.ToString()));
        _retryDelayMs = long.Parse(GetConfigValue(config, KinesisConnectorConfig.RetryDelayMsConfig, KinesisConnectorConfig.DefaultRetryDelayMs.ToString()));

        var region = GetConfigValue(config, KinesisConnectorConfig.RegionConfig, KinesisConnectorConfig.DefaultRegion);
        var accessKey = GetConfigValue(config, KinesisConnectorConfig.AccessKeyConfig, "");
        var secretKey = GetConfigValue(config, KinesisConnectorConfig.SecretKeyConfig, "");
        var endpoint = GetConfigValue(config, KinesisConnectorConfig.EndpointConfig, "");

        var clientConfig = new AmazonKinesisConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(region)
        };

        if (!string.IsNullOrEmpty(endpoint))
        {
            clientConfig.ServiceURL = endpoint;
        }

        if (!string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey))
        {
            var credentials = new BasicAWSCredentials(accessKey, secretKey);
            _kinesisClient = new AmazonKinesisClient(credentials, clientConfig);
        }
        else
        {
            _kinesisClient = new AmazonKinesisClient(clientConfig);
        }
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        _kinesisClient?.Dispose();
        _kinesisClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_kinesisClient == null || records.Count == 0)
            return;

        // Process in batches of max 500 records
        var batches = records.Chunk(_batchSize);

        foreach (var batch in batches)
        {
            await PutBatchAsync(batch.ToList(), cancellationToken);
        }
    }

    private async Task PutBatchAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        var putRecordsEntries = new List<PutRecordsRequestEntry>();

        foreach (var record in records)
        {
            var entry = CreatePutRecordsEntry(record);
            if (entry != null)
            {
                putRecordsEntries.Add(entry);
            }
        }

        if (putRecordsEntries.Count == 0)
            return;

        var request = new PutRecordsRequest
        {
            StreamName = _streamName,
            Records = putRecordsEntries
        };

        // Retry loop for failed records
        var retries = 0;
        while (retries <= _retryCount)
        {
            var response = await _kinesisClient!.PutRecordsAsync(request, cancellationToken);

            if (response.FailedRecordCount == 0)
                break;

            // Collect failed records for retry
            var failedEntries = new List<PutRecordsRequestEntry>();
            for (int i = 0; i < response.Records.Count; i++)
            {
                if (!string.IsNullOrEmpty(response.Records[i].ErrorCode))
                {
                    failedEntries.Add(request.Records[i]);
                }
            }

            if (failedEntries.Count == 0)
                break;

            retries++;
            if (retries > _retryCount)
            {
                throw new InvalidOperationException(
                    $"Failed to write {failedEntries.Count} records to Kinesis after {_retryCount} retries. " +
                    $"Last error: {response.Records.FirstOrDefault(r => !string.IsNullOrEmpty(r.ErrorCode))?.ErrorMessage}");
            }

            // Exponential backoff
            await Task.Delay(TimeSpan.FromMilliseconds(_retryDelayMs * Math.Pow(2, retries - 1)), cancellationToken);
            request.Records = failedEntries;
        }
    }

    private PutRecordsRequestEntry? CreatePutRecordsEntry(SinkRecord record)
    {
        if (record.Value == null || record.Value.Length == 0)
            return null;

        var partitionKey = GetPartitionKey(record);
        var explicitHashKey = GetExplicitHashKey(record);

        var entry = new PutRecordsRequestEntry
        {
            Data = new MemoryStream(record.Value),
            PartitionKey = partitionKey
        };

        if (!string.IsNullOrEmpty(explicitHashKey))
        {
            entry.ExplicitHashKey = explicitHashKey;
        }

        return entry;
    }

    private string GetPartitionKey(SinkRecord record)
    {
        // Try to extract partition key from value if field is specified
        if (!string.IsNullOrEmpty(_partitionKeyField) && record.Value != null && record.Value.Length > 0)
        {
            try
            {
                var json = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(record.Value);
                if (json != null && json.TryGetValue(_partitionKeyField, out var pkElement))
                {
                    return pkElement.ToString();
                }
            }
            catch (JsonException)
            {
                // Fall through to key-based or generated partition key
            }
        }

        // Use record key if available
        if (record.Key != null && record.Key.Length > 0)
        {
            try
            {
                var keyJson = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(record.Key);
                if (keyJson != null)
                {
                    // Use first key field
                    var firstKey = keyJson.FirstOrDefault();
                    if (!string.IsNullOrEmpty(firstKey.Key))
                    {
                        return firstKey.Value.ToString();
                    }
                }
            }
            catch (JsonException)
            {
                // Use raw key as string
                return Encoding.UTF8.GetString(record.Key);
            }
        }

        // Generate partition key from topic-partition-offset
        return $"{record.Topic}-{record.Partition}-{record.Offset}";
    }

    private string? GetExplicitHashKey(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_explicitHashKeyField) || record.Value == null || record.Value.Length == 0)
            return null;

        try
        {
            var json = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(record.Value);
            if (json != null && json.TryGetValue(_explicitHashKeyField, out var hashElement))
            {
                // Kinesis explicit hash key must be a 128-bit integer as a string
                var hashValue = hashElement.ToString();
                if (BigInteger.TryParse(hashValue, out var bigInt))
                {
                    return bigInt.ToString();
                }
                // Hash the string value to get a 128-bit key
                return ComputeHash128(hashValue);
            }
        }
        catch (JsonException)
        {
            // Ignore
        }

        return null;
    }

    [SuppressMessage("Security", "CA5351:Do not use broken cryptographic algorithms", Justification = "MD5 used only for hash distribution, not for security")]
    private static string ComputeHash128(string value)
    {
        var hash = MD5.HashData(Encoding.UTF8.GetBytes(value));
        var bigInt = new BigInteger(hash, isUnsigned: true);
        return bigInt.ToString();
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // Kinesis writes are synchronous in PutAsync
        return Task.CompletedTask;
    }
}
