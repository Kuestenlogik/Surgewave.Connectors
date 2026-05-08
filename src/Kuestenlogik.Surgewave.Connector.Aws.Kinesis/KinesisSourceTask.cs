using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Kinesis;

/// <summary>
/// Task that consumes records from AWS Kinesis Data Streams.
/// Reads from all shards with automatic shard discovery and iterator management.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class KinesisSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private AmazonKinesisClient? _kinesisClient;
    private string _streamName = "";
    private string _region = KinesisConnectorConfig.DefaultRegion;
    private string _topicPattern = KinesisConnectorConfig.DefaultTopicPattern;
    private string _shardIteratorType = KinesisConnectorConfig.ShardIteratorLatest;
    private long _pollIntervalMs = KinesisConnectorConfig.DefaultPollIntervalMs;
    private int _batchMaxRecords = KinesisConnectorConfig.DefaultBatchMaxRecords;
    private bool _startFromBeginning;
    private DateTime? _startTimestamp;
    private bool _includeMetadata = true;

    private readonly Dictionary<string, string> _shardIterators = new();
    private readonly Dictionary<string, string> _lastSequenceNumbers = new();
    private readonly HashSet<string> _completedShards = new();
    private readonly Dictionary<string, object> _sourcePartition = new();
    private bool _shardsInitialized;

    public override void Start(IDictionary<string, string> config)
    {
        _streamName = config[KinesisConnectorConfig.StreamNameConfig];
        _region = GetConfigValue(config, KinesisConnectorConfig.RegionConfig, KinesisConnectorConfig.DefaultRegion);
        _topicPattern = GetConfigValue(config, KinesisConnectorConfig.TopicPatternConfig, KinesisConnectorConfig.DefaultTopicPattern);
        _shardIteratorType = GetConfigValue(config, KinesisConnectorConfig.ShardIteratorTypeConfig, KinesisConnectorConfig.ShardIteratorLatest);
        _pollIntervalMs = long.Parse(GetConfigValue(config, KinesisConnectorConfig.PollIntervalMsConfig, KinesisConnectorConfig.DefaultPollIntervalMs.ToString()));
        _batchMaxRecords = Math.Min(10000, int.Parse(GetConfigValue(config, KinesisConnectorConfig.BatchMaxRecordsConfig, KinesisConnectorConfig.DefaultBatchMaxRecords.ToString())));
        _startFromBeginning = bool.Parse(GetConfigValue(config, KinesisConnectorConfig.StartFromBeginningConfig, "false"));
        _includeMetadata = bool.Parse(GetConfigValue(config, KinesisConnectorConfig.IncludeMetadataConfig, "true"));

        var startTimestampStr = GetConfigValue(config, KinesisConnectorConfig.StartTimestampConfig, "");
        if (!string.IsNullOrEmpty(startTimestampStr) && DateTime.TryParse(startTimestampStr, out var ts))
        {
            _startTimestamp = ts;
            _shardIteratorType = KinesisConnectorConfig.ShardIteratorAtTimestamp;
        }

        if (_startFromBeginning)
        {
            _shardIteratorType = KinesisConnectorConfig.ShardIteratorTrimHorizon;
        }

        _sourcePartition["stream"] = _streamName;

        // Create Kinesis client
        var clientConfig = new AmazonKinesisConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(_region)
        };

        var accessKey = GetConfigValue(config, KinesisConnectorConfig.AccessKeyConfig, "");
        var secretKey = GetConfigValue(config, KinesisConnectorConfig.SecretKeyConfig, "");
        var endpoint = GetConfigValue(config, KinesisConnectorConfig.EndpointConfig, "");

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

        RestoreOffset();
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    private void RestoreOffset()
    {
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return;

        // Restore sequence numbers for each shard
        foreach (var kvp in storedOffset)
        {
            if (kvp.Key.StartsWith("shard:", StringComparison.Ordinal) && kvp.Value != null)
            {
                var shardId = kvp.Key[6..]; // Remove "shard:" prefix
                _lastSequenceNumbers[shardId] = kvp.Value.ToString()!;
            }
        }
    }

    public override void Stop()
    {
        _kinesisClient?.Dispose();
        _kinesisClient = null;
        _shardIterators.Clear();
        _completedShards.Clear();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_kinesisClient == null)
            return [];

        // Initialize shards on first poll
        if (!_shardsInitialized)
        {
            await InitializeShardsAsync(cancellationToken);
            _shardsInitialized = true;
        }

        var records = new List<SourceRecord>();

        // Read from each shard
        foreach (var shardId in _shardIterators.Keys.ToList())
        {
            if (_completedShards.Contains(shardId))
                continue;

            var shardRecords = await ReadShardAsync(shardId, cancellationToken);
            records.AddRange(shardRecords);

            if (records.Count >= _batchMaxRecords)
                break;
        }

        // If no records, wait before next poll
        if (records.Count == 0)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs), cancellationToken);
        }

        return records;
    }

    private async Task InitializeShardsAsync(CancellationToken cancellationToken)
    {
        string? exclusiveStartShardId = null;

        do
        {
            var request = new ListShardsRequest
            {
                StreamName = _streamName,
                ExclusiveStartShardId = exclusiveStartShardId
            };

            var response = await _kinesisClient!.ListShardsAsync(request, cancellationToken);

            foreach (var shard in response.Shards)
            {
                // Skip closed shards (ended shards have EndingSequenceNumber set)
                if (shard.SequenceNumberRange.EndingSequenceNumber != null)
                    continue;

                await InitializeShardIteratorAsync(shard.ShardId, cancellationToken);
            }

            exclusiveStartShardId = response.NextToken != null ? response.Shards.LastOrDefault()?.ShardId : null;
        } while (exclusiveStartShardId != null);
    }

    private async Task InitializeShardIteratorAsync(string shardId, CancellationToken cancellationToken)
    {
        var request = new GetShardIteratorRequest
        {
            StreamName = _streamName,
            ShardId = shardId
        };

        // Check if we have a stored sequence number for this shard
        if (_lastSequenceNumbers.TryGetValue(shardId, out var sequenceNumber))
        {
            request.ShardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER;
            request.StartingSequenceNumber = sequenceNumber;
        }
        else if (_startTimestamp.HasValue)
        {
            request.ShardIteratorType = ShardIteratorType.AT_TIMESTAMP;
            request.Timestamp = _startTimestamp.Value;
        }
        else
        {
            request.ShardIteratorType = _shardIteratorType switch
            {
                KinesisConnectorConfig.ShardIteratorTrimHorizon => ShardIteratorType.TRIM_HORIZON,
                KinesisConnectorConfig.ShardIteratorLatest => ShardIteratorType.LATEST,
                _ => ShardIteratorType.LATEST
            };
        }

        var response = await _kinesisClient!.GetShardIteratorAsync(request, cancellationToken);
        _shardIterators[shardId] = response.ShardIterator;
    }

    private async Task<List<SourceRecord>> ReadShardAsync(string shardId, CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        if (!_shardIterators.TryGetValue(shardId, out var iterator) || string.IsNullOrEmpty(iterator))
        {
            _completedShards.Add(shardId);
            return records;
        }

        var request = new GetRecordsRequest
        {
            ShardIterator = iterator,
            Limit = _batchMaxRecords
        };

        try
        {
            var response = await _kinesisClient!.GetRecordsAsync(request, cancellationToken);

            foreach (var record in response.Records)
            {
                var sourceRecord = ConvertToSourceRecord(record, shardId);
                records.Add(sourceRecord);
                _lastSequenceNumbers[shardId] = record.SequenceNumber;
            }

            // Update iterator for next read
            if (!string.IsNullOrEmpty(response.NextShardIterator))
            {
                _shardIterators[shardId] = response.NextShardIterator;
            }
            else
            {
                // Shard is exhausted
                _completedShards.Add(shardId);
            }
        }
        catch (ExpiredIteratorException)
        {
            // Re-initialize the iterator
            await InitializeShardIteratorAsync(shardId, cancellationToken);
        }
        catch (ProvisionedThroughputExceededException)
        {
            // Backoff and retry on next poll
            await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs * 2), cancellationToken);
        }

        return records;
    }

    private SourceRecord ConvertToSourceRecord(Record record, string shardId)
    {
        var data = record.Data.ToArray();

        // Build key from partition key
        var key = new Dictionary<string, object>
        {
            ["partition_key"] = record.PartitionKey
        };

        // Build payload
        Dictionary<string, object?> payload;
        if (_includeMetadata)
        {
            payload = new Dictionary<string, object?>
            {
                ["data"] = TryParseJson(data) ?? Convert.ToBase64String(data),
                ["partition_key"] = record.PartitionKey,
                ["sequence_number"] = record.SequenceNumber,
                ["shard_id"] = shardId,
                ["approximate_arrival_timestamp"] = record.ApproximateArrivalTimestamp?.ToUniversalTime().ToString("o") ?? "",
                ["stream"] = _streamName
            };

            if (!string.IsNullOrEmpty(record.EncryptionType?.Value))
            {
                payload["encryption_type"] = record.EncryptionType.Value;
            }
        }
        else
        {
            // Just the data
            var jsonData = TryParseJson(data);
            if (jsonData != null)
            {
                payload = new Dictionary<string, object?> { ["data"] = jsonData };
            }
            else
            {
                payload = new Dictionary<string, object?> { ["data"] = Convert.ToBase64String(data) };
            }
        }

        // Build offset
        var offset = new Dictionary<string, object>
        {
            [$"shard:{shardId}"] = record.SequenceNumber
        };

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = offset,
            Topic = GetTopicName(),
            Key = JsonSerializer.SerializeToUtf8Bytes(key),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = record.ApproximateArrivalTimestamp ?? DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                [KinesisConnectorConfig.HeaderStreamName] = Encoding.UTF8.GetBytes(_streamName),
                [KinesisConnectorConfig.HeaderPartitionKey] = Encoding.UTF8.GetBytes(record.PartitionKey),
                [KinesisConnectorConfig.HeaderSequenceNumber] = Encoding.UTF8.GetBytes(record.SequenceNumber),
                [KinesisConnectorConfig.HeaderShardId] = Encoding.UTF8.GetBytes(shardId),
                [KinesisConnectorConfig.HeaderApproximateArrivalTimestamp] = Encoding.UTF8.GetBytes(record.ApproximateArrivalTimestamp?.ToUniversalTime().ToString("o") ?? "")
            }
        };
    }

    private static object? TryParseJson(byte[] data)
    {
        try
        {
            var json = Encoding.UTF8.GetString(data);
            return JsonSerializer.Deserialize<object>(json);
        }
        catch
        {
            return null;
        }
    }

    private string GetTopicName()
    {
        return _topicPattern.Replace("${stream}", _streamName);
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Kinesis doesn't require acknowledgment
        // Position tracking is done via sequence numbers in offsets
        return Task.CompletedTask;
    }
}
