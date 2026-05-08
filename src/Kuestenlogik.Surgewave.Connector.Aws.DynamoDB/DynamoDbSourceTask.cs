using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.DynamoDBStreams;
using Amazon.DynamoDBStreams.Model;
using Amazon.Runtime;

using StreamRecord = Amazon.DynamoDBStreams.Model.Record;
using StreamAttributeValue = Amazon.DynamoDBStreams.Model.AttributeValue;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.DynamoDB;

/// <summary>
/// Task that captures changes from DynamoDB Streams.
/// Reads shard iterators and polls for records with INSERT, MODIFY, and REMOVE events.
/// Produces Debezium-compatible JSON output.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class DynamoDbSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private AmazonDynamoDBStreamsClient? _streamsClient;
    private string _streamArn = "";
    private string _tableName = "";
    private string _region = DynamoDbConnectorConfig.DefaultRegion;
    private string _topicPattern = DynamoDbConnectorConfig.DefaultTopicPattern;
    private string _shardIteratorType = DynamoDbConnectorConfig.ShardIteratorLatest;
    private long _pollIntervalMs = DynamoDbConnectorConfig.DefaultPollIntervalMs;
    private int _batchMaxRecords = DynamoDbConnectorConfig.DefaultBatchMaxRecords;
    private bool _startFromBeginning;
    private bool _includeMetadata = true;

    private readonly Dictionary<string, string> _shardIterators = new();
    private readonly Dictionary<string, string> _lastSequenceNumbers = new();
    private readonly HashSet<string> _completedShards = new();
    private readonly Dictionary<string, object> _sourcePartition = new();
    private bool _shardsInitialized;

    public override void Start(IDictionary<string, string> config)
    {
        _streamArn = config[DynamoDbConnectorConfig.StreamArnConfig];
        _tableName = GetConfigValue(config, DynamoDbConnectorConfig.TableNameConfig, ExtractTableNameFromArn(_streamArn));
        _region = GetConfigValue(config, DynamoDbConnectorConfig.RegionConfig, DynamoDbConnectorConfig.DefaultRegion);
        _topicPattern = GetConfigValue(config, DynamoDbConnectorConfig.TopicPatternConfig, DynamoDbConnectorConfig.DefaultTopicPattern);
        _shardIteratorType = GetConfigValue(config, DynamoDbConnectorConfig.ShardIteratorTypeConfig, DynamoDbConnectorConfig.ShardIteratorLatest);
        _pollIntervalMs = long.Parse(GetConfigValue(config, DynamoDbConnectorConfig.PollIntervalMsConfig, DynamoDbConnectorConfig.DefaultPollIntervalMs.ToString()));
        _batchMaxRecords = int.Parse(GetConfigValue(config, DynamoDbConnectorConfig.BatchMaxRecordsConfig, DynamoDbConnectorConfig.DefaultBatchMaxRecords.ToString()));
        _startFromBeginning = bool.Parse(GetConfigValue(config, DynamoDbConnectorConfig.StartFromBeginningConfig, "false"));
        _includeMetadata = bool.Parse(GetConfigValue(config, DynamoDbConnectorConfig.IncludeMetadataConfig, "true"));

        if (_startFromBeginning)
        {
            _shardIteratorType = DynamoDbConnectorConfig.ShardIteratorTrimHorizon;
        }

        _sourcePartition["stream_arn"] = _streamArn;
        _sourcePartition["table"] = _tableName;

        // Create DynamoDB Streams client
        var clientConfig = new AmazonDynamoDBStreamsConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(_region)
        };

        var accessKey = GetConfigValue(config, DynamoDbConnectorConfig.AccessKeyConfig, "");
        var secretKey = GetConfigValue(config, DynamoDbConnectorConfig.SecretKeyConfig, "");
        var endpoint = GetConfigValue(config, DynamoDbConnectorConfig.EndpointConfig, "");

        if (!string.IsNullOrEmpty(endpoint))
        {
            clientConfig.ServiceURL = endpoint;
        }

        if (!string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey))
        {
            var credentials = new BasicAWSCredentials(accessKey, secretKey);
            _streamsClient = new AmazonDynamoDBStreamsClient(credentials, clientConfig);
        }
        else
        {
            _streamsClient = new AmazonDynamoDBStreamsClient(clientConfig);
        }

        RestoreOffset();
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    private static string ExtractTableNameFromArn(string streamArn)
    {
        // ARN format: arn:aws:dynamodb:region:account:table/tablename/stream/timestamp
        var parts = streamArn.Split('/');
        return parts.Length >= 2 ? parts[1] : "unknown";
    }

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
        _streamsClient?.Dispose();
        _streamsClient = null;
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
        if (_streamsClient == null)
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
        var request = new DescribeStreamRequest { StreamArn = _streamArn };
        var response = await _streamsClient!.DescribeStreamAsync(request, cancellationToken);

        foreach (var shard in response.StreamDescription.Shards)
        {
            await InitializeShardIteratorAsync(shard.ShardId, cancellationToken);
        }
    }

    private async Task InitializeShardIteratorAsync(string shardId, CancellationToken cancellationToken)
    {
        var request = new GetShardIteratorRequest
        {
            StreamArn = _streamArn,
            ShardId = shardId
        };

        // Check if we have a stored sequence number for this shard
        if (_lastSequenceNumbers.TryGetValue(shardId, out var sequenceNumber))
        {
            request.ShardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER;
            request.SequenceNumber = sequenceNumber;
        }
        else
        {
            request.ShardIteratorType = _shardIteratorType switch
            {
                DynamoDbConnectorConfig.ShardIteratorTrimHorizon => ShardIteratorType.TRIM_HORIZON,
                DynamoDbConnectorConfig.ShardIteratorLatest => ShardIteratorType.LATEST,
                _ => ShardIteratorType.LATEST
            };
        }

        var response = await _streamsClient!.GetShardIteratorAsync(request, cancellationToken);
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
            var response = await _streamsClient!.GetRecordsAsync(request, cancellationToken);

            foreach (var record in response.Records)
            {
                var sourceRecord = ConvertToSourceRecord(record, shardId);
                if (sourceRecord != null)
                {
                    records.Add(sourceRecord);
                    _lastSequenceNumbers[shardId] = record.Dynamodb.SequenceNumber;
                }
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

        return records;
    }

    private SourceRecord? ConvertToSourceRecord(StreamRecord record, string shardId)
    {
        var eventName = record.EventName.Value;

        // Convert DynamoDB attributes to JSON
        var newImage = record.Dynamodb.NewImage != null
            ? ConvertAttributeMapToJson(record.Dynamodb.NewImage)
            : null;

        var oldImage = record.Dynamodb.OldImage != null
            ? ConvertAttributeMapToJson(record.Dynamodb.OldImage)
            : null;

        var keys = record.Dynamodb.Keys != null
            ? ConvertAttributeMapToJson(record.Dynamodb.Keys)
            : new Dictionary<string, object?>();

        // Map DynamoDB event to Debezium operation
        var op = eventName switch
        {
            "INSERT" => "c",
            "MODIFY" => "u",
            "REMOVE" => "d",
            _ => "r"
        };

        var payload = new Dictionary<string, object?>
        {
            ["op"] = op,
            ["source"] = _includeMetadata ? new Dictionary<string, object>
            {
                ["table"] = _tableName,
                ["stream_arn"] = _streamArn,
                ["shard_id"] = shardId,
                ["sequence_number"] = record.Dynamodb.SequenceNumber,
                ["event_name"] = eventName,
                ["approximate_creation_time"] = record.Dynamodb.ApproximateCreationDateTime?.ToString("O") ?? ""
            } : new Dictionary<string, object> { ["table"] = _tableName },
            ["before"] = eventName == "INSERT" ? null : oldImage ?? keys,
            ["after"] = eventName == "REMOVE" ? null : newImage,
            ["ts_ms"] = record.Dynamodb.ApproximateCreationDateTime.HasValue
                ? new DateTimeOffset(record.Dynamodb.ApproximateCreationDateTime.Value).ToUnixTimeMilliseconds()
                : DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        // Build offset
        var offset = new Dictionary<string, object>
        {
            [$"shard:{shardId}"] = record.Dynamodb.SequenceNumber
        };

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = offset,
            Topic = GetTopicName(),
            Key = JsonSerializer.SerializeToUtf8Bytes(keys),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = record.Dynamodb.ApproximateCreationDateTime ?? DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                [DynamoDbConnectorConfig.HeaderTableName] = Encoding.UTF8.GetBytes(_tableName),
                [DynamoDbConnectorConfig.HeaderEventName] = Encoding.UTF8.GetBytes(eventName),
                [DynamoDbConnectorConfig.HeaderSequenceNumber] = Encoding.UTF8.GetBytes(record.Dynamodb.SequenceNumber),
                [DynamoDbConnectorConfig.HeaderShardId] = Encoding.UTF8.GetBytes(shardId)
            }
        };
    }

    private static Dictionary<string, object?> ConvertAttributeMapToJson(Dictionary<string, StreamAttributeValue> attributes)
    {
        var result = new Dictionary<string, object?>();

        foreach (var kvp in attributes)
        {
            result[kvp.Key] = ConvertAttributeValue(kvp.Value);
        }

        return result;
    }

    private static object? ConvertAttributeValue(StreamAttributeValue attr)
    {
        if (attr.NULL == true)
            return null;

        if (!string.IsNullOrEmpty(attr.S))
            return attr.S;

        if (!string.IsNullOrEmpty(attr.N))
        {
            if (decimal.TryParse(attr.N, out var num))
                return num;
            return attr.N;
        }

        if (attr.BOOL.HasValue)
            return attr.BOOL.Value;

        if (attr.B != null)
            return Convert.ToBase64String(attr.B.ToArray());

        if (attr.SS != null && attr.SS.Count > 0)
            return attr.SS;

        if (attr.NS != null && attr.NS.Count > 0)
            return attr.NS.Select(n => decimal.TryParse(n, out var d) ? (object)d : n).ToList();

        if (attr.BS != null && attr.BS.Count > 0)
            return attr.BS.Select(b => Convert.ToBase64String(b.ToArray())).ToList();

        if (attr.L != null && attr.L.Count > 0)
            return attr.L.Select(ConvertAttributeValue).ToList();

        if (attr.M != null && attr.M.Count > 0)
            return ConvertAttributeMapToJson(attr.M);

        return null;
    }

    private string GetTopicName()
    {
        return _topicPattern.Replace("${table}", _tableName);
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // DynamoDB Streams doesn't require acknowledgment
        // Position tracking is done via sequence numbers in offsets
        return Task.CompletedTask;
    }
}
