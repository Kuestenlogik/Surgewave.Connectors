using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.S3;

internal static class DictionaryExtensions
{
    public static string GetOrDefault(this IDictionary<string, string> dict, string key, string defaultValue)
    {
        return dict.TryGetValue(key, out var value) ? value : defaultValue;
    }
}

/// <summary>
/// A source connector that reads objects from S3-compatible storage.
/// Supports listing and reading objects with various formats.
/// </summary>
public sealed class S3SourceConnector : SourceConnector
{
    private const string BucketConfig = "s3.bucket.name";
    private const string PrefixConfig = "s3.prefix";
    private const string RegionConfig = "s3.region";
    private const string EndpointConfig = "s3.endpoint";
    private const string AccessKeyConfig = "s3.access.key";
    private const string SecretKeyConfig = "s3.secret.key";
    private const string TopicConfig = "topic";
    private const string FormatConfig = "format";
    private const string PollIntervalMsConfig = "poll.interval.ms";
    private const string ModeConfig = "mode";
    private const string DeleteAfterReadConfig = "delete.after.read";

    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(S3SourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(BucketConfig, ConfigType.String, Importance.High, "S3 bucket name")
        .Define(TopicConfig, ConfigType.String, Importance.High, "Topic to write to", EditorHint.Topic)
        .Define(PrefixConfig, ConfigType.String, "", Importance.Medium, "S3 object prefix filter")
        .Define(RegionConfig, ConfigType.String, "us-east-1", Importance.Medium, "AWS region", EditorHint.Select, options: ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1"])
        .Define(EndpointConfig, ConfigType.String, "", Importance.Low, "Custom S3 endpoint (for MinIO, etc.)")
        .Define(AccessKeyConfig, ConfigType.Password, Importance.High, "AWS access key ID")
        .Define(SecretKeyConfig, ConfigType.Password, Importance.High, "AWS secret access key")
        .Define(FormatConfig, ConfigType.String, "json", Importance.Medium, "Object format: json, jsonlines, csv, raw", EditorHint.Select, options: ["json", "jsonlines", "csv", "parquet", "avro", "raw"])
        .Define(PollIntervalMsConfig, ConfigType.Long, 10000L, Importance.Medium, "Poll interval in milliseconds")
        .Define(ModeConfig, ConfigType.String, "list", Importance.Medium, "Mode: list (poll for new objects), event (use S3 events)", EditorHint.Select, options: ["list", "event"])
        .Define(DeleteAfterReadConfig, ConfigType.Boolean, false, Importance.Medium, "Delete objects after reading");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(BucketConfig, out var _))
        {
            throw new ArgumentException($"Missing required config: {BucketConfig}");
        }
        if (!config.TryGetValue(TopicConfig, out var _))
        {
            throw new ArgumentException($"Missing required config: {TopicConfig}");
        }

        foreach (var kvp in config)
        {
            _config[kvp.Key] = kvp.Value;
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task for simplicity. Could partition by prefix for parallelism.
        return [new Dictionary<string, string>(_config)];
    }
}

/// <summary>
/// Task that reads objects from S3.
/// </summary>
public sealed class S3SourceTask : SourceTask
{
    private const string BucketConfig = "s3.bucket.name";
    private const string PrefixConfig = "s3.prefix";
    private const string RegionConfig = "s3.region";
    private const string EndpointConfig = "s3.endpoint";
    private const string AccessKeyConfig = "s3.access.key";
    private const string SecretKeyConfig = "s3.secret.key";
    private const string TopicConfig = "topic";
    private const string FormatConfig = "format";
    private const string PollIntervalMsConfig = "poll.interval.ms";
    private const string DeleteAfterReadConfig = "delete.after.read";
    private const string LastKeyField = "last_key";
    private const string LastModifiedField = "last_modified";

    public override string Version => "1.0.0";

    private string _bucket = "";
    private string _prefix = "";
    private string _topic = "";
    private string _format = "json";
    private long _pollIntervalMs = 10000;
    private bool _deleteAfterRead;
    private DateTimeOffset _lastPollTime = DateTimeOffset.MinValue;
    private readonly Dictionary<string, object> _sourcePartition = new();
    private string _lastProcessedKey = "";
    private DateTimeOffset _lastModifiedTime = DateTimeOffset.MinValue;
    private readonly HashSet<string> _processedKeys = new();
    private AmazonS3Client? _s3Client;

    public override void Start(IDictionary<string, string> config)
    {
        _bucket = config[BucketConfig];
        _prefix = config.GetOrDefault(PrefixConfig, "");
        var region = config.GetOrDefault(RegionConfig, "us-east-1");
        var endpoint = config.GetOrDefault(EndpointConfig, "");
        var accessKey = config.GetOrDefault(AccessKeyConfig, "");
        var secretKey = config.GetOrDefault(SecretKeyConfig, "");
        _topic = config[TopicConfig];
        _format = config.GetOrDefault(FormatConfig, "json");
        _pollIntervalMs = long.Parse(config.GetOrDefault(PollIntervalMsConfig, "10000"));
        _deleteAfterRead = bool.Parse(config.GetOrDefault(DeleteAfterReadConfig, "false"));

        _sourcePartition["bucket"] = _bucket;
        _sourcePartition["prefix"] = _prefix;

        // Restore offset
        var storedOffset = Context.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null)
        {
            if (storedOffset.TryGetValue(LastKeyField, out var lastKey))
            {
                _lastProcessedKey = lastKey.ToString() ?? "";
            }
            if (storedOffset.TryGetValue(LastModifiedField, out var lastMod))
            {
                _lastModifiedTime = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(lastMod));
            }
        }

        // Create S3 client
        _s3Client = CreateS3Client(accessKey, secretKey, region, endpoint);
    }

    private static AmazonS3Client CreateS3Client(string accessKey, string secretKey, string region, string endpoint)
    {
        var config = new AmazonS3Config
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(region)
        };

        if (!string.IsNullOrEmpty(endpoint))
        {
            config.ServiceURL = endpoint;
            config.ForcePathStyle = true; // Required for MinIO and other S3-compatible services
        }

        if (!string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey))
        {
            return new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), config);
        }

        return new AmazonS3Client(config); // Use default credential chain
    }

    public override void Stop()
    {
        _s3Client?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _s3Client?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var elapsed = (now - _lastPollTime).TotalMilliseconds;

        if (elapsed < _pollIntervalMs)
        {
            var waitTime = (int)(_pollIntervalMs - elapsed);
            await Task.Delay(waitTime, cancellationToken);
        }

        _lastPollTime = DateTimeOffset.UtcNow;

        if (_s3Client == null)
        {
            return [];
        }

        var records = new List<SourceRecord>();

        // List objects in bucket
        var listRequest = new ListObjectsV2Request
        {
            BucketName = _bucket,
            Prefix = _prefix,
            StartAfter = _lastProcessedKey
        };

        var response = await _s3Client.ListObjectsV2Async(listRequest, cancellationToken);

        foreach (var s3Object in response.S3Objects.OrderBy(o => o.LastModified))
        {
            // Skip already processed objects
            if (_processedKeys.Contains(s3Object.Key))
                continue;

            // Skip objects older than last modified time (for resumability)
            if (s3Object.LastModified <= _lastModifiedTime.UtcDateTime && !string.IsNullOrEmpty(_lastProcessedKey))
                continue;

            // Download and process object
            var getRequest = new GetObjectRequest
            {
                BucketName = _bucket,
                Key = s3Object.Key
            };

            using var getResponse = await _s3Client.GetObjectAsync(getRequest, cancellationToken);
            using var reader = new StreamReader(getResponse.ResponseStream);
            var content = await reader.ReadToEndAsync(cancellationToken);

            var parsedRecords = ParseContent(s3Object.Key, content);
            records.AddRange(parsedRecords);

            // Track processed key
            _lastProcessedKey = s3Object.Key;
            _lastModifiedTime = s3Object.LastModified.HasValue
                ? new DateTimeOffset(s3Object.LastModified.Value, TimeSpan.Zero)
                : DateTimeOffset.UtcNow;
            _processedKeys.Add(s3Object.Key);

            // Delete if configured
            if (_deleteAfterRead)
            {
                await _s3Client.DeleteObjectAsync(_bucket, s3Object.Key, cancellationToken);
            }
        }

        return records;
    }

    private IEnumerable<SourceRecord> ParseContent(string objectKey, string content)
    {
        var sourceOffset = new Dictionary<string, object>
        {
            [LastKeyField] = _lastProcessedKey,
            [LastModifiedField] = _lastModifiedTime.ToUnixTimeMilliseconds()
        };

        switch (_format.ToLowerInvariant())
        {
            case "jsonlines":
                foreach (var line in content.Split('\n', StringSplitOptions.RemoveEmptyEntries))
                {
                    yield return new SourceRecord
                    {
                        SourcePartition = _sourcePartition,
                        SourceOffset = sourceOffset,
                        Topic = _topic,
                        Key = Encoding.UTF8.GetBytes(objectKey),
                        Value = Encoding.UTF8.GetBytes(line.Trim())
                    };
                }
                break;

            case "csv":
                var lines = content.Split('\n', StringSplitOptions.RemoveEmptyEntries);
                if (lines.Length == 0) yield break;

                var headers = lines[0].Split(',').Select(h => h.Trim()).ToArray();
                for (int i = 1; i < lines.Length; i++)
                {
                    var values = lines[i].Split(',');
                    var row = new Dictionary<string, string>();
                    for (int j = 0; j < headers.Length && j < values.Length; j++)
                    {
                        row[headers[j]] = values[j].Trim();
                    }
                    yield return new SourceRecord
                    {
                        SourcePartition = _sourcePartition,
                        SourceOffset = sourceOffset,
                        Topic = _topic,
                        Key = Encoding.UTF8.GetBytes(objectKey),
                        Value = JsonSerializer.SerializeToUtf8Bytes(row)
                    };
                }
                break;

            case "raw":
                yield return new SourceRecord
                {
                    SourcePartition = _sourcePartition,
                    SourceOffset = sourceOffset,
                    Topic = _topic,
                    Key = Encoding.UTF8.GetBytes(objectKey),
                    Value = Encoding.UTF8.GetBytes(content)
                };
                break;

            default: // json
                // Parse as JSON array or single object
                foreach (var record in ParseJsonContent(objectKey, content, sourceOffset))
                {
                    yield return record;
                }
                break;
        }
    }

    private List<SourceRecord> ParseJsonContent(string objectKey, string content, Dictionary<string, object> sourceOffset)
    {
        var records = new List<SourceRecord>();
        try
        {
            using var doc = JsonDocument.Parse(content);
            if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in doc.RootElement.EnumerateArray())
                {
                    records.Add(new SourceRecord
                    {
                        SourcePartition = _sourcePartition,
                        SourceOffset = sourceOffset,
                        Topic = _topic,
                        Key = Encoding.UTF8.GetBytes(objectKey),
                        Value = Encoding.UTF8.GetBytes(element.GetRawText())
                    });
                }
            }
            else
            {
                records.Add(new SourceRecord
                {
                    SourcePartition = _sourcePartition,
                    SourceOffset = sourceOffset,
                    Topic = _topic,
                    Key = Encoding.UTF8.GetBytes(objectKey),
                    Value = Encoding.UTF8.GetBytes(content)
                });
            }
        }
        catch (JsonException)
        {
            // If not valid JSON, treat as raw
            records.Add(new SourceRecord
            {
                SourcePartition = _sourcePartition,
                SourceOffset = sourceOffset,
                Topic = _topic,
                Key = Encoding.UTF8.GetBytes(objectKey),
                Value = Encoding.UTF8.GetBytes(content)
            });
        }
        return records;
    }
}

/// <summary>
/// A sink connector that writes records to S3-compatible storage.
/// Supports various formats and partitioning strategies.
/// </summary>
public sealed class S3SinkConnector : SinkConnector
{
    private const string BucketConfig = "s3.bucket.name";
    private const string PrefixConfig = "s3.prefix";
    private const string RegionConfig = "s3.region";
    private const string EndpointConfig = "s3.endpoint";
    private const string AccessKeyConfig = "s3.access.key";
    private const string SecretKeyConfig = "s3.secret.key";
    private const string TopicsConfig = "topics";
    private const string FormatConfig = "format";
    private const string PartitionerConfig = "partitioner";
    private const string FlushSizeConfig = "flush.size";
    private const string RotateIntervalMsConfig = "rotate.interval.ms";
    private const string TimezoneConfig = "timezone";

    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(S3SinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(BucketConfig, ConfigType.String, Importance.High, "S3 bucket name")
        .Define(TopicsConfig, ConfigType.String, Importance.High, "Topics to consume from", EditorHint.Topic)
        .Define(PrefixConfig, ConfigType.String, "", Importance.Medium, "S3 object prefix")
        .Define(RegionConfig, ConfigType.String, "us-east-1", Importance.Medium, "AWS region", EditorHint.Select, options: ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1"])
        .Define(EndpointConfig, ConfigType.String, "", Importance.Low, "Custom S3 endpoint (for MinIO, etc.)")
        .Define(AccessKeyConfig, ConfigType.Password, Importance.High, "AWS access key ID")
        .Define(SecretKeyConfig, ConfigType.Password, Importance.High, "AWS secret access key")
        .Define(FormatConfig, ConfigType.String, "json", Importance.Medium, "Output format: json, jsonlines, parquet, avro", EditorHint.Select, options: ["json", "jsonlines", "csv", "parquet", "avro", "raw"])
        .Define(PartitionerConfig, ConfigType.String, "default", Importance.Medium, "Partitioner: default, field, time, custom")
        .Define(FlushSizeConfig, ConfigType.Int, 1000, Importance.Medium, "Number of records before flushing to S3")
        .Define(RotateIntervalMsConfig, ConfigType.Long, 3600000L, Importance.Medium, "Maximum time before rotating file (ms)")
        .Define(TimezoneConfig, ConfigType.String, "UTC", Importance.Low, "Timezone for time-based partitioning");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(BucketConfig, out var _))
        {
            throw new ArgumentException($"Missing required config: {BucketConfig}");
        }
        if (!config.TryGetValue(TopicsConfig, out var _))
        {
            throw new ArgumentException($"Missing required config: {TopicsConfig}");
        }

        foreach (var kvp in config)
        {
            _config[kvp.Key] = kvp.Value;
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}

/// <summary>
/// Task that writes records to S3.
/// </summary>
public sealed class S3SinkTask : SinkTask
{
    private const string BucketConfig = "s3.bucket.name";
    private const string PrefixConfig = "s3.prefix";
    private const string RegionConfig = "s3.region";
    private const string EndpointConfig = "s3.endpoint";
    private const string AccessKeyConfig = "s3.access.key";
    private const string SecretKeyConfig = "s3.secret.key";
    private const string FormatConfig = "format";
    private const string PartitionerConfig = "partitioner";
    private const string FlushSizeConfig = "flush.size";
    private const string RotateIntervalMsConfig = "rotate.interval.ms";

    public override string Version => "1.0.0";

    private string _bucket = "";
    private string _prefix = "";
    private string _format = "json";
    private string _partitioner = "default";
    private int _flushSize = 1000;
    private long _rotateIntervalMs = 3600000;
    private DateTimeOffset _lastRotateTime = DateTimeOffset.UtcNow;
    private readonly Dictionary<string, List<SinkRecord>> _buffers = new();
    private AmazonS3Client? _s3Client;

    public override void Start(IDictionary<string, string> config)
    {
        _bucket = config[BucketConfig];
        _prefix = config.GetOrDefault(PrefixConfig, "");
        var region = config.GetOrDefault(RegionConfig, "us-east-1");
        var endpoint = config.GetOrDefault(EndpointConfig, "");
        var accessKey = config.GetOrDefault(AccessKeyConfig, "");
        var secretKey = config.GetOrDefault(SecretKeyConfig, "");
        _format = config.GetOrDefault(FormatConfig, "json");
        _partitioner = config.GetOrDefault(PartitionerConfig, "default");
        _flushSize = int.Parse(config.GetOrDefault(FlushSizeConfig, "1000"));
        _rotateIntervalMs = long.Parse(config.GetOrDefault(RotateIntervalMsConfig, "3600000"));

        // Create S3 client
        _s3Client = CreateS3Client(accessKey, secretKey, region, endpoint);
    }

    private static AmazonS3Client CreateS3Client(string accessKey, string secretKey, string region, string endpoint)
    {
        var config = new AmazonS3Config
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(region)
        };

        if (!string.IsNullOrEmpty(endpoint))
        {
            config.ServiceURL = endpoint;
            config.ForcePathStyle = true;
        }

        if (!string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey))
        {
            return new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), config);
        }

        return new AmazonS3Client(config);
    }

    public override void Stop()
    {
        FlushAllBuffersAsync().GetAwaiter().GetResult();
        _s3Client?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _s3Client?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var partitionKey = GetPartitionKey(record);
            if (!_buffers.TryGetValue(partitionKey, out var buffer))
            {
                buffer = new List<SinkRecord>();
                _buffers[partitionKey] = buffer;
            }
            buffer.Add(record);

            if (buffer.Count >= _flushSize)
            {
                await FlushBufferAsync(partitionKey, buffer, cancellationToken);
            }
        }

        // Check rotation interval
        if ((DateTimeOffset.UtcNow - _lastRotateTime).TotalMilliseconds >= _rotateIntervalMs)
        {
            await FlushAllBuffersAsync(cancellationToken);
            _lastRotateTime = DateTimeOffset.UtcNow;
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        await FlushAllBuffersAsync(cancellationToken);
    }

    private async Task FlushAllBuffersAsync(CancellationToken cancellationToken = default)
    {
        foreach (var (key, buffer) in _buffers.ToList())
        {
            if (buffer.Count > 0)
            {
                await FlushBufferAsync(key, buffer, cancellationToken);
            }
        }
    }

    private async Task FlushBufferAsync(string partitionKey, List<SinkRecord> buffer, CancellationToken cancellationToken)
    {
        if (buffer.Count == 0 || _s3Client == null) return;

        var objectKey = GenerateObjectKey(partitionKey, buffer[0]);
        var content = SerializeRecords(buffer);

        var putRequest = new PutObjectRequest
        {
            BucketName = _bucket,
            Key = objectKey,
            ContentType = GetContentType(),
            InputStream = new MemoryStream(content)
        };

        await _s3Client.PutObjectAsync(putRequest, cancellationToken);

        buffer.Clear();
    }

    private byte[] SerializeRecords(List<SinkRecord> records)
    {
        return _format.ToLowerInvariant() switch
        {
            "jsonlines" => SerializeJsonLines(records),
            _ => SerializeJson(records)
        };
    }

    private string GetContentType()
    {
        return _format.ToLowerInvariant() switch
        {
            "jsonlines" => "application/x-ndjson",
            "json" => "application/json",
            "parquet" => "application/octet-stream",
            "avro" => "application/avro",
            _ => "application/json"
        };
    }

    private string GetPartitionKey(SinkRecord record)
    {
        return _partitioner switch
        {
            "time" => $"{record.Topic}/{record.Timestamp:yyyy/MM/dd/HH}",
            "field" => $"{record.Topic}/{record.Partition}",
            _ => $"{record.Topic}/{record.Partition}"
        };
    }

    private string GenerateObjectKey(string partitionKey, SinkRecord sampleRecord)
    {
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var extension = _format.ToLowerInvariant() switch
        {
            "jsonlines" => "jsonl",
            "json" => "json",
            "parquet" => "parquet",
            "avro" => "avro",
            _ => "json"
        };
        var prefix = string.IsNullOrEmpty(_prefix) ? "" : _prefix.TrimEnd('/') + "/";
        return $"{prefix}{partitionKey}/{timestamp}.{extension}";
    }

    private static byte[] SerializeJsonLines(List<SinkRecord> records)
    {
        var sb = new StringBuilder();
        foreach (var record in records)
        {
            var value = Encoding.UTF8.GetString(record.Value);
            sb.AppendLine(value);
        }
        return Encoding.UTF8.GetBytes(sb.ToString());
    }

    private static byte[] SerializeJson(List<SinkRecord> records)
    {
        var items = records.Select(r =>
        {
            try
            {
                using var doc = JsonDocument.Parse(r.Value);
                return doc.RootElement.Clone();
            }
            catch
            {
                return JsonDocument.Parse($"\"{Encoding.UTF8.GetString(r.Value)}\"").RootElement.Clone();
            }
        }).ToList();

        return JsonSerializer.SerializeToUtf8Bytes(items);
    }
}
