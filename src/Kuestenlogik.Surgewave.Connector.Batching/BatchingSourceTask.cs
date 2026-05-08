namespace Kuestenlogik.Surgewave.Connector.Batching;

using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A source task that aggregates messages into batches based on count, size, or timeout.
/// This is typically used as part of a pipeline to batch messages from an upstream source.
/// </summary>
public sealed class BatchingSourceTask : SourceTask
{
    private string _topic = "";
    private int _batchMaxMessages = BatchingConnectorConfig.DefaultBatchMaxMessages;
    private long _batchMaxBytes = BatchingConnectorConfig.DefaultBatchMaxBytes;
    private int _batchTimeoutMs = BatchingConnectorConfig.DefaultBatchTimeoutMs;
    private string _batchFormat = BatchingConnectorConfig.DefaultBatchFormat;
    private string _keyStrategy = BatchingConnectorConfig.DefaultKeyStrategy;
    private bool _includeMetadata = BatchingConnectorConfig.DefaultIncludeMetadata;
    private string _separator = BatchingConnectorConfig.DefaultSeparator;
    private bool _flushOnKeyChange = BatchingConnectorConfig.DefaultFlushOnKeyChange;
    private string _compression = BatchingConnectorConfig.DefaultCompression;

    private readonly List<SourceRecord> _buffer = [];
    private readonly List<byte[]> _keys = [];
    private long _currentBatchBytes;
    private DateTime _batchStartTime = DateTime.MinValue;
    private byte[]? _lastKey;
    private bool _disposed;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        if (config.TryGetValue(BatchingConnectorConfig.TopicsConfig, out var topics))
            _topic = topics.Split(',')[0].Trim();

        if (config.TryGetValue(BatchingConnectorConfig.BatchMaxMessagesConfig, out var maxMessages))
            _batchMaxMessages = int.Parse(maxMessages);

        if (config.TryGetValue(BatchingConnectorConfig.BatchMaxBytesConfig, out var maxBytes))
            _batchMaxBytes = long.Parse(maxBytes);

        if (config.TryGetValue(BatchingConnectorConfig.BatchTimeoutMsConfig, out var timeoutMs))
            _batchTimeoutMs = int.Parse(timeoutMs);

        if (config.TryGetValue(BatchingConnectorConfig.BatchFormatConfig, out var format))
            _batchFormat = format;

        if (config.TryGetValue(BatchingConnectorConfig.KeyStrategyConfig, out var keyStrategy))
            _keyStrategy = keyStrategy;

        if (config.TryGetValue(BatchingConnectorConfig.IncludeMetadataConfig, out var includeMetadata))
            _includeMetadata = bool.Parse(includeMetadata);

        if (config.TryGetValue(BatchingConnectorConfig.SeparatorConfig, out var separator))
            _separator = separator;

        if (config.TryGetValue(BatchingConnectorConfig.FlushOnKeyChangeConfig, out var flushOnKeyChange))
            _flushOnKeyChange = bool.Parse(flushOnKeyChange);

        if (config.TryGetValue(BatchingConnectorConfig.CompressionConfig, out var compression))
            _compression = compression;
    }

    public override void Stop()
    {
        _buffer.Clear();
        _keys.Clear();
        _currentBatchBytes = 0;
        _batchStartTime = DateTime.MinValue;
        _lastKey = null;
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        // This source task is designed to be used in a pipeline where
        // messages are added via the AddMessage method. The PollAsync
        // method checks if the batch is ready to be flushed.
        var records = new List<SourceRecord>();

        if (ShouldFlushBatch())
        {
            var batchRecord = CreateBatchRecord();
            if (batchRecord != null)
                records.Add(batchRecord);
            ResetBatch();
        }

        return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
    }

    /// <summary>
    /// Adds a message to the current batch.
    /// </summary>
    public IReadOnlyList<SourceRecord> AddMessage(byte[]? key, byte[]? value, DateTimeOffset timestamp)
    {
        var records = new List<SourceRecord>();

        if (_batchStartTime == DateTime.MinValue)
            _batchStartTime = DateTime.UtcNow;

        // Check if we should flush due to key change
        if (_flushOnKeyChange && _lastKey != null && key != null)
        {
            if (!_lastKey.SequenceEqual(key))
            {
                var batchRecord = CreateBatchRecord();
                if (batchRecord != null)
                    records.Add(batchRecord);
                ResetBatch();
                _batchStartTime = DateTime.UtcNow;
            }
        }

        // Add to buffer
        var messageSize = (value?.Length ?? 0) + (key?.Length ?? 0);

        // Check if adding this message would exceed max bytes
        if (_currentBatchBytes + messageSize > _batchMaxBytes && _buffer.Count > 0)
        {
            var batchRecord = CreateBatchRecord();
            if (batchRecord != null)
                records.Add(batchRecord);
            ResetBatch();
            _batchStartTime = DateTime.UtcNow;
        }

        var record = new SourceRecord
        {
            Topic = _topic,
            Key = key,
            Value = value ?? [],
            Timestamp = timestamp,
            SourcePartition = new Dictionary<string, object>(),
            SourceOffset = new Dictionary<string, object>()
        };

        _buffer.Add(record);
        if (key != null)
            _keys.Add(key);
        _currentBatchBytes += messageSize;
        _lastKey = key;

        // Check if batch is full by count
        if (_buffer.Count >= _batchMaxMessages)
        {
            var batchRecord = CreateBatchRecord();
            if (batchRecord != null)
                records.Add(batchRecord);
            ResetBatch();
        }

        return records;
    }

    /// <summary>
    /// Forces the current batch to be flushed regardless of policies.
    /// </summary>
    public SourceRecord? Flush()
    {
        if (_buffer.Count == 0)
            return null;

        var record = CreateBatchRecord();
        ResetBatch();
        return record;
    }

    /// <summary>
    /// Gets the current number of messages in the batch.
    /// </summary>
    public int CurrentBatchCount => _buffer.Count;

    /// <summary>
    /// Gets the current batch size in bytes.
    /// </summary>
    public long CurrentBatchBytes => _currentBatchBytes;

    private bool ShouldFlushBatch()
    {
        if (_buffer.Count == 0)
            return false;

        // Flush by count
        if (_buffer.Count >= _batchMaxMessages)
            return true;

        // Flush by size
        if (_currentBatchBytes >= _batchMaxBytes)
            return true;

        // Flush by timeout
        if (_batchStartTime != DateTime.MinValue &&
            (DateTime.UtcNow - _batchStartTime).TotalMilliseconds >= _batchTimeoutMs)
            return true;

        return false;
    }

    private SourceRecord? CreateBatchRecord()
    {
        if (_buffer.Count == 0)
            return null;

        byte[] value;
        byte[]? key = DetermineKey();

        switch (_batchFormat)
        {
            case BatchingConnectorConfig.FormatJsonArray:
                value = CreateJsonArrayBatch();
                break;
            case BatchingConnectorConfig.FormatJsonLines:
                value = CreateJsonLinesBatch();
                break;
            case BatchingConnectorConfig.FormatRaw:
                value = CreateRawBatch();
                break;
            default:
                value = CreateJsonArrayBatch();
                break;
        }

        if (_compression == BatchingConnectorConfig.CompressionGzip)
        {
            value = CompressGzip(value);
        }

        return new SourceRecord
        {
            Topic = _topic,
            Key = key,
            Value = value,
            Timestamp = DateTimeOffset.UtcNow,
            SourcePartition = new Dictionary<string, object> { ["batch"] = true },
            SourceOffset = new Dictionary<string, object>
            {
                ["batchCount"] = _buffer.Count,
                ["batchBytes"] = _currentBatchBytes,
                ["batchTime"] = DateTime.UtcNow.ToString("O")
            }
        };
    }

    private byte[]? DetermineKey()
    {
        return _keyStrategy switch
        {
            BatchingConnectorConfig.KeyStrategyFirst => _keys.Count > 0 ? _keys[0] : null,
            BatchingConnectorConfig.KeyStrategyLast => _keys.Count > 0 ? _keys[^1] : null,
            BatchingConnectorConfig.KeyStrategyNull => null,
            BatchingConnectorConfig.KeyStrategyConcat => ConcatenateKeys(),
            _ => _keys.Count > 0 ? _keys[0] : null
        };
    }

    private byte[]? ConcatenateKeys()
    {
        if (_keys.Count == 0)
            return null;

        var separatorBytes = Encoding.UTF8.GetBytes(_separator);
        var totalLength = _keys.Sum(k => k.Length) + separatorBytes.Length * (_keys.Count - 1);
        var result = new byte[totalLength];
        var offset = 0;

        for (var i = 0; i < _keys.Count; i++)
        {
            if (i > 0)
            {
                Array.Copy(separatorBytes, 0, result, offset, separatorBytes.Length);
                offset += separatorBytes.Length;
            }
            Array.Copy(_keys[i], 0, result, offset, _keys[i].Length);
            offset += _keys[i].Length;
        }

        return result;
    }

    private byte[] CreateJsonArrayBatch()
    {
        var array = new JsonArray();

        foreach (var record in _buffer)
        {
            if (_includeMetadata)
            {
                var obj = new JsonObject
                {
                    ["key"] = record.Key != null ? Convert.ToBase64String(record.Key) : null,
                    ["value"] = TryParseJson(record.Value!),
                    ["timestamp"] = record.Timestamp?.ToString("O")
                };
                array.Add(obj);
            }
            else
            {
                array.Add(TryParseJson(record.Value!));
            }
        }

        return Encoding.UTF8.GetBytes(array.ToJsonString());
    }

    private byte[] CreateJsonLinesBatch()
    {
        var sb = new StringBuilder();

        foreach (var record in _buffer)
        {
            if (sb.Length > 0)
                sb.Append(_separator);

            if (_includeMetadata)
            {
                var obj = new JsonObject
                {
                    ["key"] = record.Key != null ? Convert.ToBase64String(record.Key) : null,
                    ["value"] = TryParseJson(record.Value!),
                    ["timestamp"] = record.Timestamp?.ToString("O")
                };
                sb.Append(obj.ToJsonString());
            }
            else
            {
                var json = TryParseJson(record.Value!);
                sb.Append(json?.ToJsonString() ?? Encoding.UTF8.GetString(record.Value!));
            }
        }

        return Encoding.UTF8.GetBytes(sb.ToString());
    }

    private byte[] CreateRawBatch()
    {
        var separatorBytes = Encoding.UTF8.GetBytes(_separator);
        var totalLength = _buffer.Sum(r => r.Value?.Length ?? 0) + separatorBytes.Length * (_buffer.Count - 1);
        var result = new byte[totalLength];
        var offset = 0;

        for (var i = 0; i < _buffer.Count; i++)
        {
            if (i > 0)
            {
                Array.Copy(separatorBytes, 0, result, offset, separatorBytes.Length);
                offset += separatorBytes.Length;
            }

            if (_buffer[i].Value != null)
            {
                Array.Copy(_buffer[i].Value!, 0, result, offset, _buffer[i].Value!.Length);
                offset += _buffer[i].Value!.Length;
            }
        }

        return result;
    }

    private static JsonNode? TryParseJson(byte[] data)
    {
        try
        {
            return JsonNode.Parse(data);
        }
        catch
        {
            return JsonValue.Create(Convert.ToBase64String(data));
        }
    }

    private static byte[] CompressGzip(byte[] data)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal))
        {
            gzip.Write(data, 0, data.Length);
        }
        return output.ToArray();
    }

    private void ResetBatch()
    {
        _buffer.Clear();
        _keys.Clear();
        _currentBatchBytes = 0;
        _batchStartTime = DateTime.MinValue;
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                _buffer.Clear();
                _keys.Clear();
            }
            _disposed = true;
        }
        base.Dispose(disposing);
    }
}
