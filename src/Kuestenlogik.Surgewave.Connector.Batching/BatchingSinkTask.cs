namespace Kuestenlogik.Surgewave.Connector.Batching;

using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A sink task that aggregates incoming records into batches based on count, size, or timeout.
/// Batched records can be retrieved via the GetBatches method or via the OnBatchReady callback.
/// </summary>
public sealed class BatchingSinkTask : SinkTask
{
    private int _batchMaxMessages = BatchingConnectorConfig.DefaultBatchMaxMessages;
    private long _batchMaxBytes = BatchingConnectorConfig.DefaultBatchMaxBytes;
    private int _batchTimeoutMs = BatchingConnectorConfig.DefaultBatchTimeoutMs;
    private string _batchFormat = BatchingConnectorConfig.DefaultBatchFormat;
    private string _keyStrategy = BatchingConnectorConfig.DefaultKeyStrategy;
    private bool _includeMetadata = BatchingConnectorConfig.DefaultIncludeMetadata;
    private string _separator = BatchingConnectorConfig.DefaultSeparator;
    private bool _flushOnKeyChange = BatchingConnectorConfig.DefaultFlushOnKeyChange;
    private string _compression = BatchingConnectorConfig.DefaultCompression;

    private readonly List<SinkRecord> _buffer = [];
    private readonly List<byte[]> _keys = [];
    private readonly List<BatchedRecord> _completedBatches = [];
    private long _currentBatchBytes;
    private DateTime _batchStartTime = DateTime.MinValue;
    private byte[]? _lastKey;
    private bool _disposed;

    /// <summary>
    /// Event raised when a batch is ready.
    /// </summary>
#pragma warning disable CA1003 // Use generic event handler instances
    public event Action<BatchedRecord>? OnBatchReady;
#pragma warning restore CA1003

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
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
        // Flush any remaining records
        FlushInternal();

        _buffer.Clear();
        _keys.Clear();
        _currentBatchBytes = 0;
        _batchStartTime = DateTime.MinValue;
        _lastKey = null;
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            AddRecord(record);
        }

        // Check for timeout-based flush
        if (ShouldFlushByTimeout())
        {
            FlushInternal();
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Gets and clears all completed batches.
    /// </summary>
    public IReadOnlyList<BatchedRecord> GetBatches()
    {
        var batches = _completedBatches.ToList();
        _completedBatches.Clear();
        return batches;
    }

    /// <summary>
    /// Forces the current batch to be flushed regardless of policies.
    /// </summary>
    public BatchedRecord? Flush()
    {
        return FlushInternal();
    }

    /// <summary>
    /// Gets the current number of messages in the batch.
    /// </summary>
    public int CurrentBatchCount => _buffer.Count;

    /// <summary>
    /// Gets the current batch size in bytes.
    /// </summary>
    public long CurrentBatchBytes => _currentBatchBytes;

    private void AddRecord(SinkRecord record)
    {
        if (_batchStartTime == DateTime.MinValue)
            _batchStartTime = DateTime.UtcNow;

        var key = record.Key;
        var value = record.Value;

        // Check if we should flush due to key change
        if (_flushOnKeyChange && _lastKey != null && key != null)
        {
            if (!_lastKey.SequenceEqual(key))
            {
                FlushInternal();
                _batchStartTime = DateTime.UtcNow;
            }
        }

        // Add to buffer
        var messageSize = (value?.Length ?? 0) + (key?.Length ?? 0);

        // Check if adding this message would exceed max bytes
        if (_currentBatchBytes + messageSize > _batchMaxBytes && _buffer.Count > 0)
        {
            FlushInternal();
            _batchStartTime = DateTime.UtcNow;
        }

        _buffer.Add(record);
        if (key != null)
            _keys.Add(key);
        _currentBatchBytes += messageSize;
        _lastKey = key;

        // Check if batch is full by count
        if (_buffer.Count >= _batchMaxMessages)
        {
            FlushInternal();
        }
    }

    private bool ShouldFlushByTimeout()
    {
        if (_buffer.Count == 0)
            return false;

        if (_batchStartTime != DateTime.MinValue &&
            (DateTime.UtcNow - _batchStartTime).TotalMilliseconds >= _batchTimeoutMs)
            return true;

        return false;
    }

    private BatchedRecord? FlushInternal()
    {
        if (_buffer.Count == 0)
            return null;

        var record = CreateBatchedRecord();
        _completedBatches.Add(record);
        OnBatchReady?.Invoke(record);
        ResetBatch();
        return record;
    }

    private BatchedRecord CreateBatchedRecord()
    {
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

        return new BatchedRecord
        {
            Key = key,
            Value = value,
            Timestamp = DateTimeOffset.UtcNow,
            MessageCount = _buffer.Count,
            TotalBytes = _currentBatchBytes,
            IsCompressed = _compression == BatchingConnectorConfig.CompressionGzip
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
                    ["topic"] = record.Topic,
                    ["partition"] = record.Partition,
                    ["offset"] = record.Offset,
                    ["key"] = record.Key != null ? Convert.ToBase64String(record.Key) : null,
                    ["value"] = record.Value != null ? TryParseJson(record.Value) : null,
                    ["timestamp"] = record.Timestamp.ToString("O")
                };
                array.Add(obj);
            }
            else
            {
                if (record.Value != null)
                {
                    array.Add(TryParseJson(record.Value));
                }
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
                    ["topic"] = record.Topic,
                    ["partition"] = record.Partition,
                    ["offset"] = record.Offset,
                    ["key"] = record.Key != null ? Convert.ToBase64String(record.Key) : null,
                    ["value"] = record.Value != null ? TryParseJson(record.Value) : null,
                    ["timestamp"] = record.Timestamp.ToString("O")
                };
                sb.Append(obj.ToJsonString());
            }
            else
            {
                if (record.Value != null)
                {
                    var json = TryParseJson(record.Value);
                    sb.Append(json?.ToJsonString() ?? Encoding.UTF8.GetString(record.Value));
                }
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
                _completedBatches.Clear();
            }
            _disposed = true;
        }
        base.Dispose(disposing);
    }
}

/// <summary>
/// Represents a batched record containing aggregated messages.
/// </summary>
public sealed record BatchedRecord
{
    public byte[]? Key { get; init; }
    public required byte[] Value { get; init; }
    public DateTimeOffset Timestamp { get; init; }
    public int MessageCount { get; init; }
    public long TotalBytes { get; init; }
    public bool IsCompressed { get; init; }
}
