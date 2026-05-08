namespace Kuestenlogik.Surgewave.Connector.Gcp.Storage;

using System.Text;
using System.Text.Json;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that writes records to Google Cloud Storage.
/// </summary>
public sealed class GcsSinkTask : SinkTask
{
    private StorageClient? _storageClient;
    private string _bucketName = "";
    private string _prefix = "";
    private string _format = GcsConnectorConfig.DefaultFormat;
    private string _partitioner = GcsConnectorConfig.DefaultPartitioner;
    private int _flushSize = GcsConnectorConfig.DefaultFlushSize;
    private long _rotateIntervalMs = GcsConnectorConfig.DefaultRotateIntervalMs;

    private readonly Dictionary<string, List<SinkRecord>> _buffers = new();
    private readonly Dictionary<string, DateTimeOffset> _bufferStartTimes = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _bucketName = config[GcsConnectorConfig.BucketNameConfig];
        _prefix = GetConfigValue(config, GcsConnectorConfig.PrefixConfig, "");
        _format = GetConfigValue(config, GcsConnectorConfig.FormatConfig, GcsConnectorConfig.DefaultFormat);
        _partitioner = GetConfigValue(config, GcsConnectorConfig.PartitionerConfig, GcsConnectorConfig.DefaultPartitioner);
        _flushSize = GetConfigInt(config, GcsConnectorConfig.FlushSizeConfig, GcsConnectorConfig.DefaultFlushSize);
        _rotateIntervalMs = GetConfigLong(config, GcsConnectorConfig.RotateIntervalMsConfig, GcsConnectorConfig.DefaultRotateIntervalMs);

        var credentialsJson = GetConfigValue(config, GcsConnectorConfig.CredentialsJsonConfig, "");
        var credentialsFile = GetConfigValue(config, GcsConnectorConfig.CredentialsFileConfig, "");

        _storageClient = CreateStorageClient(credentialsJson, credentialsFile);
    }

    private static StorageClient CreateStorageClient(string credentialsJson, string credentialsFile)
    {
#pragma warning disable CS0618 // GoogleCredential.FromJson/FromFile are deprecated but still functional
        if (!string.IsNullOrEmpty(credentialsJson))
        {
            var credential = GoogleCredential.FromJson(credentialsJson);
            return StorageClient.Create(credential);
        }

        if (!string.IsNullOrEmpty(credentialsFile))
        {
            var credential = GoogleCredential.FromFile(credentialsFile);
            return StorageClient.Create(credential);
        }
#pragma warning restore CS0618

        // Use default credentials (ADC - Application Default Credentials)
        return StorageClient.Create();
    }

    public override void Stop()
    {
        // Flush remaining buffers
        FlushAllBuffersAsync(CancellationToken.None).GetAwaiter().GetResult();

        _storageClient?.Dispose();
        _storageClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _storageClient?.Dispose();
            _storageClient = null;
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_storageClient == null || records.Count == 0)
            return;

        foreach (var record in records)
        {
            var partitionKey = GetPartitionKey(record);

            if (!_buffers.TryGetValue(partitionKey, out var buffer))
            {
                buffer = new List<SinkRecord>();
                _buffers[partitionKey] = buffer;
                _bufferStartTimes[partitionKey] = DateTimeOffset.UtcNow;
            }

            buffer.Add(record);

            // Check if we need to flush this buffer
            if (buffer.Count >= _flushSize)
            {
                await FlushBufferAsync(partitionKey, buffer, cancellationToken);
                buffer.Clear();
                _bufferStartTimes[partitionKey] = DateTimeOffset.UtcNow;
            }
        }

        // Check rotation intervals for all buffers
        await CheckRotationIntervalsAsync(cancellationToken);
    }

    private async Task CheckRotationIntervalsAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var keysToFlush = new List<string>();

        foreach (var kvp in _bufferStartTimes)
        {
            if (_buffers.TryGetValue(kvp.Key, out var buffer) && buffer.Count > 0)
            {
                var elapsed = (now - kvp.Value).TotalMilliseconds;
                if (elapsed >= _rotateIntervalMs)
                {
                    keysToFlush.Add(kvp.Key);
                }
            }
        }

        foreach (var key in keysToFlush)
        {
            if (_buffers.TryGetValue(key, out var buffer) && buffer.Count > 0)
            {
                await FlushBufferAsync(key, buffer, cancellationToken);
                buffer.Clear();
                _bufferStartTimes[key] = DateTimeOffset.UtcNow;
            }
        }
    }

    private async Task FlushAllBuffersAsync(CancellationToken cancellationToken)
    {
        foreach (var kvp in _buffers)
        {
            if (kvp.Value.Count > 0)
            {
                await FlushBufferAsync(kvp.Key, kvp.Value, cancellationToken);
            }
        }
        _buffers.Clear();
        _bufferStartTimes.Clear();
    }

    private async Task FlushBufferAsync(string partitionKey, List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_storageClient == null || records.Count == 0)
            return;

        var content = SerializeRecords(records);
        var objectName = GenerateObjectName(partitionKey, records[0]);

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
        var contentType = _format == GcsConnectorConfig.FormatJson ? "application/json" : "text/plain";

        await _storageClient.UploadObjectAsync(
            _bucketName,
            objectName,
            contentType,
            stream,
            cancellationToken: cancellationToken);
    }

    private string SerializeRecords(List<SinkRecord> records)
    {
        if (_format == GcsConnectorConfig.FormatJsonLines)
        {
            var sb = new StringBuilder();
            foreach (var record in records)
            {
                if (record.Value != null)
                {
                    sb.AppendLine(Encoding.UTF8.GetString(record.Value));
                }
            }
            return sb.ToString();
        }

        // JSON array format
        var jsonRecords = new List<object?>();
        foreach (var record in records)
        {
            if (record.Value != null)
            {
                try
                {
                    var json = JsonSerializer.Deserialize<JsonElement>(record.Value);
                    jsonRecords.Add(json);
                }
                catch
                {
                    jsonRecords.Add(Encoding.UTF8.GetString(record.Value));
                }
            }
        }
        return JsonSerializer.Serialize(jsonRecords);
    }

    private string GetPartitionKey(SinkRecord record)
    {
        return _partitioner switch
        {
            GcsConnectorConfig.PartitionerTime => GetTimePartitionKey(),
            GcsConnectorConfig.PartitionerField => GetFieldPartitionKey(record),
            _ => $"{record.Topic ?? "unknown"}/{record.Partition}"
        };
    }

    private static string GetTimePartitionKey()
    {
        var now = DateTimeOffset.UtcNow;
        return $"year={now.Year}/month={now.Month:D2}/day={now.Day:D2}/hour={now.Hour:D2}";
    }

    private static string GetFieldPartitionKey(SinkRecord record)
    {
        // Try to extract a field from the value for partitioning
        if (record.Value != null)
        {
            try
            {
                using var doc = JsonDocument.Parse(record.Value);
                if (doc.RootElement.TryGetProperty("id", out var idProp))
                {
                    return idProp.ToString();
                }
                if (doc.RootElement.TryGetProperty("key", out var keyProp))
                {
                    return keyProp.ToString();
                }
            }
            catch
            {
                // Not JSON, use default
            }
        }

        return $"{record.Topic ?? "unknown"}/{record.Partition}";
    }

    private string GenerateObjectName(string partitionKey, SinkRecord record)
    {
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var extension = _format == GcsConnectorConfig.FormatJson ? "json" : "jsonl";

        var parts = new List<string>();

        if (!string.IsNullOrEmpty(_prefix))
        {
            parts.Add(_prefix.TrimEnd('/'));
        }

        if (!string.IsNullOrEmpty(record.Topic))
        {
            parts.Add(record.Topic);
        }

        parts.Add(partitionKey);
        parts.Add($"{timestamp}.{extension}");

        return string.Join("/", parts);
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        await FlushAllBuffersAsync(cancellationToken);
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static int GetConfigInt(IDictionary<string, string> config, string key, int defaultValue)
        => config.TryGetValue(key, out var value) && int.TryParse(value, out var intValue) ? intValue : defaultValue;

    private static long GetConfigLong(IDictionary<string, string> config, string key, long defaultValue)
        => config.TryGetValue(key, out var value) && long.TryParse(value, out var longValue) ? longValue : defaultValue;
}
