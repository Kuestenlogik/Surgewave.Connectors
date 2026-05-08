namespace Kuestenlogik.Surgewave.Connector.Azure.Blob;

using System.Text;
using System.Text.Json;
using global::Azure.Storage.Blobs;
using global::Azure.Storage.Blobs.Models;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that writes records to Azure Blob Storage.
/// </summary>
public sealed class AzureBlobSinkTask : SinkTask
{
    private BlobContainerClient? _containerClient;
    private string _prefix = "";
    private string _format = AzureBlobConnectorConfig.DefaultFormat;
    private string _partitioner = AzureBlobConnectorConfig.DefaultPartitioner;
    private int _flushSize = AzureBlobConnectorConfig.DefaultFlushSize;
    private long _rotateIntervalMs = AzureBlobConnectorConfig.DefaultRotateIntervalMs;
    private DateTimeOffset _lastRotateTime = DateTimeOffset.UtcNow;

    private readonly Dictionary<string, List<SinkRecord>> _buffers = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var connectionString = GetConfigValue(config, AzureBlobConnectorConfig.ConnectionStringConfig, "");
        var accountName = GetConfigValue(config, AzureBlobConnectorConfig.AccountNameConfig, "");
        var accountKey = GetConfigValue(config, AzureBlobConnectorConfig.AccountKeyConfig, "");
        var containerName = config[AzureBlobConnectorConfig.ContainerNameConfig];
        var endpoint = GetConfigValue(config, AzureBlobConnectorConfig.EndpointConfig, "");

        _prefix = GetConfigValue(config, AzureBlobConnectorConfig.PrefixConfig, "");
        _format = GetConfigValue(config, AzureBlobConnectorConfig.FormatConfig, AzureBlobConnectorConfig.DefaultFormat);
        _partitioner = GetConfigValue(config, AzureBlobConnectorConfig.PartitionerConfig, AzureBlobConnectorConfig.DefaultPartitioner);
        _flushSize = GetConfigInt(config, AzureBlobConnectorConfig.FlushSizeConfig, AzureBlobConnectorConfig.DefaultFlushSize);
        _rotateIntervalMs = GetConfigLong(config, AzureBlobConnectorConfig.RotateIntervalMsConfig, AzureBlobConnectorConfig.DefaultRotateIntervalMs);

        // Create blob container client
        _containerClient = CreateContainerClient(connectionString, accountName, accountKey, containerName, endpoint);
    }

    private static BlobContainerClient CreateContainerClient(
        string connectionString, string accountName, string accountKey, string containerName, string endpoint)
    {
        if (!string.IsNullOrEmpty(connectionString))
        {
            return new BlobContainerClient(connectionString, containerName);
        }

        // Build connection string from account name/key
        var connStr = string.IsNullOrEmpty(endpoint)
            ? $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net"
            : $"DefaultEndpointsProtocol=http;AccountName={accountName};AccountKey={accountKey};BlobEndpoint={endpoint}";

        return new BlobContainerClient(connStr, containerName);
    }

    public override void Stop()
    {
        FlushAllBuffersAsync().GetAwaiter().GetResult();
        _containerClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            try
            {
                FlushAllBuffersAsync().GetAwaiter().GetResult();
            }
            catch
            {
                // Ignore disposal errors
            }
            _containerClient = null;
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
        if (buffer.Count == 0 || _containerClient == null) return;

        var blobName = GenerateBlobName(partitionKey, buffer[0]);
        var content = SerializeRecords(buffer);

        var blobClient = _containerClient.GetBlobClient(blobName);

        var options = new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders
            {
                ContentType = GetContentType()
            }
        };

        using var stream = new MemoryStream(content);
        await blobClient.UploadAsync(stream, options, cancellationToken);

        buffer.Clear();
    }

    private byte[] SerializeRecords(List<SinkRecord> records)
    {
        return _format.ToLowerInvariant() switch
        {
            AzureBlobConnectorConfig.FormatJsonLines => SerializeJsonLines(records),
            _ => SerializeJson(records)
        };
    }

    private string GetContentType()
    {
        return _format.ToLowerInvariant() switch
        {
            AzureBlobConnectorConfig.FormatJsonLines => "application/x-ndjson",
            _ => "application/json"
        };
    }

    private string GetPartitionKey(SinkRecord record)
    {
        return _partitioner switch
        {
            AzureBlobConnectorConfig.PartitionerTime => $"{record.Topic}/{record.Timestamp:yyyy/MM/dd/HH}",
            AzureBlobConnectorConfig.PartitionerField => $"{record.Topic}/{record.Partition}",
            _ => $"{record.Topic}/{record.Partition}"
        };
    }

    private string GenerateBlobName(string partitionKey, SinkRecord sampleRecord)
    {
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var extension = _format.ToLowerInvariant() switch
        {
            AzureBlobConnectorConfig.FormatJsonLines => "jsonl",
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

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static int GetConfigInt(IDictionary<string, string> config, string key, int defaultValue)
        => config.TryGetValue(key, out var value) && int.TryParse(value, out var intValue) ? intValue : defaultValue;

    private static long GetConfigLong(IDictionary<string, string> config, string key, long defaultValue)
        => config.TryGetValue(key, out var value) && long.TryParse(value, out var longValue) ? longValue : defaultValue;
}
