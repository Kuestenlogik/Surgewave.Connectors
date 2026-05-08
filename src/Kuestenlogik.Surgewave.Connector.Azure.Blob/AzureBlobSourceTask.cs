namespace Kuestenlogik.Surgewave.Connector.Azure.Blob;

using System.Text;
using System.Text.Json;
using global::Azure.Storage.Blobs;
using global::Azure.Storage.Blobs.Models;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that reads blobs from Azure Blob Storage.
/// </summary>
public sealed class AzureBlobSourceTask : SourceTask
{
    private BlobContainerClient? _containerClient;
    private string _topic = "";
    private string _prefix = "";
    private string _format = AzureBlobConnectorConfig.DefaultFormat;
    private long _pollIntervalMs = AzureBlobConnectorConfig.DefaultPollIntervalMs;
    private bool _deleteAfterRead;
    private DateTimeOffset _lastPollTime = DateTimeOffset.MinValue;

    private readonly Dictionary<string, object> _sourcePartition = new();
    private string _lastProcessedBlob = "";
    private DateTimeOffset _lastModifiedTime = DateTimeOffset.MinValue;
    private readonly HashSet<string> _processedBlobs = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var connectionString = GetConfigValue(config, AzureBlobConnectorConfig.ConnectionStringConfig, "");
        var accountName = GetConfigValue(config, AzureBlobConnectorConfig.AccountNameConfig, "");
        var accountKey = GetConfigValue(config, AzureBlobConnectorConfig.AccountKeyConfig, "");
        var containerName = config[AzureBlobConnectorConfig.ContainerNameConfig];
        var endpoint = GetConfigValue(config, AzureBlobConnectorConfig.EndpointConfig, "");

        _topic = config[AzureBlobConnectorConfig.TopicConfig];
        _prefix = GetConfigValue(config, AzureBlobConnectorConfig.PrefixConfig, "");
        _format = GetConfigValue(config, AzureBlobConnectorConfig.FormatConfig, AzureBlobConnectorConfig.DefaultFormat);
        _pollIntervalMs = GetConfigLong(config, AzureBlobConnectorConfig.PollIntervalMsConfig, AzureBlobConnectorConfig.DefaultPollIntervalMs);
        _deleteAfterRead = GetConfigBool(config, AzureBlobConnectorConfig.DeleteAfterReadConfig, AzureBlobConnectorConfig.DefaultDeleteAfterRead);

        _sourcePartition["container"] = containerName;
        _sourcePartition["prefix"] = _prefix;

        // Restore offset
        var storedOffset = Context.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null)
        {
            if (storedOffset.TryGetValue("last_blob", out var lastBlob))
            {
                _lastProcessedBlob = lastBlob?.ToString() ?? "";
            }
            if (storedOffset.TryGetValue("last_modified", out var lastMod))
            {
                _lastModifiedTime = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(lastMod));
            }
        }

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
        _containerClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _containerClient = null;
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

        if (_containerClient == null)
        {
            return [];
        }

        var records = new List<SourceRecord>();

        // List blobs in container
        await foreach (var blobItem in _containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, _prefix, cancellationToken))
        {
            // Skip already processed blobs
            if (_processedBlobs.Contains(blobItem.Name))
                continue;

            // Skip blobs older than last modified time (for resumability)
            var blobModified = blobItem.Properties.LastModified ?? DateTimeOffset.MinValue;
            if (blobModified <= _lastModifiedTime && !string.IsNullOrEmpty(_lastProcessedBlob))
                continue;

            // Download and process blob
            var blobClient = _containerClient.GetBlobClient(blobItem.Name);
            var response = await blobClient.DownloadContentAsync(cancellationToken);
            var content = response.Value.Content.ToString();

            var parsedRecords = ParseContent(blobItem.Name, content);
            records.AddRange(parsedRecords);

            // Track processed blob
            _lastProcessedBlob = blobItem.Name;
            _lastModifiedTime = blobModified;
            _processedBlobs.Add(blobItem.Name);

            // Delete if configured
            if (_deleteAfterRead)
            {
                await blobClient.DeleteIfExistsAsync(cancellationToken: cancellationToken);
            }
        }

        return records;
    }

    private IEnumerable<SourceRecord> ParseContent(string blobName, string content)
    {
        var sourceOffset = new Dictionary<string, object>
        {
            ["last_blob"] = _lastProcessedBlob,
            ["last_modified"] = _lastModifiedTime.ToUnixTimeMilliseconds()
        };

        switch (_format.ToLowerInvariant())
        {
            case AzureBlobConnectorConfig.FormatJsonLines:
                foreach (var line in content.Split('\n', StringSplitOptions.RemoveEmptyEntries))
                {
                    yield return new SourceRecord
                    {
                        SourcePartition = _sourcePartition,
                        SourceOffset = sourceOffset,
                        Topic = _topic,
                        Key = Encoding.UTF8.GetBytes(blobName),
                        Value = Encoding.UTF8.GetBytes(line.Trim())
                    };
                }
                break;

            case AzureBlobConnectorConfig.FormatCsv:
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
                        Key = Encoding.UTF8.GetBytes(blobName),
                        Value = JsonSerializer.SerializeToUtf8Bytes(row)
                    };
                }
                break;

            case AzureBlobConnectorConfig.FormatRaw:
                yield return new SourceRecord
                {
                    SourcePartition = _sourcePartition,
                    SourceOffset = sourceOffset,
                    Topic = _topic,
                    Key = Encoding.UTF8.GetBytes(blobName),
                    Value = Encoding.UTF8.GetBytes(content)
                };
                break;

            default: // json
                foreach (var record in ParseJsonContent(blobName, content, sourceOffset))
                {
                    yield return record;
                }
                break;
        }
    }

    private List<SourceRecord> ParseJsonContent(string blobName, string content, Dictionary<string, object> sourceOffset)
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
                        Key = Encoding.UTF8.GetBytes(blobName),
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
                    Key = Encoding.UTF8.GetBytes(blobName),
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
                Key = Encoding.UTF8.GetBytes(blobName),
                Value = Encoding.UTF8.GetBytes(content)
            });
        }
        return records;
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static long GetConfigLong(IDictionary<string, string> config, string key, long defaultValue)
        => config.TryGetValue(key, out var value) && long.TryParse(value, out var longValue) ? longValue : defaultValue;

    private static bool GetConfigBool(IDictionary<string, string> config, string key, bool defaultValue)
        => config.TryGetValue(key, out var value) && bool.TryParse(value, out var boolValue) ? boolValue : defaultValue;
}
