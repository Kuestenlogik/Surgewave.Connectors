namespace Kuestenlogik.Surgewave.Connector.Gcp.Storage;

using System.Text;
using System.Text.Json;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that reads objects from Google Cloud Storage.
/// </summary>
public sealed class GcsSourceTask : SourceTask
{
    private StorageClient? _storageClient;
    private string _bucketName = "";
    private string _topic = "";
    private string _prefix = "";
    private string _format = GcsConnectorConfig.DefaultFormat;
    private long _pollIntervalMs = GcsConnectorConfig.DefaultPollIntervalMs;
    private bool _deleteAfterRead;
    private DateTimeOffset _lastPollTime = DateTimeOffset.MinValue;

    private readonly Dictionary<string, object> _sourcePartition = new();
    private string _lastProcessedObject = "";
    private DateTimeOffset _lastModifiedTime = DateTimeOffset.MinValue;
    private readonly HashSet<string> _processedObjects = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _bucketName = config[GcsConnectorConfig.BucketNameConfig];
        _topic = config[GcsConnectorConfig.TopicConfig];
        _prefix = GetConfigValue(config, GcsConnectorConfig.PrefixConfig, "");
        _format = GetConfigValue(config, GcsConnectorConfig.FormatConfig, GcsConnectorConfig.DefaultFormat);
        _pollIntervalMs = GetConfigLong(config, GcsConnectorConfig.PollIntervalMsConfig, GcsConnectorConfig.DefaultPollIntervalMs);
        _deleteAfterRead = GetConfigBool(config, GcsConnectorConfig.DeleteAfterReadConfig, GcsConnectorConfig.DefaultDeleteAfterRead);

        var credentialsJson = GetConfigValue(config, GcsConnectorConfig.CredentialsJsonConfig, "");
        var credentialsFile = GetConfigValue(config, GcsConnectorConfig.CredentialsFileConfig, "");

        _sourcePartition["bucket"] = _bucketName;
        _sourcePartition["prefix"] = _prefix;

        // Restore offset
        var storedOffset = Context.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null)
        {
            if (storedOffset.TryGetValue("last_object", out var lastObject))
            {
                _lastProcessedObject = lastObject?.ToString() ?? "";
            }
            if (storedOffset.TryGetValue("last_modified", out var lastMod))
            {
                _lastModifiedTime = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(lastMod));
            }
        }

        // Create storage client
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

        if (_storageClient == null)
        {
            return [];
        }

        var records = new List<SourceRecord>();

        // List objects in bucket
        var objects = _storageClient.ListObjects(_bucketName, _prefix);

        foreach (var obj in objects)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Skip already processed objects
            if (_processedObjects.Contains(obj.Name))
                continue;

            // Skip objects older than last modified time (for resumability)
            var objModified = obj.UpdatedDateTimeOffset ?? obj.TimeCreatedDateTimeOffset ?? DateTimeOffset.MinValue;
            if (objModified <= _lastModifiedTime && !string.IsNullOrEmpty(_lastProcessedObject))
                continue;

            // Download and process object
            using var memoryStream = new MemoryStream();
            await _storageClient.DownloadObjectAsync(_bucketName, obj.Name, memoryStream, cancellationToken: cancellationToken);
            memoryStream.Position = 0;

            using var reader = new StreamReader(memoryStream);
            var content = await reader.ReadToEndAsync(cancellationToken);

            var parsedRecords = ParseContent(obj.Name, content);
            records.AddRange(parsedRecords);

            // Track processed object
            _lastProcessedObject = obj.Name;
            _lastModifiedTime = objModified;
            _processedObjects.Add(obj.Name);

            // Delete if configured
            if (_deleteAfterRead)
            {
                await _storageClient.DeleteObjectAsync(_bucketName, obj.Name, cancellationToken: cancellationToken);
            }
        }

        return records;
    }

    private IEnumerable<SourceRecord> ParseContent(string objectName, string content)
    {
        var sourceOffset = new Dictionary<string, object>
        {
            ["last_object"] = _lastProcessedObject,
            ["last_modified"] = _lastModifiedTime.ToUnixTimeMilliseconds()
        };

        switch (_format.ToLowerInvariant())
        {
            case GcsConnectorConfig.FormatJsonLines:
                foreach (var line in content.Split('\n', StringSplitOptions.RemoveEmptyEntries))
                {
                    yield return new SourceRecord
                    {
                        SourcePartition = _sourcePartition,
                        SourceOffset = sourceOffset,
                        Topic = _topic,
                        Key = Encoding.UTF8.GetBytes(objectName),
                        Value = Encoding.UTF8.GetBytes(line.Trim())
                    };
                }
                break;

            case GcsConnectorConfig.FormatCsv:
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
                        Key = Encoding.UTF8.GetBytes(objectName),
                        Value = JsonSerializer.SerializeToUtf8Bytes(row)
                    };
                }
                break;

            case GcsConnectorConfig.FormatRaw:
                yield return new SourceRecord
                {
                    SourcePartition = _sourcePartition,
                    SourceOffset = sourceOffset,
                    Topic = _topic,
                    Key = Encoding.UTF8.GetBytes(objectName),
                    Value = Encoding.UTF8.GetBytes(content)
                };
                break;

            default: // json
                foreach (var record in ParseJsonContent(objectName, content, sourceOffset))
                {
                    yield return record;
                }
                break;
        }
    }

    private List<SourceRecord> ParseJsonContent(string objectName, string content, Dictionary<string, object> sourceOffset)
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
                        Key = Encoding.UTF8.GetBytes(objectName),
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
                    Key = Encoding.UTF8.GetBytes(objectName),
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
                Key = Encoding.UTF8.GetBytes(objectName),
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
