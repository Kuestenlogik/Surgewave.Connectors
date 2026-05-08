using System.Text.Json;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Firestore;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Firestore;

/// <summary>
/// Task that writes records to Google Cloud Firestore.
/// Uses WriteBatch for atomic batch operations with retry logic.
/// </summary>
public sealed class FirestoreSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private FirestoreDb? _firestoreDb;
    private string _projectId = "";
    private string _collectionPath = "";
    private string _documentIdField = "id";
    private string _writeMode = FirestoreConnectorConfig.DefaultWriteMode;
    private int _batchSize = FirestoreConnectorConfig.DefaultBatchSize;
    private int _maxRetryCount = FirestoreConnectorConfig.DefaultMaxRetryCount;
    private long _retryDelayMs = FirestoreConnectorConfig.DefaultRetryDelayMs;

    public override void Start(IDictionary<string, string> config)
    {
        _projectId = config[FirestoreConnectorConfig.ProjectIdConfig];
        _collectionPath = config[FirestoreConnectorConfig.CollectionPathConfig];
        _documentIdField = GetConfigValue(config, FirestoreConnectorConfig.DocumentIdFieldConfig, "id");
        _writeMode = GetConfigValue(config, FirestoreConnectorConfig.WriteModeConfig, FirestoreConnectorConfig.DefaultWriteMode);
        _batchSize = int.Parse(GetConfigValue(config, FirestoreConnectorConfig.BatchSizeConfig, FirestoreConnectorConfig.DefaultBatchSize.ToString()));
        _maxRetryCount = int.Parse(GetConfigValue(config, FirestoreConnectorConfig.MaxRetryCountConfig, FirestoreConnectorConfig.DefaultMaxRetryCount.ToString()));
        _retryDelayMs = long.Parse(GetConfigValue(config, FirestoreConnectorConfig.RetryDelayMsConfig, FirestoreConnectorConfig.DefaultRetryDelayMs.ToString()));

        // Initialize Firestore client
        var builder = new FirestoreDbBuilder { ProjectId = _projectId };

        var emulatorHost = GetConfigValue(config, FirestoreConnectorConfig.EmulatorHostConfig, "");
        if (!string.IsNullOrEmpty(emulatorHost))
        {
            builder.EmulatorDetection = Google.Api.Gax.EmulatorDetection.EmulatorOnly;
            Environment.SetEnvironmentVariable("FIRESTORE_EMULATOR_HOST", emulatorHost);
        }

        var credentialsJson = GetConfigValue(config, FirestoreConnectorConfig.CredentialsJsonConfig, "");
        var credentialsFile = GetConfigValue(config, FirestoreConnectorConfig.CredentialsFileConfig, "");

#pragma warning disable CS0618 // GoogleCredential.FromJson/FromFile - CredentialFactory alternative requires internal IGoogleCredential
        if (!string.IsNullOrEmpty(credentialsJson))
        {
            builder.GoogleCredential = GoogleCredential.FromJson(credentialsJson);
        }
        else if (!string.IsNullOrEmpty(credentialsFile))
        {
            builder.GoogleCredential = GoogleCredential.FromFile(credentialsFile);
        }
#pragma warning restore CS0618

        _firestoreDb = builder.Build();
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    public override void Stop()
    {
        _firestoreDb = null;
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
        if (_firestoreDb == null || records.Count == 0)
            return;

        // Process in batches (Firestore limit is 500 operations per batch)
        var batches = records.Chunk(Math.Min(_batchSize, 500));

        foreach (var batch in batches)
        {
            await ProcessBatchWithRetryAsync(batch, cancellationToken);
        }
    }

    private async Task ProcessBatchWithRetryAsync(SinkRecord[] records, CancellationToken cancellationToken)
    {
        var retryCount = 0;

        while (retryCount <= _maxRetryCount)
        {
            try
            {
                var writeBatch = _firestoreDb!.StartBatch();

                foreach (var record in records)
                {
                    ProcessRecord(writeBatch, record);
                }

                await writeBatch.CommitAsync(cancellationToken);
                return;
            }
            catch (Exception ex) when (IsRetriableException(ex) && retryCount < _maxRetryCount)
            {
                retryCount++;
                var delay = _retryDelayMs * Math.Pow(2, retryCount - 1);
                await Task.Delay(TimeSpan.FromMilliseconds(delay), cancellationToken);
            }
        }
    }

    private static bool IsRetriableException(Exception ex)
    {
        // Retry on common transient errors
        return ex is Grpc.Core.RpcException rpcEx &&
            (rpcEx.StatusCode == Grpc.Core.StatusCode.Unavailable ||
             rpcEx.StatusCode == Grpc.Core.StatusCode.DeadlineExceeded ||
             rpcEx.StatusCode == Grpc.Core.StatusCode.ResourceExhausted);
    }

    private void ProcessRecord(WriteBatch batch, SinkRecord record)
    {
        if (record.Value == null || record.Value.Length == 0)
        {
            // Tombstone - delete document
            DeleteFromKey(batch, record);
            return;
        }

        try
        {
            var data = JsonSerializer.Deserialize<Dictionary<string, object>>(record.Value, s_jsonOptions);

            if (data == null)
                return;

            // Extract document ID
            var documentId = ExtractDocumentId(data, record);
            if (string.IsNullOrEmpty(documentId))
            {
                documentId = Guid.NewGuid().ToString();
            }

            // Convert JSON data to Firestore-compatible format
            var firestoreData = ConvertToFirestoreData(data);

            // Get document reference
            var docRef = _firestoreDb!.Collection(_collectionPath).Document(documentId);

            // Apply write operation based on mode
            switch (_writeMode.ToLowerInvariant())
            {
                case "set":
                    batch.Set(docRef, firestoreData);
                    break;

                case "create":
                    batch.Create(docRef, firestoreData);
                    break;

                case "update":
                    batch.Update(docRef, firestoreData);
                    break;

                case "merge":
                    batch.Set(docRef, firestoreData, SetOptions.MergeAll);
                    break;

                default:
                    batch.Set(docRef, firestoreData);
                    break;
            }
        }
        catch (JsonException)
        {
            // Invalid JSON, skip this record
        }
    }

    private string? ExtractDocumentId(Dictionary<string, object> data, SinkRecord record)
    {
        // Try to get ID from document data
        if (data.TryGetValue(_documentIdField, out var idValue) && idValue != null)
        {
            return idValue.ToString();
        }

        // Fallback to "id" field
        if (_documentIdField != "id" && data.TryGetValue("id", out var fallbackId) && fallbackId != null)
        {
            return fallbackId.ToString();
        }

        // Try to extract from record key
        if (record.Key != null && record.Key.Length > 0)
        {
            try
            {
                var keyDoc = JsonSerializer.Deserialize<Dictionary<string, object>>(record.Key);
                if (keyDoc?.TryGetValue("id", out var keyId) == true && keyId != null)
                {
                    return keyId.ToString();
                }

                if (keyDoc?.TryGetValue(_documentIdField, out var keyFieldId) == true && keyFieldId != null)
                {
                    return keyFieldId.ToString();
                }
            }
            catch (JsonException)
            {
                // Use key as-is if it's not JSON
                return Convert.ToBase64String(record.Key);
            }
        }

        return null;
    }

    private void DeleteFromKey(WriteBatch batch, SinkRecord record)
    {
        if (record.Key == null || record.Key.Length == 0)
            return;

        try
        {
            var keyDoc = JsonSerializer.Deserialize<Dictionary<string, object>>(record.Key);

            string? documentId = null;
            if (keyDoc?.TryGetValue("id", out var idValue) == true && idValue != null)
            {
                documentId = idValue.ToString();
            }
            else if (keyDoc?.TryGetValue(_documentIdField, out var fieldId) == true && fieldId != null)
            {
                documentId = fieldId.ToString();
            }

            if (!string.IsNullOrEmpty(documentId))
            {
                var docRef = _firestoreDb!.Collection(_collectionPath).Document(documentId);
                batch.Delete(docRef);
            }
        }
        catch (JsonException)
        {
            // Invalid key JSON, skip
        }
    }

    private static Dictionary<string, object?> ConvertToFirestoreData(Dictionary<string, object> data)
    {
        var result = new Dictionary<string, object?>();

        foreach (var kvp in data)
        {
            result[kvp.Key] = ConvertValue(kvp.Value);
        }

        return result;
    }

    private static object? ConvertValue(object? value)
    {
        return value switch
        {
            null => null,
            JsonElement jsonElement => ConvertJsonElement(jsonElement),
            IDictionary<string, object> dict => ConvertToFirestoreData(new Dictionary<string, object>(dict)),
            IList<object> list => list.Select(ConvertValue).ToList(),
            _ => value
        };
    }

    private static object? ConvertJsonElement(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number when element.TryGetInt64(out var l) => l,
            JsonValueKind.Number when element.TryGetDouble(out var d) => d,
            JsonValueKind.String => TryParseTimestamp(element.GetString()!),
            JsonValueKind.Array => element.EnumerateArray().Select(ConvertJsonElement).ToList(),
            JsonValueKind.Object => ConvertJsonObject(element),
            _ => element.GetRawText()
        };
    }

    private static object TryParseTimestamp(string value)
    {
        // Try to parse as timestamp
        if (DateTimeOffset.TryParse(value, out var dto))
        {
            return Timestamp.FromDateTimeOffset(dto);
        }
        return value;
    }

    private static Dictionary<string, object?> ConvertJsonObject(JsonElement element)
    {
        var result = new Dictionary<string, object?>();

        foreach (var prop in element.EnumerateObject())
        {
            result[prop.Name] = ConvertJsonElement(prop.Value);
        }

        // Check for GeoPoint structure
        if (result.TryGetValue("lat", out var latValue) && result.TryGetValue("lng", out var lngValue) &&
            latValue is double lat && lngValue is double lng)
        {
            return new Dictionary<string, object?>
            {
                ["_geopoint"] = new GeoPoint(lat, lng)
            };
        }

        return result;
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // Firestore writes are committed in PutAsync
        return Task.CompletedTask;
    }
}
