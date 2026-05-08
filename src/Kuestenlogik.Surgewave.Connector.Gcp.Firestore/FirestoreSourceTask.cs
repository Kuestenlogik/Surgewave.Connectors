using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Firestore;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Firestore;

/// <summary>
/// Task that captures changes from Google Cloud Firestore.
/// Supports polling mode and real-time listener mode.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Listener stopped via Stop()")]
public sealed class FirestoreSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private FirestoreDb? _firestoreDb;
    private FirestoreChangeListener? _listener;
    private string _projectId = "";
    private string _collectionPath = "";
    private string _topicPattern = FirestoreConnectorConfig.DefaultTopicPattern;
    private string _watchMode = FirestoreConnectorConfig.DefaultWatchMode;
    private long _pollIntervalMs = FirestoreConnectorConfig.DefaultPollIntervalMs;
    private int _maxDocumentsPerPoll = FirestoreConnectorConfig.DefaultMaxDocumentsPerPoll;
    private bool _includeMetadata = true;
    private string? _queryFilter;
    private string? _orderByField;
    private string _orderDirection = FirestoreConnectorConfig.DefaultOrderDirection;
    private string? _timestampField;

    private readonly Dictionary<string, object> _sourcePartition = new();
    private readonly ConcurrentQueue<DocumentChangeInfo> _changeQueue = new();
    private DateTimeOffset _lastPollTimestamp = DateTimeOffset.MinValue;
    private string? _lastDocumentId;

    public override void Start(IDictionary<string, string> config)
    {
        _projectId = config[FirestoreConnectorConfig.ProjectIdConfig];
        _collectionPath = config[FirestoreConnectorConfig.CollectionPathConfig];
        _topicPattern = GetConfigValue(config, FirestoreConnectorConfig.TopicPatternConfig, FirestoreConnectorConfig.DefaultTopicPattern);
        _watchMode = GetConfigValue(config, FirestoreConnectorConfig.WatchModeConfig, FirestoreConnectorConfig.DefaultWatchMode);
        _pollIntervalMs = long.Parse(GetConfigValue(config, FirestoreConnectorConfig.PollIntervalMsConfig, FirestoreConnectorConfig.DefaultPollIntervalMs.ToString()));
        _maxDocumentsPerPoll = int.Parse(GetConfigValue(config, FirestoreConnectorConfig.MaxDocumentsPerPollConfig, FirestoreConnectorConfig.DefaultMaxDocumentsPerPoll.ToString()));
        _includeMetadata = bool.Parse(GetConfigValue(config, FirestoreConnectorConfig.IncludeMetadataConfig, "true"));
        _queryFilter = GetConfigValue(config, FirestoreConnectorConfig.QueryFilterConfig, "");
        _orderByField = GetConfigValue(config, FirestoreConnectorConfig.OrderByFieldConfig, "");
        _orderDirection = GetConfigValue(config, FirestoreConnectorConfig.OrderDirectionConfig, FirestoreConnectorConfig.DefaultOrderDirection);
        _timestampField = GetConfigValue(config, FirestoreConnectorConfig.TimestampFieldConfig, "");

        _sourcePartition["project_id"] = _projectId;
        _sourcePartition["collection"] = _collectionPath;

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

        RestoreOffset();

        if (_watchMode.Equals("listen", StringComparison.OrdinalIgnoreCase))
        {
            StartListener();
        }
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private void RestoreOffset()
    {
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return;

        if (storedOffset.TryGetValue(FirestoreConnectorConfig.OffsetUpdateTime, out var updateTime) && updateTime != null)
        {
            if (DateTimeOffset.TryParse(updateTime.ToString(), out var timestamp))
            {
                _lastPollTimestamp = timestamp;
            }
        }

        if (storedOffset.TryGetValue(FirestoreConnectorConfig.OffsetDocumentId, out var docId) && docId != null)
        {
            _lastDocumentId = docId.ToString();
        }
    }

    private void StartListener()
    {
        var query = BuildQuery();

        _listener = query.Listen(snapshot =>
        {
            foreach (var change in snapshot.Changes)
            {
                _changeQueue.Enqueue(new DocumentChangeInfo
                {
                    Document = change.Document,
                    ChangeType = change.ChangeType,
                    Timestamp = DateTimeOffset.UtcNow
                });
            }
        });
    }

    private Query BuildQuery()
    {
        Query query = _firestoreDb!.Collection(_collectionPath);

        // Apply filter if specified
        if (!string.IsNullOrEmpty(_queryFilter))
        {
            var parts = _queryFilter.Split(':');
            if (parts.Length >= 3)
            {
                var field = parts[0];
                var op = parts[1];
                var value = parts[2];

                query = op.ToLowerInvariant() switch
                {
                    "eq" or "==" => query.WhereEqualTo(field, ParseValue(value)),
                    "neq" or "!=" => query.WhereNotEqualTo(field, ParseValue(value)),
                    "gt" or ">" => query.WhereGreaterThan(field, ParseValue(value)),
                    "gte" or ">=" => query.WhereGreaterThanOrEqualTo(field, ParseValue(value)),
                    "lt" or "<" => query.WhereLessThan(field, ParseValue(value)),
                    "lte" or "<=" => query.WhereLessThanOrEqualTo(field, ParseValue(value)),
                    _ => query
                };
            }
        }

        // Apply ordering
        if (!string.IsNullOrEmpty(_orderByField))
        {
            query = _orderDirection.Equals("desc", StringComparison.OrdinalIgnoreCase)
                ? query.OrderByDescending(_orderByField)
                : query.OrderBy(_orderByField);
        }

        // Apply limit
        query = query.Limit(_maxDocumentsPerPoll);

        return query;
    }

    private static object ParseValue(string value)
    {
        if (long.TryParse(value, out var longVal))
            return longVal;
        if (double.TryParse(value, out var doubleVal))
            return doubleVal;
        if (bool.TryParse(value, out var boolVal))
            return boolVal;
        return value;
    }

    public override void Stop()
    {
        _listener?.StopAsync().GetAwaiter().GetResult();
        _listener = null;
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

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_firestoreDb == null)
            return [];

        var records = new List<SourceRecord>();

        if (_watchMode.Equals("listen", StringComparison.OrdinalIgnoreCase))
        {
            // Drain the change queue
            while (_changeQueue.TryDequeue(out var changeInfo) && records.Count < _maxDocumentsPerPoll)
            {
                var record = ConvertToSourceRecord(changeInfo.Document, changeInfo.ChangeType, changeInfo.Timestamp);
                records.Add(record);
            }

            if (records.Count == 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs), cancellationToken);
            }
        }
        else
        {
            // Poll mode
            var query = BuildQuery();

            // If we have a timestamp field and last poll timestamp, use incremental polling
            if (!string.IsNullOrEmpty(_timestampField) && _lastPollTimestamp != DateTimeOffset.MinValue)
            {
                query = query.WhereGreaterThan(_timestampField, Timestamp.FromDateTimeOffset(_lastPollTimestamp));
            }

            var snapshot = await query.GetSnapshotAsync(cancellationToken);

            foreach (var doc in snapshot.Documents)
            {
                var record = ConvertToSourceRecord(doc, DocumentChange.Type.Modified, DateTimeOffset.UtcNow);
                records.Add(record);

                // Update last document tracking
                _lastDocumentId = doc.Id;
                if (doc.UpdateTime.HasValue)
                {
                    _lastPollTimestamp = doc.UpdateTime.Value.ToDateTimeOffset();
                }
            }

            if (records.Count == 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs), cancellationToken);
            }
        }

        return records;
    }

    private SourceRecord ConvertToSourceRecord(DocumentSnapshot doc, DocumentChange.Type changeType, DateTimeOffset timestamp)
    {
        var data = doc.ToDictionary();
        var updateTime = doc.UpdateTime?.ToDateTimeOffset() ?? timestamp;
        var createTime = doc.CreateTime?.ToDateTimeOffset() ?? timestamp;

        // Build key
        var key = new Dictionary<string, object>
        {
            ["id"] = doc.Id,
            ["path"] = doc.Reference.Path
        };

        // Determine operation type
        var op = changeType switch
        {
            DocumentChange.Type.Added => "c",
            DocumentChange.Type.Modified => "u",
            DocumentChange.Type.Removed => "d",
            _ => "u"
        };

        // Build payload
        Dictionary<string, object?> payload;
        if (_includeMetadata)
        {
            payload = new Dictionary<string, object?>
            {
                ["op"] = op,
                ["source"] = new Dictionary<string, object>
                {
                    ["project_id"] = _projectId,
                    ["collection"] = _collectionPath,
                    ["document_id"] = doc.Id,
                    ["document_path"] = doc.Reference.Path,
                    ["update_time"] = updateTime.ToString("O"),
                    ["create_time"] = createTime.ToString("O")
                },
                ["ts_ms"] = updateTime.ToUnixTimeMilliseconds()
            };

            if (changeType == DocumentChange.Type.Removed)
            {
                payload["before"] = ConvertFirestoreData(data);
            }
            else
            {
                payload["after"] = ConvertFirestoreData(data);
            }
        }
        else
        {
            payload = new Dictionary<string, object?>
            {
                ["data"] = ConvertFirestoreData(data)
            };
        }

        // Build offset
        var offset = new Dictionary<string, object>
        {
            [FirestoreConnectorConfig.OffsetDocumentId] = doc.Id,
            [FirestoreConnectorConfig.OffsetUpdateTime] = updateTime.ToString("O"),
            [FirestoreConnectorConfig.OffsetCollectionPath] = _collectionPath
        };

        var headers = new Dictionary<string, byte[]>
        {
            [FirestoreConnectorConfig.HeaderProjectId] = Encoding.UTF8.GetBytes(_projectId),
            [FirestoreConnectorConfig.HeaderCollectionPath] = Encoding.UTF8.GetBytes(_collectionPath),
            [FirestoreConnectorConfig.HeaderDocumentId] = Encoding.UTF8.GetBytes(doc.Id),
            [FirestoreConnectorConfig.HeaderDocumentPath] = Encoding.UTF8.GetBytes(doc.Reference.Path),
            [FirestoreConnectorConfig.HeaderUpdateTime] = Encoding.UTF8.GetBytes(updateTime.ToString("O")),
            [FirestoreConnectorConfig.HeaderCreateTime] = Encoding.UTF8.GetBytes(createTime.ToString("O")),
            [FirestoreConnectorConfig.HeaderChangeType] = Encoding.UTF8.GetBytes(changeType.ToString())
        };

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = offset,
            Topic = GetTopicName(),
            Key = JsonSerializer.SerializeToUtf8Bytes(key),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = updateTime,
            Headers = headers
        };
    }

    private static Dictionary<string, object?> ConvertFirestoreData(IDictionary<string, object?> data)
    {
        var result = new Dictionary<string, object?>();

        foreach (var kvp in data)
        {
            result[kvp.Key] = ConvertFirestoreValue(kvp.Value);
        }

        return result;
    }

    private static object? ConvertFirestoreValue(object? value)
    {
        return value switch
        {
            null => null,
            Timestamp ts => ts.ToDateTimeOffset().ToString("O"),
            Google.Cloud.Firestore.GeoPoint gp => new { lat = gp.Latitude, lng = gp.Longitude },
            DocumentReference docRef => docRef.Path,
            byte[] bytes => Convert.ToBase64String(bytes),
            IDictionary<string, object?> dict => ConvertFirestoreData(dict),
            IList<object?> list => list.Select(ConvertFirestoreValue).ToList(),
            _ => value
        };
    }

    private string GetTopicName()
    {
        var collectionName = _collectionPath.Contains('/')
            ? _collectionPath.Split('/').Last()
            : _collectionPath;

        return _topicPattern.Replace("${collection}", collectionName);
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Position is tracked via offset storage automatically
        return Task.CompletedTask;
    }

    private sealed record DocumentChangeInfo
    {
        public required DocumentSnapshot Document { get; init; }
        public required DocumentChange.Type ChangeType { get; init; }
        public required DateTimeOffset Timestamp { get; init; }
    }
}
