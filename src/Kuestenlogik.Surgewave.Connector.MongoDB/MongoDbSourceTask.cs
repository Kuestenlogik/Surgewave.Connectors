namespace Kuestenlogik.Surgewave.Connector.MongoDB;

using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using global::MongoDB.Bson;
using global::MongoDB.Driver;

/// <summary>
/// A source task that captures changes from MongoDB using Change Streams or polling.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "MongoClient is designed as long-lived and doesn't require explicit disposal")]
public sealed class MongoDbSourceTask : SourceTask
{
    private MongoClient? _client;
    private IMongoDatabase? _database;
    private IMongoCollection<BsonDocument>? _collection;
    private IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>? _changeStreamCursor;

    private string _databaseName = "";
    private string _collectionName = "";
    private string _sourceMode = MongoDbConnectorConfig.SourceModeChangeStream;
    private string _topicPrefix = "";
    private string _topicPattern = MongoDbConnectorConfig.DefaultTopicPattern;
    private string _fullDocumentMode = MongoDbConnectorConfig.FullDocumentUpdateLookup;
    private string _pollField = MongoDbConnectorConfig.DefaultPollField;
    private int _pollIntervalMs = (int)MongoDbConnectorConfig.DefaultPollIntervalMs;
    private int _batchMaxRecords = MongoDbConnectorConfig.DefaultBatchMaxRecords;
    private string? _pipelineJson;

    private BsonDocument? _resumeToken;
    private BsonValue? _lastPolledValue;
    private IDictionary<string, object>? _sourcePartition;
    private DateTime _lastPollTime = DateTime.MinValue;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var connectionString = config[MongoDbConnectorConfig.ConnectionString];
        _databaseName = config[MongoDbConnectorConfig.Database];
        _collectionName = config[MongoDbConnectorConfig.Collection];

        if (config.TryGetValue(MongoDbConnectorConfig.SourceMode, out var mode))
            _sourceMode = mode;

        if (config.TryGetValue(MongoDbConnectorConfig.TopicPrefix, out var prefix))
            _topicPrefix = prefix;

        if (config.TryGetValue(MongoDbConnectorConfig.TopicPattern, out var pattern))
            _topicPattern = pattern;

        if (config.TryGetValue(MongoDbConnectorConfig.ChangeStreamFullDocument, out var fullDoc))
            _fullDocumentMode = fullDoc;

        if (config.TryGetValue(MongoDbConnectorConfig.PollField, out var pollField))
            _pollField = pollField;

        if (config.TryGetValue(MongoDbConnectorConfig.PollIntervalMs, out var pollInterval))
            _pollIntervalMs = int.Parse(pollInterval);

        if (config.TryGetValue(MongoDbConnectorConfig.BatchMaxRecords, out var maxRecords))
            _batchMaxRecords = int.Parse(maxRecords);

        if (config.TryGetValue(MongoDbConnectorConfig.Pipeline, out var pipeline) && !string.IsNullOrEmpty(pipeline))
            _pipelineJson = pipeline;

        _client = new MongoClient(connectionString);
        _database = _client.GetDatabase(_databaseName);

        if (_collectionName != "*")
        {
            _collection = _database.GetCollection<BsonDocument>(_collectionName);
        }

        _sourcePartition = new Dictionary<string, object>
        {
            ["database"] = _databaseName,
            ["collection"] = _collectionName
        };

        // Restore offset from context if available
        if (Context?.OffsetStorageReader != null)
        {
            var storedOffset = Context.OffsetStorageReader.Offset(_sourcePartition);
            if (storedOffset != null)
            {
                if (_sourceMode == MongoDbConnectorConfig.SourceModeChangeStream)
                {
                    if (storedOffset.TryGetValue(MongoDbConnectorConfig.OffsetResumeToken, out var token) && token is string tokenString)
                    {
                        _resumeToken = BsonDocument.Parse(tokenString);
                    }
                }
                else
                {
                    if (storedOffset.TryGetValue(MongoDbConnectorConfig.OffsetLastPolledValue, out var lastValue) && lastValue is string valueString)
                    {
                        _lastPolledValue = BsonValue.Create(valueString);
                    }
                }
            }
        }
    }

    public override void Stop()
    {
        _changeStreamCursor?.Dispose();
        _changeStreamCursor = null;
        _client = null;
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_sourceMode == MongoDbConnectorConfig.SourceModeChangeStream)
        {
            return await PollChangeStreamAsync(cancellationToken);
        }
        else
        {
            return await PollCollectionAsync(cancellationToken);
        }
    }

    private async Task<IReadOnlyList<SourceRecord>> PollChangeStreamAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        if (_changeStreamCursor == null)
        {
            await StartChangeStreamAsync(cancellationToken);
        }

        try
        {
            while (records.Count < _batchMaxRecords)
            {
                // Use TryGetNextAsync instead of MoveNextAsync for non-blocking operation
                if (!await _changeStreamCursor!.MoveNextAsync(cancellationToken))
                {
                    break;
                }

                foreach (var change in _changeStreamCursor.Current)
                {
                    var record = CreateChangeStreamRecord(change);
                    records.Add(record);
                    _resumeToken = change.ResumeToken;
                }

                if (!_changeStreamCursor.Current.Any())
                {
                    break;
                }
            }
        }
        catch (MongoException ex) when (ex.Message.Contains("cursor not found") || ex.Message.Contains("CursorNotFound"))
        {
            // Cursor expired, restart the change stream
            _changeStreamCursor?.Dispose();
            _changeStreamCursor = null;
            await StartChangeStreamAsync(cancellationToken);
        }

        return records;
    }

    private async Task StartChangeStreamAsync(CancellationToken cancellationToken)
    {
        var fullDocumentOption = _fullDocumentMode switch
        {
            MongoDbConnectorConfig.FullDocumentDefault => ChangeStreamFullDocumentOption.Default,
            MongoDbConnectorConfig.FullDocumentUpdateLookup => ChangeStreamFullDocumentOption.UpdateLookup,
            MongoDbConnectorConfig.FullDocumentWhenAvailable => ChangeStreamFullDocumentOption.WhenAvailable,
            _ => ChangeStreamFullDocumentOption.UpdateLookup
        };

        var options = new ChangeStreamOptions
        {
            FullDocument = fullDocumentOption,
            ResumeAfter = _resumeToken
        };

        if (_collectionName == "*")
        {
            // Watch all collections in the database
            if (!string.IsNullOrEmpty(_pipelineJson))
            {
                var pipeline = PipelineDefinition<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>>
                    .Create(BsonDocument.Parse(_pipelineJson));
                _changeStreamCursor = await _database!.WatchAsync(pipeline, options, cancellationToken);
            }
            else
            {
                _changeStreamCursor = await _database!.WatchAsync(options, cancellationToken);
            }
        }
        else
        {
            // Watch specific collection
            if (!string.IsNullOrEmpty(_pipelineJson))
            {
                var pipeline = PipelineDefinition<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>>
                    .Create(BsonDocument.Parse(_pipelineJson));
                _changeStreamCursor = await _collection!.WatchAsync(pipeline, options, cancellationToken);
            }
            else
            {
                _changeStreamCursor = await _collection!.WatchAsync(options, cancellationToken);
            }
        }
    }

    private SourceRecord CreateChangeStreamRecord(ChangeStreamDocument<BsonDocument> change)
    {
        var collectionName = change.CollectionNamespace?.CollectionName ?? _collectionName;

        var payload = new Dictionary<string, object?>
        {
            ["op"] = MapOperationType(change.OperationType),
            ["ns"] = new { db = _databaseName, coll = collectionName },
            ["documentKey"] = change.DocumentKey?.ToJson(),
            ["fullDocument"] = change.FullDocument?.ToJson(),
            ["updateDescription"] = change.UpdateDescription != null ? new
            {
                updatedFields = change.UpdateDescription.UpdatedFields?.ToJson(),
                removedFields = change.UpdateDescription.RemovedFields?.ToList()
            } : null,
            ["ts_ms"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        var topic = GetTopicName(collectionName);
        var key = change.DocumentKey != null ? Encoding.UTF8.GetBytes(change.DocumentKey.ToJson()) : null;
        var value = JsonSerializer.SerializeToUtf8Bytes(payload);

        return new SourceRecord
        {
            SourcePartition = _sourcePartition!,
            SourceOffset = new Dictionary<string, object>
            {
                [MongoDbConnectorConfig.OffsetResumeToken] = change.ResumeToken.ToJson()
            },
            Topic = topic,
            Key = key,
            Value = value,
            Timestamp = DateTimeOffset.UtcNow
        };
    }

    private async Task<IReadOnlyList<SourceRecord>> PollCollectionAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        // Respect poll interval
        var elapsed = (DateTime.UtcNow - _lastPollTime).TotalMilliseconds;
        if (elapsed < _pollIntervalMs)
        {
            await Task.Delay((int)(_pollIntervalMs - elapsed), cancellationToken);
        }

        _lastPollTime = DateTime.UtcNow;

        var filter = _lastPolledValue != null
            ? Builders<BsonDocument>.Filter.Gt(_pollField, _lastPolledValue)
            : Builders<BsonDocument>.Filter.Empty;

        var sort = Builders<BsonDocument>.Sort.Ascending(_pollField);

        using var cursor = await _collection!.Find(filter)
            .Sort(sort)
            .Limit(_batchMaxRecords)
            .ToCursorAsync(cancellationToken);

        while (await cursor.MoveNextAsync(cancellationToken))
        {
            foreach (var doc in cursor.Current)
            {
                records.Add(CreatePollRecord(doc));
                _lastPolledValue = doc[_pollField];
            }
        }

        return records;
    }

    private SourceRecord CreatePollRecord(BsonDocument doc)
    {
        var payload = new Dictionary<string, object?>
        {
            ["op"] = MongoDbConnectorConfig.OpCreate,
            ["ns"] = new { db = _databaseName, coll = _collectionName },
            ["fullDocument"] = doc.ToJson(),
            ["ts_ms"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        var topic = GetTopicName(_collectionName);
        var key = doc.Contains("_id") ? Encoding.UTF8.GetBytes(doc["_id"].ToJson()) : null;
        var value = JsonSerializer.SerializeToUtf8Bytes(payload);

        var offset = new Dictionary<string, object>
        {
            [MongoDbConnectorConfig.OffsetLastPolledValue] = _lastPolledValue?.ToJson() ?? ""
        };

        return new SourceRecord
        {
            SourcePartition = _sourcePartition!,
            SourceOffset = offset,
            Topic = topic,
            Key = key,
            Value = value,
            Timestamp = DateTimeOffset.UtcNow
        };
    }

    private string GetTopicName(string collectionName)
    {
        var topic = _topicPattern
            .Replace("${database}", _databaseName)
            .Replace("${collection}", collectionName);

        return string.IsNullOrEmpty(_topicPrefix) ? topic : $"{_topicPrefix}.{topic}";
    }

    private static string MapOperationType(ChangeStreamOperationType operationType)
    {
        return operationType switch
        {
            ChangeStreamOperationType.Insert => MongoDbConnectorConfig.OpCreate,
            ChangeStreamOperationType.Update => MongoDbConnectorConfig.OpUpdate,
            ChangeStreamOperationType.Replace => MongoDbConnectorConfig.OpReplace,
            ChangeStreamOperationType.Delete => MongoDbConnectorConfig.OpDelete,
            _ => "u" // Default to update for unknown types
        };
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _changeStreamCursor?.Dispose();
        }
        base.Dispose(disposing);
    }
}
