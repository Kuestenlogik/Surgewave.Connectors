namespace Kuestenlogik.Surgewave.Connector.MongoDB;

using System.Diagnostics.CodeAnalysis;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using global::MongoDB.Bson;
using global::MongoDB.Driver;

/// <summary>
/// Task that writes records to MongoDB using batch operations.
/// Supports insert, upsert, and replace write modes.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "MongoClient is designed as long-lived and doesn't require explicit disposal")]
public sealed class MongoDbSinkTask : SinkTask
{
    private MongoClient? _client;
    private IMongoCollection<BsonDocument>? _collection;

    private string _writeMode = MongoDbConnectorConfig.WriteModeInsert;
    private string _docIdStrategy = MongoDbConnectorConfig.DocumentIdStrategyAuto;
    private string _docIdField = MongoDbConnectorConfig.DefaultDocumentIdField;
    private int _batchSize = MongoDbConnectorConfig.DefaultBatchSize;
    private int _retryMax = MongoDbConnectorConfig.DefaultRetryMax;
    private long _retryBackoffMs = MongoDbConnectorConfig.DefaultRetryBackoffMs;
    private readonly List<SinkRecord> _buffer = [];

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var connectionString = config[MongoDbConnectorConfig.ConnectionString];
        var databaseName = config[MongoDbConnectorConfig.Database];
        var collectionName = config[MongoDbConnectorConfig.Collection];

        _writeMode = GetConfigValue(config, MongoDbConnectorConfig.WriteMode, MongoDbConnectorConfig.WriteModeInsert);
        _docIdStrategy = GetConfigValue(config, MongoDbConnectorConfig.DocumentIdStrategy, MongoDbConnectorConfig.DocumentIdStrategyAuto);
        _docIdField = GetConfigValue(config, MongoDbConnectorConfig.DocumentIdField, MongoDbConnectorConfig.DefaultDocumentIdField);
        _batchSize = int.Parse(GetConfigValue(config, MongoDbConnectorConfig.BatchSize, MongoDbConnectorConfig.DefaultBatchSize.ToString()));
        _retryMax = int.Parse(GetConfigValue(config, MongoDbConnectorConfig.RetryMax, MongoDbConnectorConfig.DefaultRetryMax.ToString()));
        _retryBackoffMs = long.Parse(GetConfigValue(config, MongoDbConnectorConfig.RetryBackoffMs, MongoDbConnectorConfig.DefaultRetryBackoffMs.ToString()));

        var writeConcern = GetConfigValue(config, MongoDbConnectorConfig.WriteConcern, MongoDbConnectorConfig.WriteConcernMajority);
        var mongoWriteConcern = writeConcern switch
        {
            MongoDbConnectorConfig.WriteConcernW1 => WriteConcern.W1,
            MongoDbConnectorConfig.WriteConcernMajority => WriteConcern.WMajority,
            MongoDbConnectorConfig.WriteConcernUnacknowledged => WriteConcern.Unacknowledged,
            _ => WriteConcern.WMajority
        };

        _client = new MongoClient(connectionString);
        var database = _client.GetDatabase(databaseName);
        _collection = database.GetCollection<BsonDocument>(collectionName).WithWriteConcern(mongoWriteConcern);
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        FlushBuffer();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _buffer.Clear();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        _buffer.AddRange(records);
        if (_buffer.Count >= _batchSize)
        {
            await FlushBufferAsync(cancellationToken);
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        await FlushBufferAsync(cancellationToken);
    }

    private void FlushBuffer() => FlushBufferAsync(CancellationToken.None).GetAwaiter().GetResult();

    private async Task FlushBufferAsync(CancellationToken cancellationToken)
    {
        if (_buffer.Count == 0)
            return;

        var attempt = 0;
        while (true)
        {
            try
            {
                var writeModels = new List<WriteModel<BsonDocument>>();

                foreach (var record in _buffer)
                {
                    try
                    {
                        var doc = BsonDocument.Parse(Encoding.UTF8.GetString(record.Value));
                        var docId = GetDocumentId(record, doc);

                        switch (_writeMode)
                        {
                            case MongoDbConnectorConfig.WriteModeInsert:
                                EnsureDocumentHasId(doc, docId);
                                writeModels.Add(new InsertOneModel<BsonDocument>(doc));
                                break;

                            case MongoDbConnectorConfig.WriteModeUpsert:
                                EnsureDocumentHasId(doc, docId);
                                var upsertFilter = Builders<BsonDocument>.Filter.Eq("_id", docId);
                                writeModels.Add(new ReplaceOneModel<BsonDocument>(upsertFilter, doc) { IsUpsert = true });
                                break;

                            case MongoDbConnectorConfig.WriteModeReplace:
                                EnsureDocumentHasId(doc, docId);
                                var replaceFilter = Builders<BsonDocument>.Filter.Eq("_id", docId);
                                writeModels.Add(new ReplaceOneModel<BsonDocument>(replaceFilter, doc));
                                break;
                        }
                    }
                    catch (BsonException)
                    {
                        // Skip invalid BSON documents
                    }
                }

                if (writeModels.Count > 0)
                {
                    await _collection!.BulkWriteAsync(writeModels, new BulkWriteOptions { IsOrdered = false }, cancellationToken);
                }

                _buffer.Clear();
                return;
            }
            catch (MongoException) when (attempt < _retryMax)
            {
                attempt++;
                await Task.Delay((int)(_retryBackoffMs * Math.Pow(2, attempt - 1)), cancellationToken);
            }
        }
    }

    private BsonValue GetDocumentId(SinkRecord record, BsonDocument doc)
    {
        return _docIdStrategy switch
        {
            MongoDbConnectorConfig.DocumentIdStrategyAuto =>
                doc.Contains("_id") ? doc["_id"] : ObjectId.GenerateNewId(),

            MongoDbConnectorConfig.DocumentIdStrategyKey =>
                record.Key != null ? BsonValue.Create(Encoding.UTF8.GetString(record.Key)) : ObjectId.GenerateNewId(),

            MongoDbConnectorConfig.DocumentIdStrategyField =>
                doc.Contains(_docIdField) ? doc[_docIdField] : ObjectId.GenerateNewId(),

            _ => ObjectId.GenerateNewId()
        };
    }

    private static void EnsureDocumentHasId(BsonDocument doc, BsonValue docId)
    {
        if (!doc.Contains("_id"))
        {
            doc["_id"] = docId;
        }
    }
}
