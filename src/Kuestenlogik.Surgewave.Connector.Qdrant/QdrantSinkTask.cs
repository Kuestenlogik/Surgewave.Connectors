namespace Kuestenlogik.Surgewave.Connector.Qdrant;

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using global::Qdrant.Client;
using global::Qdrant.Client.Grpc;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that upserts vector embeddings to Qdrant.
/// Expects input records with JSON containing a vector field (array of floats).
/// </summary>
public sealed class QdrantSinkTask : SinkTask
{
    private QdrantClient? _client;
    private string _collection = "";
    private bool _createCollection = QdrantConnectorConfig.DefaultCreateCollection;
    private int _vectorSize = QdrantConnectorConfig.DefaultVectorSize;
    private string _distanceMetric = QdrantConnectorConfig.DistanceCosine;
    private string _vectorField = QdrantConnectorConfig.DefaultVectorField;
    private string _idField = "";
    private string _idStrategy = QdrantConnectorConfig.IdStrategyAuto;
    private HashSet<string> _payloadFields = [];
    private bool _allPayloadFields = true;
    private int _batchSize = QdrantConnectorConfig.DefaultBatchSize;
    private int _retryMax = QdrantConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = QdrantConnectorConfig.DefaultRetryBackoffMs;
    private readonly List<SinkRecord> _buffer = [];
    private bool _collectionEnsured;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var host = GetConfig(config, QdrantConnectorConfig.HostConfig, QdrantConnectorConfig.DefaultHost);
        var port = int.Parse(GetConfig(config, QdrantConnectorConfig.PortConfig, QdrantConnectorConfig.DefaultPort.ToString()));
        var https = bool.Parse(GetConfig(config, QdrantConnectorConfig.HttpsConfig, "false"));
        var apiKey = GetConfig(config, QdrantConnectorConfig.ApiKeyConfig, "");

        _collection = config[QdrantConnectorConfig.CollectionConfig];
        _createCollection = bool.Parse(GetConfig(config, QdrantConnectorConfig.CreateCollectionConfig, QdrantConnectorConfig.DefaultCreateCollection.ToString()));
        _vectorSize = int.Parse(GetConfig(config, QdrantConnectorConfig.VectorSizeConfig, QdrantConnectorConfig.DefaultVectorSize.ToString()));
        _distanceMetric = GetConfig(config, QdrantConnectorConfig.DistanceMetricConfig, QdrantConnectorConfig.DistanceCosine);
        _vectorField = GetConfig(config, QdrantConnectorConfig.VectorFieldConfig, QdrantConnectorConfig.DefaultVectorField);
        _idField = GetConfig(config, QdrantConnectorConfig.IdFieldConfig, "");
        _idStrategy = GetConfig(config, QdrantConnectorConfig.IdStrategyConfig, QdrantConnectorConfig.IdStrategyAuto);
        _batchSize = int.Parse(GetConfig(config, QdrantConnectorConfig.BatchSizeConfig, QdrantConnectorConfig.DefaultBatchSize.ToString()));
        _retryMax = int.Parse(GetConfig(config, QdrantConnectorConfig.RetryMaxConfig, QdrantConnectorConfig.DefaultRetryMax.ToString()));
        _retryBackoffMs = int.Parse(GetConfig(config, QdrantConnectorConfig.RetryBackoffMsConfig, QdrantConnectorConfig.DefaultRetryBackoffMs.ToString()));

        // Parse payload fields
        var payloadFieldsStr = GetConfig(config, QdrantConnectorConfig.PayloadFieldsConfig, "");
        if (!string.IsNullOrEmpty(payloadFieldsStr))
        {
            _payloadFields = payloadFieldsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet(StringComparer.Ordinal);
            _allPayloadFields = false;
        }

        // Create Qdrant client
        _client = string.IsNullOrEmpty(apiKey)
            ? new QdrantClient(host, port, https)
            : new QdrantClient(host, port, https, apiKey);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0)
            return;

        _buffer.AddRange(records);

        if (_buffer.Count >= _batchSize)
        {
            await FlushBufferAsync(cancellationToken);
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        if (_buffer.Count > 0)
        {
            await FlushBufferAsync(cancellationToken);
        }
    }

    private async Task FlushBufferAsync(CancellationToken cancellationToken)
    {
        if (_client == null)
            return;

        var batch = _buffer.ToList();
        _buffer.Clear();

        // Ensure collection exists
        if (!_collectionEnsured)
        {
            await EnsureCollectionAsync(cancellationToken);
            _collectionEnsured = true;
        }

        // Parse records into points
        var points = new List<PointStruct>();

        foreach (var record in batch)
        {
            try
            {
                var point = ParseRecordToPoint(record);
                if (point != null)
                {
                    points.Add(point);
                }
            }
            catch (Exception ex)
            {
                Context?.RaiseError?.Invoke(new Exception(
                    $"Failed to parse record {record.Topic}:{record.Partition}:{record.Offset}: {ex.Message}", ex));
            }
        }

        if (points.Count == 0)
            return;

        // Upsert with retry
        for (int attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                await _client.UpsertAsync(_collection, points, cancellationToken: cancellationToken);
                break;
            }
            catch (Exception ex) when (attempt < _retryMax)
            {
                Context?.RaiseError?.Invoke(new Exception(
                    $"Qdrant upsert failed (attempt {attempt + 1}/{_retryMax + 1}): {ex.Message}"));
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }
    }

    private async Task EnsureCollectionAsync(CancellationToken cancellationToken)
    {
        if (_client == null || !_createCollection)
            return;

        try
        {
            // Check if collection exists
            var collections = await _client.ListCollectionsAsync(cancellationToken);
            if (collections.Contains(_collection))
                return;

            // Create collection with specified vector size and distance metric
            var distance = _distanceMetric switch
            {
                QdrantConnectorConfig.DistanceEuclid => Distance.Euclid,
                QdrantConnectorConfig.DistanceDot => Distance.Dot,
                _ => Distance.Cosine
            };

            await _client.CreateCollectionAsync(
                _collection,
                new VectorParams { Size = (ulong)_vectorSize, Distance = distance },
                cancellationToken: cancellationToken);
        }
        catch (Exception ex)
        {
            Context?.RaiseError?.Invoke(new Exception($"Failed to ensure collection '{_collection}': {ex.Message}", ex));
            throw;
        }
    }

    private PointStruct? ParseRecordToPoint(SinkRecord record)
    {
        var rawValue = Encoding.UTF8.GetString(record.Value);
        JsonNode? json;

        try
        {
            json = JsonNode.Parse(rawValue);
        }
        catch (JsonException)
        {
            return null; // Skip non-JSON records
        }

        if (json == null)
            return null;

        // Extract vector
        var vectorNode = json[_vectorField];
        if (vectorNode == null)
            return null;

        float[] vector;
        try
        {
            vector = vectorNode.AsArray()
                .Select(n => n?.GetValue<float>() ?? 0f)
                .ToArray();
        }
        catch
        {
            return null; // Invalid vector format
        }

        // Generate point ID
        var pointId = GeneratePointId(record, json);

        // Build payload (all fields except vector, or specified fields only)
        var payload = new Dictionary<string, Value>();

        if (json is JsonObject jsonObj)
        {
            foreach (var prop in jsonObj)
            {
                // Skip vector field
                if (string.Equals(prop.Key, _vectorField, StringComparison.Ordinal))
                    continue;

                // Filter by payload fields if specified
                if (!_allPayloadFields && !_payloadFields.Contains(prop.Key))
                    continue;

                var value = ConvertToQdrantValue(prop.Value);
                if (value != null)
                {
                    payload[prop.Key] = value;
                }
            }
        }

        // Add metadata
        payload["_topic"] = new Value { StringValue = record.Topic };
        payload["_partition"] = new Value { IntegerValue = record.Partition };
        payload["_offset"] = new Value { IntegerValue = record.Offset };
        payload["_timestamp"] = new Value { IntegerValue = record.Timestamp.ToUnixTimeMilliseconds() };

        return new PointStruct
        {
            Id = pointId,
            Vectors = vector,
            Payload = { payload }
        };
    }

    private PointId GeneratePointId(SinkRecord record, JsonNode json)
    {
        return _idStrategy switch
        {
            QdrantConnectorConfig.IdStrategyField when !string.IsNullOrEmpty(_idField) =>
                GetFieldPointId(json),
            QdrantConnectorConfig.IdStrategyKey when record.Key != null =>
                new PointId { Uuid = CreateDeterministicGuid(Encoding.UTF8.GetString(record.Key)).ToString() },
            _ => new PointId { Uuid = Guid.NewGuid().ToString() }
        };
    }

    private PointId GetFieldPointId(JsonNode json)
    {
        var fieldValue = json[_idField];
        if (fieldValue == null)
            return new PointId { Uuid = Guid.NewGuid().ToString() };

        // Try to parse as number first (Qdrant supports numeric IDs)
        if (fieldValue is JsonValue jv)
        {
            if (jv.TryGetValue(out long longVal))
                return new PointId { Num = (ulong)longVal };
            if (jv.TryGetValue(out ulong ulongVal))
                return new PointId { Num = ulongVal };
            if (jv.TryGetValue(out int intVal))
                return new PointId { Num = (ulong)intVal };
        }

        // Fall back to UUID based on string value
        var stringVal = fieldValue.ToString();
        if (Guid.TryParse(stringVal, out var guid))
            return new PointId { Uuid = guid.ToString() };

        return new PointId { Uuid = CreateDeterministicGuid(stringVal).ToString() };
    }

    private static Guid CreateDeterministicGuid(string input)
    {
        // Using SHA256 and taking first 16 bytes for deterministic UUID generation
        // This is not for cryptographic security, just for consistent ID mapping
        var hash = System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return new Guid(hash.AsSpan(0, 16));
    }

    private static Value? ConvertToQdrantValue(JsonNode? node)
    {
        if (node == null)
            return null;

        return node switch
        {
            JsonValue jv when jv.TryGetValue(out string? s) => new Value { StringValue = s },
            JsonValue jv when jv.TryGetValue(out bool b) => new Value { BoolValue = b },
            JsonValue jv when jv.TryGetValue(out long l) => new Value { IntegerValue = l },
            JsonValue jv when jv.TryGetValue(out double d) => new Value { DoubleValue = d },
            JsonArray ja => new Value
            {
                ListValue = new ListValue
                {
                    Values = { ja.Select(ConvertToQdrantValue).Where(v => v != null).Cast<Value>() }
                }
            },
            JsonObject jo => new Value
            {
                StructValue = new Struct
                {
                    Fields =
                    {
                        jo.Where(p => ConvertToQdrantValue(p.Value) != null)
                          .ToDictionary(p => p.Key, p => ConvertToQdrantValue(p.Value)!)
                    }
                }
            },
            _ => new Value { StringValue = node.ToString() }
        };
    }

    public override void Stop()
    {
        _buffer.Clear();
        _client?.Dispose();
        _client = null;
        _collectionEnsured = false;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _client?.Dispose();
            _client = null;
        }
        base.Dispose(disposing);
    }

    private static string GetConfig(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;
}
