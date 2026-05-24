using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Neo4j.Driver;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Neo4j;

/// <summary>
/// Task that writes graph data to Neo4j using Cypher queries.
/// Supports MERGE and CREATE operations with batch transactions.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Driver disposed in Stop()")]
public sealed class Neo4jSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private IDriver? _driver;
    private string _database = Neo4jConnectorConfig.DefaultDatabase;
    private string _label = "";
    private string _writeMode = Neo4jConnectorConfig.DefaultWriteMode;
    private int _batchSize = Neo4jConnectorConfig.DefaultBatchSize;
    private int _maxRetryCount = Neo4jConnectorConfig.DefaultMaxRetryCount;
    private long _retryDelayMs = Neo4jConnectorConfig.DefaultRetryDelayMs;
    private string[] _mergeProperties = [];
    private string _nodeLabelField = "";
    private string _idProperty = "";
    private string _customCypher = "";
    private string _unwindParameter = Neo4jConnectorConfig.DefaultUnwindParameter;

    private readonly List<Dictionary<string, object?>> _batch = [];

    public override void Start(IDictionary<string, string> config)
    {
        var uri = config[Neo4jConnectorConfig.UriConfig];
        var username = GetConfigValue(config, Neo4jConnectorConfig.UsernameConfig, "");
        var password = GetConfigValue(config, Neo4jConnectorConfig.PasswordConfig, "");
        _database = GetConfigValue(config, Neo4jConnectorConfig.DatabaseConfig, Neo4jConnectorConfig.DefaultDatabase);
        _label = GetConfigValue(config, Neo4jConnectorConfig.LabelConfig, "");
        _writeMode = GetConfigValue(config, Neo4jConnectorConfig.WriteModeConfig, Neo4jConnectorConfig.DefaultWriteMode);
        _batchSize = int.Parse(GetConfigValue(config, Neo4jConnectorConfig.BatchSizeConfig, Neo4jConnectorConfig.DefaultBatchSize.ToString()));
        _maxRetryCount = int.Parse(GetConfigValue(config, Neo4jConnectorConfig.MaxRetryCountConfig, Neo4jConnectorConfig.DefaultMaxRetryCount.ToString()));
        _retryDelayMs = long.Parse(GetConfigValue(config, Neo4jConnectorConfig.RetryDelayMsConfig, Neo4jConnectorConfig.DefaultRetryDelayMs.ToString()));
        _nodeLabelField = GetConfigValue(config, Neo4jConnectorConfig.NodeLabelFieldConfig, "");
        _idProperty = GetConfigValue(config, Neo4jConnectorConfig.IdPropertyConfig, "");
        _customCypher = GetConfigValue(config, Neo4jConnectorConfig.CustomCypherConfig, "");
        _unwindParameter = GetConfigValue(config, Neo4jConnectorConfig.UnwindParameterConfig, Neo4jConnectorConfig.DefaultUnwindParameter);
        var encrypted = bool.Parse(GetConfigValue(config, Neo4jConnectorConfig.EncryptedConfig, "false"));

        var mergePropertiesStr = GetConfigValue(config, Neo4jConnectorConfig.MergePropertiesConfig, "");
        _mergeProperties = string.IsNullOrEmpty(mergePropertiesStr) ? [] : mergePropertiesStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var authToken = string.IsNullOrEmpty(username) ? AuthTokens.None : AuthTokens.Basic(username, password);

        _driver = GraphDatabase.Driver(new Uri(uri), authToken, builder =>
        {
            if (encrypted)
                builder.WithEncryptionLevel(EncryptionLevel.Encrypted);
            else
                builder.WithEncryptionLevel(EncryptionLevel.None);
        });
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    public override void Stop()
    {
        FlushBatch().GetAwaiter().GetResult();
        _driver?.Dispose();
        _driver = null;
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
        foreach (var record in records)
        {
            // Skip tombstones (null value)
            if (record.Value == null)
                continue;

            var data = ParseRecordValue(record);
            if (data == null || data.Count == 0)
                continue;

            _batch.Add(data);

            if (_batch.Count >= _batchSize)
            {
                await FlushBatch();
            }
        }
    }

    private async Task FlushBatch()
    {
        if (_batch.Count == 0 || _driver == null)
            return;

        var retryCount = 0;
        while (retryCount < _maxRetryCount)
        {
            try
            {
                await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));

                await session.ExecuteWriteAsync(async tx =>
                {
                    var query = BuildBatchQuery();
                    var parameters = new Dictionary<string, object?> { [_unwindParameter] = _batch };

                    await tx.RunAsync(query, parameters);
                });

                _batch.Clear();
                return;
            }
            catch (Exception ex)
            {
                retryCount++;
                if (retryCount >= _maxRetryCount)
                {
                    Console.Error.WriteLine($"Neo4j batch write failed after {_maxRetryCount} retries: {ex.Message}");
                    throw;
                }
                await Task.Delay(TimeSpan.FromMilliseconds(_retryDelayMs * retryCount));
            }
        }
    }

    private string BuildBatchQuery()
    {
        if (!string.IsNullOrEmpty(_customCypher))
        {
            return _customCypher;
        }

        var sb = new StringBuilder();
        sb.AppendLine($"UNWIND ${_unwindParameter} AS event");

        // Determine label
        var labelExpression = !string.IsNullOrEmpty(_nodeLabelField)
            ? $"event.{_nodeLabelField}"
            : $"'{_label}'";

        if (_writeMode.Equals("merge", StringComparison.OrdinalIgnoreCase))
        {
            // Build MERGE with identity properties
            if (_mergeProperties.Length > 0 || !string.IsNullOrEmpty(_idProperty))
            {
                var mergeProps = _mergeProperties.Length > 0
                    ? _mergeProperties
                    : [_idProperty];

                var mergePropsStr = string.Join(", ", mergeProps.Select(p => $"{p}: event.{p}"));
                sb.AppendLine($"MERGE (n:{_label} {{{mergePropsStr}}})");
            }
            else
            {
                // Simple MERGE on all properties (not recommended for production)
                sb.AppendLine($"MERGE (n:{_label})");
            }

            sb.AppendLine("SET n += event");
        }
        else // create
        {
            // Use dynamic label if configured
            if (!string.IsNullOrEmpty(_nodeLabelField))
            {
                // Dynamic label requires APOC or post-processing
                // For simplicity, we'll use the static label
                sb.AppendLine($"CREATE (n:{_label})");
            }
            else
            {
                sb.AppendLine($"CREATE (n:{_label})");
            }

            sb.AppendLine("SET n = event");
        }

        return sb.ToString();
    }

    private Dictionary<string, object?>? ParseRecordValue(SinkRecord record)
    {
        if (record.Value == null)
            return null;

        try
        {
            var json = Encoding.UTF8.GetString(record.Value);
            var dict = JsonSerializer.Deserialize<Dictionary<string, object?>>(json, JsonSerializerOptions);

            if (dict == null)
                return null;

            // Convert JsonElement values to Neo4j-compatible types
            return dict.ToDictionary(
                kv => kv.Key,
                kv => ConvertToNeo4jType(kv.Value)
            );
        }
        catch
        {
            return null;
        }
    }

    private static object? ConvertToNeo4jType(object? value)
    {
        if (value == null)
            return null;

        if (value is JsonElement je)
        {
            return je.ValueKind switch
            {
                JsonValueKind.Null => null,
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Number when je.TryGetInt64(out var l) => l,
                JsonValueKind.Number when je.TryGetDouble(out var d) => d,
                JsonValueKind.String => je.GetString(),
                JsonValueKind.Array => je.EnumerateArray().Select(e => ConvertToNeo4jType(e)).ToList(),
                JsonValueKind.Object => je.EnumerateObject().ToDictionary(p => p.Name, p => ConvertToNeo4jType(p.Value)),
                _ => je.ToString()
            };
        }

        return value;
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return FlushBatch();
    }

    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };
}
