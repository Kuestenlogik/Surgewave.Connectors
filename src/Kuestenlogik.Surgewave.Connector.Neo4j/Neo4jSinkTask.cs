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

        if (!string.IsNullOrEmpty(_customCypher))
        {
            // A custom statement does its own labelling.
            await WriteRowsAsync(_customCypher, _batch);
            _batch.Clear();
            return;
        }

        // With neo4j.node.label.field configured the label varies per record, so the
        // batch is written as one UNWIND per distinct label instead of forcing every
        // record onto the static label.
        foreach (var group in _batch.GroupBy(ResolveLabel, StringComparer.Ordinal))
        {
            var rows = group.ToList();

            if (string.IsNullOrEmpty(group.Key))
            {
                // Poison records: no label to write them under. Skip, but stay visible.
                Context?.RaiseError?.Invoke(new InvalidOperationException(
                    $"Skipping {rows.Count} record(s): neither '{Neo4jConnectorConfig.NodeLabelFieldConfig}' nor " +
                    $"'{Neo4jConnectorConfig.LabelConfig}' yielded a node label"));
                continue;
            }

            await WriteRowsAsync(BuildBatchQuery(group.Key), rows);
        }

        _batch.Clear();
    }

    private async Task WriteRowsAsync(string query, IReadOnlyList<Dictionary<string, object?>> rows)
    {
        var retryCount = 0;
        while (true)
        {
            try
            {
                await using var session = _driver!.AsyncSession(o => o.WithDatabase(_database));

                await session.ExecuteWriteAsync(async tx =>
                {
                    var parameters = new Dictionary<string, object?> { [_unwindParameter] = rows };

                    await tx.RunAsync(query, parameters);
                });

                return;
            }
            catch (Exception ex)
            {
                retryCount++;
                if (retryCount >= _maxRetryCount)
                {
                    Context?.RaiseError?.Invoke(ex);
                    throw;
                }
                await Task.Delay(TimeSpan.FromMilliseconds(_retryDelayMs * retryCount));
            }
        }
    }

    /// <summary>
    /// Label for a single row: the value of the configured label field when present,
    /// otherwise the static label.
    /// </summary>
    private string ResolveLabel(Dictionary<string, object?> row)
    {
        if (!string.IsNullOrEmpty(_nodeLabelField) &&
            row.TryGetValue(_nodeLabelField, out var value) &&
            value?.ToString() is { Length: > 0 } dynamicLabel)
        {
            return dynamicLabel;
        }

        return _label;
    }

    /// <summary>
    /// Quotes a label as a Cypher identifier. Labels can originate from record data when
    /// neo4j.node.label.field is configured, so they are never inlined unescaped.
    /// </summary>
    private static string EscapeLabel(string label)
        => $"`{label.Replace("`", "``", StringComparison.Ordinal)}`";

    private string BuildBatchQuery(string label)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"UNWIND ${_unwindParameter} AS event");

        var escapedLabel = EscapeLabel(label);

        if (_writeMode.Equals("merge", StringComparison.OrdinalIgnoreCase))
        {
            // Build MERGE with identity properties
            if (_mergeProperties.Length > 0 || !string.IsNullOrEmpty(_idProperty))
            {
                var mergeProps = _mergeProperties.Length > 0
                    ? _mergeProperties
                    : [_idProperty];

                var mergePropsStr = string.Join(", ", mergeProps.Select(p => $"{p}: event.{p}"));
                sb.AppendLine($"MERGE (n:{escapedLabel} {{{mergePropsStr}}})");
            }
            else
            {
                // Simple MERGE on all properties (not recommended for production)
                sb.AppendLine($"MERGE (n:{escapedLabel})");
            }

            sb.AppendLine("SET n += event");
        }
        else // create
        {
            sb.AppendLine($"CREATE (n:{escapedLabel})");
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
        catch (JsonException ex)
        {
            // Poison record: skip it, but surface it instead of dropping it silently.
            Context?.RaiseError?.Invoke(new InvalidOperationException(
                $"Skipping record {record.Topic}[{record.Partition}]@{record.Offset}: value is not valid JSON", ex));
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
