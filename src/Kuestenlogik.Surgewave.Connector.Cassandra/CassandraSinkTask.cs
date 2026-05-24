using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Cassandra;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Cassandra;

/// <summary>
/// Task that writes data to Cassandra tables.
/// Supports batch inserts and configurable TTL.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Session disposed in Stop()")]
public sealed class CassandraSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private ICluster? _cluster;
    private ISession? _session;
    private string _keyspace = "";
    private string _table = "";
    private string _writeMode = CassandraConnectorConfig.DefaultWriteMode;
    private int _batchSize = CassandraConnectorConfig.DefaultBatchSize;
    private int _maxRetryCount = CassandraConnectorConfig.DefaultMaxRetryCount;
    private long _retryDelayMs = CassandraConnectorConfig.DefaultRetryDelayMs;
    private string _batchType = CassandraConnectorConfig.DefaultBatchType;
    private int _ttlSeconds = CassandraConnectorConfig.DefaultTtlSeconds;
    private string[] _partitionKeyColumns = [];
    private string[] _clusteringKeyColumns = [];

    private readonly List<SinkRecord> _batch = [];
    private PreparedStatement? _insertStatement;
    private string[]? _columnNames;

    public override void Start(IDictionary<string, string> config)
    {
        _keyspace = config[CassandraConnectorConfig.KeyspaceConfig];
        _table = config[CassandraConnectorConfig.TableConfig];
        _writeMode = GetConfigValue(config, CassandraConnectorConfig.WriteModeConfig, CassandraConnectorConfig.DefaultWriteMode);
        _batchSize = int.Parse(GetConfigValue(config, CassandraConnectorConfig.BatchSizeConfig, CassandraConnectorConfig.DefaultBatchSize.ToString()));
        _maxRetryCount = int.Parse(GetConfigValue(config, CassandraConnectorConfig.MaxRetryCountConfig, CassandraConnectorConfig.DefaultMaxRetryCount.ToString()));
        _retryDelayMs = long.Parse(GetConfigValue(config, CassandraConnectorConfig.RetryDelayMsConfig, CassandraConnectorConfig.DefaultRetryDelayMs.ToString()));
        _batchType = GetConfigValue(config, CassandraConnectorConfig.BatchTypeConfig, CassandraConnectorConfig.DefaultBatchType);
        _ttlSeconds = int.Parse(GetConfigValue(config, CassandraConnectorConfig.TtlSecondsConfig, CassandraConnectorConfig.DefaultTtlSeconds.ToString()));

        var partitionKeys = GetConfigValue(config, CassandraConnectorConfig.PartitionKeyColumnsConfig, "");
        _partitionKeyColumns = string.IsNullOrEmpty(partitionKeys) ? [] : partitionKeys.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var clusteringKeys = GetConfigValue(config, CassandraConnectorConfig.ClusteringKeyColumnsConfig, "");
        _clusteringKeyColumns = string.IsNullOrEmpty(clusteringKeys) ? [] : clusteringKeys.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        (_cluster, _session) = CreateSession(config);
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static (ICluster cluster, ISession session) CreateSession(IDictionary<string, string> config)
    {
        var contactPoints = config[CassandraConnectorConfig.ContactPointsConfig]
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var port = int.Parse(GetConfigValue(config, CassandraConnectorConfig.PortConfig, CassandraConnectorConfig.DefaultPort.ToString()));
        var datacenter = GetConfigValue(config, CassandraConnectorConfig.DatacenterConfig, "");
        var keyspace = config[CassandraConnectorConfig.KeyspaceConfig];
        var username = GetConfigValue(config, CassandraConnectorConfig.UsernameConfig, "");
        var password = GetConfigValue(config, CassandraConnectorConfig.PasswordConfig, "");
        var sslEnabled = bool.Parse(GetConfigValue(config, CassandraConnectorConfig.SslEnabledConfig, "false"));
        var consistencyLevel = ParseConsistencyLevel(GetConfigValue(config, CassandraConnectorConfig.ConsistencyLevelConfig, CassandraConnectorConfig.DefaultConsistencyLevel));

        var builder = Cluster.Builder()
            .AddContactPoints(contactPoints)
            .WithPort(port)
            .WithQueryOptions(new QueryOptions().SetConsistencyLevel(consistencyLevel));

        if (!string.IsNullOrEmpty(datacenter))
        {
            builder = builder.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(datacenter)));
        }

        if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
        {
            builder = builder.WithCredentials(username, password);
        }

        if (sslEnabled)
        {
            builder = builder.WithSSL();
        }

        var cluster = builder.Build();
        var session = cluster.Connect(keyspace);

        return (cluster, session);
    }

    private static ConsistencyLevel ParseConsistencyLevel(string level) => level.ToUpperInvariant() switch
    {
        "ANY" => ConsistencyLevel.Any,
        "ONE" => ConsistencyLevel.One,
        "TWO" => ConsistencyLevel.Two,
        "THREE" => ConsistencyLevel.Three,
        "QUORUM" => ConsistencyLevel.Quorum,
        "ALL" => ConsistencyLevel.All,
        "LOCAL_QUORUM" => ConsistencyLevel.LocalQuorum,
        "EACH_QUORUM" => ConsistencyLevel.EachQuorum,
        "SERIAL" => ConsistencyLevel.Serial,
        "LOCAL_SERIAL" => ConsistencyLevel.LocalSerial,
        "LOCAL_ONE" => ConsistencyLevel.LocalOne,
        _ => ConsistencyLevel.LocalQuorum
    };

    private static BatchType ParseBatchType(string type) => type.ToLowerInvariant() switch
    {
        "logged" => BatchType.Logged,
        "unlogged" => BatchType.Unlogged,
        "counter" => BatchType.Counter,
        _ => BatchType.Unlogged
    };

    public override void Stop()
    {
        FlushBatch().GetAwaiter().GetResult();
        _session?.Dispose();
        _cluster?.Dispose();
        _session = null;
        _cluster = null;
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
            // Handle tombstones (null value = delete)
            if (record.Value == null)
            {
                await FlushBatch();
                await DeleteRecordAsync(record, cancellationToken);
                continue;
            }

            _batch.Add(record);

            if (_batch.Count >= _batchSize)
            {
                await FlushBatch();
            }
        }
    }

    private async Task FlushBatch()
    {
        if (_batch.Count == 0 || _session == null)
            return;

        var retryCount = 0;
        while (retryCount < _maxRetryCount)
        {
            try
            {
                await WriteBatchAsync();
                _batch.Clear();
                return;
            }
            catch (Exception ex)
            {
                retryCount++;
                if (retryCount >= _maxRetryCount)
                {
                    Console.Error.WriteLine($"Cassandra batch write failed after {_maxRetryCount} retries: {ex.Message}");
                    throw;
                }
                await Task.Delay(TimeSpan.FromMilliseconds(_retryDelayMs * retryCount));
            }
        }
    }

    private async Task WriteBatchAsync()
    {
        if (_session == null || _batch.Count == 0)
            return;

        var batchType = ParseBatchType(_batchType);
        var batch = new BatchStatement();
        batch.SetBatchType(batchType);

        foreach (var record in _batch)
        {
            var statement = await CreateInsertStatementAsync(record);
            if (statement != null)
            {
                batch.Add(statement);
            }
        }

        await _session.ExecuteAsync(batch);
    }

    private async Task<BoundStatement?> CreateInsertStatementAsync(SinkRecord record)
    {
        if (record.Value == null)
            return null;

        var data = ParseRecordValue(record);
        if (data == null || data.Count == 0)
            return null;

        // Prepare insert statement if not already done or columns changed
        var columns = data.Keys.ToArray();
        if (_insertStatement == null || _columnNames == null || !columns.SequenceEqual(_columnNames))
        {
            _columnNames = columns;
            var columnList = string.Join(", ", columns);
            var placeholders = string.Join(", ", columns.Select(_ => "?"));

            var cql = _ttlSeconds > 0
                ? $"INSERT INTO {_table} ({columnList}) VALUES ({placeholders}) USING TTL {_ttlSeconds}"
                : $"INSERT INTO {_table} ({columnList}) VALUES ({placeholders})";

            _insertStatement = await _session!.PrepareAsync(cql);
        }

        var values = columns.Select(c => ConvertToCassandraValue(data[c])).ToArray();
        return _insertStatement.Bind(values);
    }

    private async Task DeleteRecordAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        if (_session == null)
            return;

        // Extract key for deletion
        var keyData = ParseKeyValue(record);
        if (keyData == null || keyData.Count == 0)
            return;

        // Build delete statement based on primary key columns
        var whereConditions = new List<string>();
        var values = new List<object?>();

        // Use configured partition key columns first
        foreach (var pk in _partitionKeyColumns)
        {
            if (keyData.TryGetValue(pk, out var value))
            {
                whereConditions.Add($"{pk} = ?");
                values.Add(ConvertToCassandraValue(value));
            }
        }

        // Then clustering key columns
        foreach (var ck in _clusteringKeyColumns)
        {
            if (keyData.TryGetValue(ck, out var value))
            {
                whereConditions.Add($"{ck} = ?");
                values.Add(ConvertToCassandraValue(value));
            }
        }

        // If no configured keys, try to use all key data
        if (whereConditions.Count == 0)
        {
            foreach (var kvp in keyData)
            {
                whereConditions.Add($"{kvp.Key} = ?");
                values.Add(ConvertToCassandraValue(kvp.Value));
            }
        }

        if (whereConditions.Count == 0)
            return;

        var cql = $"DELETE FROM {_table} WHERE {string.Join(" AND ", whereConditions)}";
        var statement = new SimpleStatement(cql, values.ToArray());

        await _session.ExecuteAsync(statement);
    }

    private Dictionary<string, object?>? ParseRecordValue(SinkRecord record)
    {
        if (record.Value == null)
            return null;

        try
        {
            var json = Encoding.UTF8.GetString(record.Value);
            return JsonSerializer.Deserialize<Dictionary<string, object?>>(json, JsonSerializerOptions);
        }
        catch
        {
            return null;
        }
    }

    private Dictionary<string, object?>? ParseKeyValue(SinkRecord record)
    {
        if (record.Key == null)
            return null;

        try
        {
            var keyString = Encoding.UTF8.GetString(record.Key);

            // Try parsing as JSON first
            if (keyString.StartsWith('{'))
            {
                return JsonSerializer.Deserialize<Dictionary<string, object?>>(keyString, JsonSerializerOptions);
            }

            // If partition key columns are configured, create key dictionary from colon-separated values
            if (_partitionKeyColumns.Length > 0)
            {
                var keyParts = keyString.Split(':');
                var keyDict = new Dictionary<string, object?>();
                for (var i = 0; i < Math.Min(_partitionKeyColumns.Length, keyParts.Length); i++)
                {
                    keyDict[_partitionKeyColumns[i]] = keyParts[i];
                }
                return keyDict;
            }

            return null;
        }
        catch
        {
            return null;
        }
    }

    private static object? ConvertToCassandraValue(object? value)
    {
        if (value == null)
            return null;

        // Handle JsonElement from deserialization
        if (value is JsonElement je)
        {
            return je.ValueKind switch
            {
                JsonValueKind.String => je.GetString(),
                JsonValueKind.Number when je.TryGetInt64(out var l) => l,
                JsonValueKind.Number when je.TryGetDouble(out var d) => d,
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                JsonValueKind.Array => je.EnumerateArray().Select(e => ConvertToCassandraValue(e)).ToList(),
                JsonValueKind.Object => je.EnumerateObject().ToDictionary(p => p.Name, p => ConvertToCassandraValue(p.Value)),
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
