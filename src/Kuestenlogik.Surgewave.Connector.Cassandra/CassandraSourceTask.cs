using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Cassandra;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Cassandra;

/// <summary>
/// Task that reads data from Cassandra tables or custom CQL queries.
/// Supports incremental polling with timestamp-based tracking.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Session disposed in Stop()")]
public sealed class CassandraSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private ICluster? _cluster;
    private ISession? _session;
    private string _keyspace = "";
    private string _table = "";
    private string _query = "";
    private string _mode = CassandraConnectorConfig.DefaultMode;
    private string _topicPattern = CassandraConnectorConfig.DefaultTopicPattern;
    private long _pollIntervalMs = CassandraConnectorConfig.DefaultPollIntervalMs;
    private int _maxRowsPerPoll = CassandraConnectorConfig.DefaultMaxRowsPerPoll;
    private bool _includeMetadata = true;
    private string _timestampColumn = "";
    private string[] _partitionKeyColumns = [];
    private string[] _clusteringKeyColumns = [];

    private DateTimeOffset? _lastTimestamp;
    private DateTime _lastPollTime = DateTime.MinValue;
    private IDictionary<string, object> _sourcePartition = new Dictionary<string, object>();

    public override void Start(IDictionary<string, string> config)
    {
        _keyspace = config[CassandraConnectorConfig.KeyspaceConfig];
        _mode = GetConfigValue(config, CassandraConnectorConfig.ModeConfig, CassandraConnectorConfig.DefaultMode);
        _table = GetConfigValue(config, CassandraConnectorConfig.TableConfig, "");
        _query = GetConfigValue(config, CassandraConnectorConfig.QueryConfig, "");
        _topicPattern = GetConfigValue(config, CassandraConnectorConfig.TopicPatternConfig, CassandraConnectorConfig.DefaultTopicPattern);
        _pollIntervalMs = long.Parse(GetConfigValue(config, CassandraConnectorConfig.PollIntervalMsConfig, CassandraConnectorConfig.DefaultPollIntervalMs.ToString()));
        _maxRowsPerPoll = int.Parse(GetConfigValue(config, CassandraConnectorConfig.MaxRowsPerPollConfig, CassandraConnectorConfig.DefaultMaxRowsPerPoll.ToString()));
        _includeMetadata = bool.Parse(GetConfigValue(config, CassandraConnectorConfig.IncludeMetadataConfig, "true"));
        _timestampColumn = GetConfigValue(config, CassandraConnectorConfig.TimestampColumnConfig, "");

        var partitionKeys = GetConfigValue(config, CassandraConnectorConfig.PartitionKeyColumnsConfig, "");
        _partitionKeyColumns = string.IsNullOrEmpty(partitionKeys) ? [] : partitionKeys.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var clusteringKeys = GetConfigValue(config, CassandraConnectorConfig.ClusteringKeyColumnsConfig, "");
        _clusteringKeyColumns = string.IsNullOrEmpty(clusteringKeys) ? [] : clusteringKeys.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        _sourcePartition = new Dictionary<string, object>
        {
            [CassandraConnectorConfig.OffsetTable] = _table
        };

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

    public override void Stop()
    {
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

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_session == null)
            return [];

        // Respect poll interval
        var elapsed = DateTime.UtcNow - _lastPollTime;
        if (elapsed.TotalMilliseconds < _pollIntervalMs)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs - elapsed.TotalMilliseconds), cancellationToken);
        }

        _lastPollTime = DateTime.UtcNow;

        var records = new List<SourceRecord>();

        try
        {
            var cql = BuildQuery();
            var statement = new SimpleStatement(cql);
            statement.SetPageSize(_maxRowsPerPoll);

            var resultSet = await _session.ExecuteAsync(statement);

            foreach (var row in resultSet)
            {
                var record = CreateSourceRecord(row, resultSet.Columns);
                records.Add(record);

                // Track timestamp for incremental polling
                if (!string.IsNullOrEmpty(_timestampColumn))
                {
                    var colIndex = Array.FindIndex(resultSet.Columns, c => c.Name.Equals(_timestampColumn, StringComparison.OrdinalIgnoreCase));
                    if (colIndex >= 0 && !row.IsNull(colIndex))
                    {
                        var tsValue = row.GetValue<DateTimeOffset>(colIndex);
                        if (!_lastTimestamp.HasValue || tsValue > _lastTimestamp.Value)
                            _lastTimestamp = tsValue;
                    }
                }

                if (records.Count >= _maxRowsPerPoll)
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Cassandra poll error: {ex.Message}");
        }

        return records;
    }

    private string BuildQuery()
    {
        if (_mode.Equals("query", StringComparison.OrdinalIgnoreCase))
        {
            var query = _query;

            // Add timestamp filter if configured and supported
            if (!string.IsNullOrEmpty(_timestampColumn) && _lastTimestamp.HasValue)
            {
                // Note: Cassandra requires filtering on primary key columns for efficient queries
                // This is a best-effort approach; users should configure partition keys appropriately
                if (query.Contains(" WHERE ", StringComparison.OrdinalIgnoreCase))
                {
                    query = query.Replace(" ALLOW FILTERING", "", StringComparison.OrdinalIgnoreCase);
                    query += $" AND {_timestampColumn} > '{_lastTimestamp.Value:yyyy-MM-dd HH:mm:ss.fff}+0000' ALLOW FILTERING";
                }
                else if (query.Contains(" ORDER BY ", StringComparison.OrdinalIgnoreCase))
                {
                    query = query.Replace(" ORDER BY ", $" WHERE {_timestampColumn} > '{_lastTimestamp.Value:yyyy-MM-dd HH:mm:ss.fff}+0000' ORDER BY ", StringComparison.OrdinalIgnoreCase);
                    query += " ALLOW FILTERING";
                }
                else
                {
                    query += $" WHERE {_timestampColumn} > '{_lastTimestamp.Value:yyyy-MM-dd HH:mm:ss.fff}+0000' ALLOW FILTERING";
                }
            }

            return query + $" LIMIT {_maxRowsPerPoll}";
        }
        else
        {
            // Table mode
            var sb = new StringBuilder();
            sb.Append($"SELECT * FROM {_table}");

            if (!string.IsNullOrEmpty(_timestampColumn) && _lastTimestamp.HasValue)
            {
                sb.Append($" WHERE {_timestampColumn} > '{_lastTimestamp.Value:yyyy-MM-dd HH:mm:ss.fff}+0000'");
                sb.Append(" ALLOW FILTERING");
            }

            sb.Append($" LIMIT {_maxRowsPerPoll}");

            return sb.ToString();
        }
    }

    private SourceRecord CreateSourceRecord(Row row, CqlColumn[] columns)
    {
        var rowDict = new Dictionary<string, object?>();
        for (var i = 0; i < columns.Length; i++)
        {
            var column = columns[i];
            rowDict[column.Name] = row.IsNull(i) ? null : ConvertCassandraValue(row.GetValue<object>(i));
        }

        var topic = BuildTopic();
        var key = BuildKey(rowDict);
        var value = JsonSerializer.SerializeToUtf8Bytes(rowDict, JsonSerializerOptions);
        var headers = BuildHeaders(rowDict);

        var sourceOffset = new Dictionary<string, object>();
        if (!string.IsNullOrEmpty(_timestampColumn) && rowDict.TryGetValue(_timestampColumn, out var ts) && ts != null)
        {
            sourceOffset[CassandraConnectorConfig.OffsetTimestamp] = ts.ToString()!;
        }

        // Add partition key to offset
        if (_partitionKeyColumns.Length > 0)
        {
            var pkValues = _partitionKeyColumns
                .Where(pk => rowDict.ContainsKey(pk))
                .Select(pk => rowDict[pk]?.ToString() ?? "");
            sourceOffset[CassandraConnectorConfig.OffsetPartitionKey] = string.Join(":", pkValues);
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = sourceOffset,
            Topic = topic,
            Key = key,
            Value = value,
            Timestamp = DateTimeOffset.UtcNow,
            Headers = headers
        };
    }

    private static object? ConvertCassandraValue(object? value)
    {
        if (value == null)
            return null;

        return value switch
        {
            LocalDate ld => ld.ToString(),
            LocalTime lt => lt.ToString(),
            Duration d => d.ToString(),
            TimeUuid tu => tu.ToString(),
            Guid g => g.ToString(),
            byte[] bytes => Convert.ToBase64String(bytes),
            System.Net.IPAddress ip => ip.ToString(),
            _ => value
        };
    }

    private string BuildTopic()
    {
        return _topicPattern
            .Replace("${keyspace}", _keyspace)
            .Replace("${table}", _table);
    }

    private byte[]? BuildKey(Dictionary<string, object?> row)
    {
        // Use partition key columns if configured
        if (_partitionKeyColumns.Length > 0)
        {
            var keyParts = _partitionKeyColumns
                .Where(pk => row.ContainsKey(pk) && row[pk] != null)
                .Select(pk => row[pk]!.ToString());
            var keyString = string.Join(":", keyParts);
            if (!string.IsNullOrEmpty(keyString))
                return Encoding.UTF8.GetBytes(keyString);
        }

        // Fall back to first column
        if (row.Count > 0)
        {
            var firstKey = row.Keys.First();
            var firstValue = row[firstKey];
            if (firstValue != null)
            {
                return Encoding.UTF8.GetBytes(firstValue.ToString()!);
            }
        }
        return null;
    }

    private Dictionary<string, byte[]> BuildHeaders(Dictionary<string, object?> row)
    {
        var headers = new Dictionary<string, byte[]>();

        if (_includeMetadata)
        {
            headers[CassandraConnectorConfig.HeaderKeyspace] = Encoding.UTF8.GetBytes(_keyspace);

            if (!string.IsNullOrEmpty(_table))
                headers[CassandraConnectorConfig.HeaderTable] = Encoding.UTF8.GetBytes(_table);

            headers[CassandraConnectorConfig.HeaderTimestamp] = Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O"));

            // Add partition key header
            if (_partitionKeyColumns.Length > 0)
            {
                var pkValues = _partitionKeyColumns
                    .Where(pk => row.ContainsKey(pk) && row[pk] != null)
                    .Select(pk => row[pk]!.ToString());
                headers[CassandraConnectorConfig.HeaderPartitionKey] = Encoding.UTF8.GetBytes(string.Join(":", pkValues));
            }

            // Add clustering key header
            if (_clusteringKeyColumns.Length > 0)
            {
                var ckValues = _clusteringKeyColumns
                    .Where(ck => row.ContainsKey(ck) && row[ck] != null)
                    .Select(ck => row[ck]!.ToString());
                headers[CassandraConnectorConfig.HeaderClusteringKey] = Encoding.UTF8.GetBytes(string.Join(":", ckValues));
            }
        }

        return headers;
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Offsets tracked internally
        return Task.CompletedTask;
    }

    public override void CommitRecord(SourceRecord record, RecordMetadata metadata)
    {
        // Individual record commit - nothing to do
    }

    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };
}
