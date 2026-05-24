using System.Data;
using System.Data.Common;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Database;

internal static class DictionaryExtensions
{
    public static string GetOrDefault(this IDictionary<string, string> dict, string key, string defaultValue)
    {
        return dict.TryGetValue(key, out var value) ? value : defaultValue;
    }
}

/// <summary>
/// A source connector that reads data from a database using JDBC-style polling.
/// Supports incremental queries using a timestamp or incrementing column.
/// </summary>
public sealed class DatabaseSourceConnector : SourceConnector
{
    private const string ConnectionStringConfig = "connection.string";
    private const string ProviderConfig = "db.provider";
    private const string TopicPrefixConfig = "topic.prefix";
    private const string TableWhitelistConfig = "table.whitelist";
    private const string QueryConfig = "query";
    private const string ModeConfig = "mode";
    private const string IncrementingColumnConfig = "incrementing.column";
    private const string TimestampColumnConfig = "timestamp.column";
    private const string PollIntervalMsConfig = "poll.interval.ms";
    private const string BatchMaxRowsConfig = "batch.max.rows";

    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(DatabaseSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(ConnectionStringConfig, ConfigType.String, Importance.High, "Database connection string")
        .Define(ProviderConfig, ConfigType.String, "SqlClient", Importance.High, "Database provider (SqlClient, Npgsql, MySql, Sqlite)")
        .Define(TopicPrefixConfig, ConfigType.String, "", Importance.Medium, "Prefix for generated topic names")
        .Define(TableWhitelistConfig, ConfigType.String, "", Importance.Medium, "Comma-separated list of tables to include")
        .Define(QueryConfig, ConfigType.String, "", Importance.Medium, "Custom query (overrides table whitelist)")
        .Define(ModeConfig, ConfigType.String, "bulk", Importance.Medium, "Query mode: bulk, incrementing, timestamp, timestamp+incrementing")
        .Define(IncrementingColumnConfig, ConfigType.String, "id", Importance.Medium, "Column name for incrementing mode")
        .Define(TimestampColumnConfig, ConfigType.String, "updated_at", Importance.Medium, "Column name for timestamp mode")
        .Define(PollIntervalMsConfig, ConfigType.Long, 5000L, Importance.Medium, "Poll interval in milliseconds")
        .Define(BatchMaxRowsConfig, ConfigType.Int, 1000, Importance.Medium, "Maximum rows per batch");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(ConnectionStringConfig, out var _))
        {
            throw new ArgumentException($"Missing required config: {ConnectionStringConfig}");
        }

        foreach (var kvp in config)
        {
            _config[kvp.Key] = kvp.Value;
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // For simplicity, single task. Could partition by table for parallelism.
        return [new Dictionary<string, string>(_config)];
    }
}

/// <summary>
/// Task that reads data from a database.
/// </summary>
public sealed class DatabaseSourceTask : SourceTask
{
    private const string ConnectionStringConfig = "connection.string";
    private const string ProviderConfig = "db.provider";
    private const string TopicPrefixConfig = "topic.prefix";
    private const string TableWhitelistConfig = "table.whitelist";
    private const string QueryConfig = "query";
    private const string ModeConfig = "mode";
    private const string IncrementingColumnConfig = "incrementing.column";
    private const string TimestampColumnConfig = "timestamp.column";
    private const string PollIntervalMsConfig = "poll.interval.ms";
    private const string BatchMaxRowsConfig = "batch.max.rows";
    private const string LastOffsetField = "last_offset";
    private const string LastTimestampField = "last_timestamp";

    public override string Version => "1.0.0";

    private string _connectionString = "";
    private string _provider = "SqlClient";
    private string _topicPrefix = "";
    private string _tableWhitelist = "";
    private string _query = "";
    private string _mode = "bulk";
    private string _incrementingColumn = "id";
    private string _timestampColumn = "updated_at";
    private long _pollIntervalMs = 5000;
    private int _batchMaxRows = 1000;
    private DbConnection? _connection;
    private DateTimeOffset _lastPollTime = DateTimeOffset.MinValue;
    private readonly Dictionary<string, object> _sourcePartition = new();
    private long _lastIncrementingValue;
    private DateTimeOffset _lastTimestampValue = DateTimeOffset.MinValue;

    public override void Start(IDictionary<string, string> config)
    {
        _connectionString = config[ConnectionStringConfig];
        _provider = config.GetOrDefault(ProviderConfig, "SqlClient");
        _topicPrefix = config.GetOrDefault(TopicPrefixConfig, "");
        _tableWhitelist = config.GetOrDefault(TableWhitelistConfig, "");
        _query = config.GetOrDefault(QueryConfig, "");
        _mode = config.GetOrDefault(ModeConfig, "bulk");
        _incrementingColumn = config.GetOrDefault(IncrementingColumnConfig, "id");
        _timestampColumn = config.GetOrDefault(TimestampColumnConfig, "updated_at");
        _pollIntervalMs = long.Parse(config.GetOrDefault(PollIntervalMsConfig, "5000"));
        _batchMaxRows = int.Parse(config.GetOrDefault(BatchMaxRowsConfig, "1000"));

        _sourcePartition["connection"] = _connectionString.GetHashCode().ToString();
        _sourcePartition["query"] = string.IsNullOrEmpty(_query) ? _tableWhitelist : _query;

        // Restore offset
        var storedOffset = Context.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null)
        {
            if (storedOffset.TryGetValue(LastOffsetField, out var lastOffset))
            {
                _lastIncrementingValue = Convert.ToInt64(lastOffset);
            }
            if (storedOffset.TryGetValue(LastTimestampField, out var lastTs))
            {
                _lastTimestampValue = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(lastTs));
            }
        }

        // Create database connection using DbProviderFactories
        _connection = CreateConnection(_provider, _connectionString);
        _connection.Open();
    }

    private static DbConnection CreateConnection(string provider, string connectionString)
    {
        // Use DbProviderFactories if available, otherwise return null and handle in PollAsync
        // This requires the appropriate provider package to be installed
        try
        {
            var factory = DbProviderFactories.GetFactory(provider switch
            {
                "SqlClient" => "Microsoft.Data.SqlClient",
                "Npgsql" => "Npgsql",
                "MySql" => "MySql.Data.MySqlClient",
                "Sqlite" => "Microsoft.Data.Sqlite",
                _ => provider
            });
            var connection = factory.CreateConnection()
                ?? throw new InvalidOperationException($"Failed to create connection for provider: {provider}");
            connection.ConnectionString = connectionString;
            return connection;
        }
        catch (ArgumentException)
        {
            // Provider not registered, throw helpful error
            throw new InvalidOperationException(
                $"Database provider '{provider}' is not registered. " +
                $"Ensure the appropriate NuGet package is installed and DbProviderFactories.RegisterFactory() is called.");
        }
    }

    public override void Stop()
    {
        _connection?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _connection?.Dispose();
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

        if (_connection == null || _connection.State != ConnectionState.Open)
        {
            return [];
        }

        var queryTemplate = BuildQuery();
        if (string.IsNullOrEmpty(queryTemplate))
        {
            return [];
        }

        var records = new List<SourceRecord>();
        var topic = GetTopic();

        await using var command = _connection.CreateCommand();
#pragma warning disable CA2100 // Query comes from trusted configuration, not user input
        command.CommandText = queryTemplate;
#pragma warning restore CA2100

        // Add parameters based on mode
        if (_mode is "incrementing" or "timestamp+incrementing")
        {
            var param = command.CreateParameter();
            param.ParameterName = "@lastValue";
            param.Value = _lastIncrementingValue;
            command.Parameters.Add(param);
        }

        if (_mode is "timestamp" or "timestamp+incrementing")
        {
            var param = command.CreateParameter();
            param.ParameterName = "@lastTimestamp";
            param.Value = _lastTimestampValue.UtcDateTime;
            command.Parameters.Add(param);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                row[columnName] = value;

                // Track incrementing/timestamp values
                if (columnName.Equals(_incrementingColumn, StringComparison.OrdinalIgnoreCase) && value != null)
                {
                    _lastIncrementingValue = Convert.ToInt64(value);
                }
                if (columnName.Equals(_timestampColumn, StringComparison.OrdinalIgnoreCase) && value != null)
                {
                    _lastTimestampValue = value switch
                    {
                        DateTime dt => new DateTimeOffset(dt, TimeSpan.Zero),
                        DateTimeOffset dto => dto,
                        _ => DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(value))
                    };
                }
            }

            var valueJson = JsonSerializer.SerializeToUtf8Bytes(row);
            var keyJson = BuildKeyJson(row);

            var sourceOffset = new Dictionary<string, object>
            {
                [LastOffsetField] = _lastIncrementingValue,
                [LastTimestampField] = _lastTimestampValue.ToUnixTimeMilliseconds()
            };

            records.Add(new SourceRecord
            {
                SourcePartition = _sourcePartition,
                SourceOffset = sourceOffset,
                Topic = topic,
                Key = keyJson,
                Value = valueJson
            });
        }

        return records;
    }

    private string GetTopic()
    {
        if (!string.IsNullOrEmpty(_query))
        {
            return string.IsNullOrEmpty(_topicPrefix) ? "query_results" : $"{_topicPrefix}query_results";
        }

        var tables = _tableWhitelist.Split(',', StringSplitOptions.RemoveEmptyEntries);
        var table = tables.Length > 0 ? tables[0].Trim() : "unknown";
        return string.IsNullOrEmpty(_topicPrefix) ? table : $"{_topicPrefix}{table}";
    }

    private byte[]? BuildKeyJson(Dictionary<string, object?> row)
    {
        // Use incrementing column as key if available
        if (row.TryGetValue(_incrementingColumn, out var keyValue) && keyValue != null)
        {
            return JsonSerializer.SerializeToUtf8Bytes(new { key = keyValue });
        }
        return null;
    }

    private string BuildQuery()
    {
        if (!string.IsNullOrEmpty(_query))
        {
            return _query;
        }

        var tables = _tableWhitelist.Split(',', StringSplitOptions.RemoveEmptyEntries);
        if (tables.Length == 0)
        {
            return "";
        }

        var table = tables[0].Trim();
        var query = $"SELECT * FROM {table}";

        switch (_mode)
        {
            case "incrementing":
                query += $" WHERE {_incrementingColumn} > @lastValue ORDER BY {_incrementingColumn} LIMIT {_batchMaxRows}";
                break;
            case "timestamp":
                query += $" WHERE {_timestampColumn} > @lastTimestamp ORDER BY {_timestampColumn} LIMIT {_batchMaxRows}";
                break;
            case "timestamp+incrementing":
                query += $" WHERE {_timestampColumn} > @lastTimestamp OR ({_timestampColumn} = @lastTimestamp AND {_incrementingColumn} > @lastValue) ORDER BY {_timestampColumn}, {_incrementingColumn} LIMIT {_batchMaxRows}";
                break;
            default: // bulk
                query += $" LIMIT {_batchMaxRows}";
                break;
        }

        return query;
    }
}

/// <summary>
/// A sink connector that writes records to a database.
/// </summary>
public sealed class DatabaseSinkConnector : SinkConnector
{
    private const string ConnectionStringConfig = "connection.string";
    private const string ProviderConfig = "db.provider";
    private const string TopicsConfig = "topics";
    private const string TableNameFormatConfig = "table.name.format";
    private const string InsertModeConfig = "insert.mode";
    private const string PkModeConfig = "pk.mode";
    private const string PkFieldsConfig = "pk.fields";
    private const string BatchSizeConfig = "batch.size";
    private const string AutoCreateConfig = "auto.create";
    private const string AutoEvolveConfig = "auto.evolve";

    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(DatabaseSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(ConnectionStringConfig, ConfigType.String, Importance.High, "Database connection string")
        .Define(ProviderConfig, ConfigType.String, "SqlClient", Importance.High, "Database provider (SqlClient, Npgsql, MySql, Sqlite)")
        .Define(TopicsConfig, ConfigType.String, Importance.High, "Topics to consume from")
        .Define(TableNameFormatConfig, ConfigType.String, "${topic}", Importance.Medium, "Table name format (use ${topic} for topic name)")
        .Define(InsertModeConfig, ConfigType.String, "insert", Importance.Medium, "Insert mode: insert, upsert, update")
        .Define(PkModeConfig, ConfigType.String, "none", Importance.Medium, "Primary key mode: none, kafka, record_key, record_value")
        .Define(PkFieldsConfig, ConfigType.String, "", Importance.Medium, "Comma-separated primary key fields")
        .Define(BatchSizeConfig, ConfigType.Int, 100, Importance.Medium, "Batch size for bulk operations")
        .Define(AutoCreateConfig, ConfigType.Boolean, false, Importance.Medium, "Auto-create tables")
        .Define(AutoEvolveConfig, ConfigType.Boolean, false, Importance.Medium, "Auto-evolve table schema");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(ConnectionStringConfig, out var _))
        {
            throw new ArgumentException($"Missing required config: {ConnectionStringConfig}");
        }
        if (!config.TryGetValue(TopicsConfig, out var _))
        {
            throw new ArgumentException($"Missing required config: {TopicsConfig}");
        }

        foreach (var kvp in config)
        {
            _config[kvp.Key] = kvp.Value;
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}

/// <summary>
/// Task that writes records to a database.
/// </summary>
public sealed class DatabaseSinkTask : SinkTask
{
    private const string ConnectionStringConfig = "connection.string";
    private const string ProviderConfig = "db.provider";
    private const string TableNameFormatConfig = "table.name.format";
    private const string InsertModeConfig = "insert.mode";
    private const string BatchSizeConfig = "batch.size";

    public override string Version => "1.0.0";

    private string _connectionString = "";
    private string _provider = "SqlClient";
    private string _tableNameFormat = "${topic}";
    private string _insertMode = "insert";
    private int _batchSize = 100;
    private DbConnection? _connection;
    private readonly List<SinkRecord> _buffer = new();

    public override void Start(IDictionary<string, string> config)
    {
        _connectionString = config[ConnectionStringConfig];
        _provider = config.GetOrDefault(ProviderConfig, "SqlClient");
        _tableNameFormat = config.GetOrDefault(TableNameFormatConfig, "${topic}");
        _insertMode = config.GetOrDefault(InsertModeConfig, "insert");
        _batchSize = int.Parse(config.GetOrDefault(BatchSizeConfig, "100"));

        // Create database connection using DbProviderFactories
        _connection = CreateConnection(_provider, _connectionString);
        _connection.Open();
    }

    private static DbConnection CreateConnection(string provider, string connectionString)
    {
        try
        {
            var factory = DbProviderFactories.GetFactory(provider switch
            {
                "SqlClient" => "Microsoft.Data.SqlClient",
                "Npgsql" => "Npgsql",
                "MySql" => "MySql.Data.MySqlClient",
                "Sqlite" => "Microsoft.Data.Sqlite",
                _ => provider
            });
            var connection = factory.CreateConnection()
                ?? throw new InvalidOperationException($"Failed to create connection for provider: {provider}");
            connection.ConnectionString = connectionString;
            return connection;
        }
        catch (ArgumentException)
        {
            throw new InvalidOperationException(
                $"Database provider '{provider}' is not registered. " +
                $"Ensure the appropriate NuGet package is installed and DbProviderFactories.RegisterFactory() is called.");
        }
    }

    public override void Stop()
    {
        _connection?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _connection?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        _buffer.AddRange(records);

        if (_buffer.Count >= _batchSize)
        {
            FlushBuffer();
        }

        return Task.CompletedTask;
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        FlushBuffer();
        return Task.CompletedTask;
    }

    private void FlushBuffer()
    {
        if (_buffer.Count == 0 || _connection == null || _connection.State != ConnectionState.Open)
            return;

        // Group records by topic/table
        var groupedRecords = _buffer.GroupBy(r => GetTableName(r.Topic));

        using var transaction = _connection.BeginTransaction();
        try
        {
            foreach (var group in groupedRecords)
            {
                var tableName = group.Key;
                var records = group.ToList();

                foreach (var record in records)
                {
                    if (record.Value == null) continue;

                    // Parse JSON value to extract columns
                    var jsonDoc = JsonDocument.Parse(record.Value);
                    var columns = new List<string>();
                    var values = new List<object?>();

                    foreach (var property in jsonDoc.RootElement.EnumerateObject())
                    {
                        columns.Add(property.Name);
                        values.Add(GetJsonValue(property.Value));
                    }

                    if (columns.Count == 0) continue;

                    using var command = _connection.CreateCommand();
                    command.Transaction = transaction;

#pragma warning disable CA2100 // Table/column names from trusted configuration, not user input
                    switch (_insertMode)
                    {
                        case "upsert":
                            command.CommandText = BuildUpsertStatement(tableName, columns);
                            break;
                        case "update":
                            command.CommandText = BuildUpdateStatement(tableName, columns);
                            break;
                        default: // insert
                            command.CommandText = BuildInsertStatement(tableName, columns);
                            break;
                    }
#pragma warning restore CA2100

                    // Add parameters
                    for (int i = 0; i < columns.Count; i++)
                    {
                        var param = command.CreateParameter();
                        param.ParameterName = $"@p{i}";
                        param.Value = values[i] ?? DBNull.Value;
                        command.Parameters.Add(param);
                    }

                    command.ExecuteNonQuery();
                }
            }

            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _buffer.Clear();
        }
    }

    private static object? GetJsonValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number when element.TryGetInt64(out var l) => l,
            JsonValueKind.Number => element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => element.GetRawText()
        };
    }

    private static string BuildInsertStatement(string tableName, List<string> columns)
    {
        var columnList = string.Join(", ", columns.Select(c => $"\"{c}\""));
        var paramList = string.Join(", ", columns.Select((_, i) => $"@p{i}"));
        return $"INSERT INTO \"{tableName}\" ({columnList}) VALUES ({paramList})";
    }

    private static string BuildUpsertStatement(string tableName, List<string> columns)
    {
        // Generic upsert using INSERT OR REPLACE (SQLite-compatible)
        // For other databases, this would need to be database-specific
        var columnList = string.Join(", ", columns.Select(c => $"\"{c}\""));
        var paramList = string.Join(", ", columns.Select((_, i) => $"@p{i}"));
        return $"INSERT OR REPLACE INTO \"{tableName}\" ({columnList}) VALUES ({paramList})";
    }

    private static string BuildUpdateStatement(string tableName, List<string> columns)
    {
        if (columns.Count < 2)
            return BuildInsertStatement(tableName, columns);

        // Assume first column is the key for WHERE clause
        var setClause = string.Join(", ", columns.Skip(1).Select((c, i) => $"\"{c}\" = @p{i + 1}"));
        return $"UPDATE \"{tableName}\" SET {setClause} WHERE \"{columns[0]}\" = @p0";
    }

    private string GetTableName(string topic)
    {
        return _tableNameFormat.Replace("${topic}", topic);
    }
}
