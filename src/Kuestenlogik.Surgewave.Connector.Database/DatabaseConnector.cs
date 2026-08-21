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
/// Resolves <see cref="DbProviderFactory"/> instances for the supported providers.
/// The .NET factory registry starts empty, so unregistered providers are resolved by
/// loading the well-known factory type from the provider assembly deployed with the
/// plugin, then registering it for subsequent lookups.
/// </summary>
internal static class DbConnectionFactory
{
    private static readonly Dictionary<string, (string InvariantName, string[] FactoryTypeNames)> KnownProviders =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["SqlClient"] = ("Microsoft.Data.SqlClient", ["Microsoft.Data.SqlClient.SqlClientFactory, Microsoft.Data.SqlClient"]),
            ["Npgsql"] = ("Npgsql", ["Npgsql.NpgsqlFactory, Npgsql"]),
            ["MySql"] = ("MySql.Data.MySqlClient",
            [
                "MySqlConnector.MySqlConnectorFactory, MySqlConnector",
                "MySql.Data.MySqlClient.MySqlClientFactory, MySql.Data"
            ]),
            ["Sqlite"] = ("Microsoft.Data.Sqlite", ["Microsoft.Data.Sqlite.SqliteFactory, Microsoft.Data.Sqlite"])
        };

    public static DbConnection Create(string provider, string connectionString)
    {
        var factory = ResolveFactory(provider);
        var connection = factory.CreateConnection()
            ?? throw new InvalidOperationException($"Failed to create connection for provider: {provider}");
        connection.ConnectionString = connectionString;
        return connection;
    }

    private static DbProviderFactory ResolveFactory(string provider)
    {
        var invariantName = KnownProviders.TryGetValue(provider, out var known) ? known.InvariantName : provider;
        if (DbProviderFactories.TryGetFactory(invariantName, out var registered))
        {
            return registered;
        }

        foreach (var typeName in known.FactoryTypeNames ?? [])
        {
            var factoryType = Type.GetType(typeName, throwOnError: false);
            var instance = factoryType?.GetField("Instance")?.GetValue(null)
                ?? factoryType?.GetProperty("Instance")?.GetValue(null);
            if (instance is DbProviderFactory factory)
            {
                DbProviderFactories.RegisterFactory(invariantName, factory);
                return factory;
            }
        }

        throw new InvalidOperationException(
            $"Database provider '{provider}' is not available. Deploy the provider assembly " +
            $"(e.g. {invariantName}) alongside the connector, or call " +
            $"DbProviderFactories.RegisterFactory(\"{invariantName}\", ...) before starting the connector.");
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
    private readonly List<TableQueryState> _tableStates = new();

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

        // One state (source partition + watermarks) per table, so every whitelisted
        // table is polled and tracked independently
        var connectionKey = _connectionString.GetHashCode().ToString();
        if (!string.IsNullOrEmpty(_query))
        {
            _tableStates.Add(CreateTableState(connectionKey, ""));
        }
        else
        {
            foreach (var table in _tableWhitelist.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                _tableStates.Add(CreateTableState(connectionKey, table));
            }
        }

        _connection = DbConnectionFactory.Create(_provider, _connectionString);
        _connection.Open();
    }

    private TableQueryState CreateTableState(string connectionKey, string table)
    {
        var sourcePartition = new Dictionary<string, object>
        {
            ["connection"] = connectionKey,
            ["query"] = string.IsNullOrEmpty(_query) ? table : _query
        };

        var state = new TableQueryState
        {
            SourcePartition = sourcePartition,
            Topic = BuildTopic(table),
            Table = table
        };

        // Restore offset
        var storedOffset = Context.OffsetStorageReader?.Offset(sourcePartition);
        if (storedOffset != null)
        {
            if (storedOffset.TryGetValue(LastOffsetField, out var lastOffset))
            {
                state.LastIncrementingValue = Convert.ToInt64(lastOffset);
            }
            if (storedOffset.TryGetValue(LastTimestampField, out var lastTs))
            {
                state.LastTimestampValue = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(lastTs));
            }
        }

        return state;
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

        var records = new List<SourceRecord>();

        foreach (var state in _tableStates)
        {
            await PollTableAsync(state, records, cancellationToken);
        }

        return records;
    }

    private async Task PollTableAsync(TableQueryState state, List<SourceRecord> records, CancellationToken cancellationToken)
    {
        var queryTemplate = BuildQuery(state.Table);
        if (string.IsNullOrEmpty(queryTemplate))
        {
            return;
        }

        await using var command = _connection!.CreateCommand();
#pragma warning disable CA2100 // Query comes from trusted configuration, not user input
        command.CommandText = queryTemplate;
#pragma warning restore CA2100

        // Add parameters based on mode
        if (_mode is "incrementing" or "timestamp+incrementing")
        {
            var param = command.CreateParameter();
            param.ParameterName = "@lastValue";
            param.Value = state.LastIncrementingValue;
            command.Parameters.Add(param);
        }

        if (_mode is "timestamp" or "timestamp+incrementing")
        {
            var param = command.CreateParameter();
            param.ParameterName = "@lastTimestamp";
            param.Value = state.LastTimestampValue.UtcDateTime;
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
                    state.LastIncrementingValue = Convert.ToInt64(value);
                }
                if (columnName.Equals(_timestampColumn, StringComparison.OrdinalIgnoreCase) && value != null)
                {
                    state.LastTimestampValue = value switch
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
                [LastOffsetField] = state.LastIncrementingValue,
                [LastTimestampField] = state.LastTimestampValue.ToUnixTimeMilliseconds()
            };

            records.Add(new SourceRecord
            {
                SourcePartition = state.SourcePartition,
                SourceOffset = sourceOffset,
                Topic = state.Topic,
                Key = keyJson,
                Value = valueJson
            });
        }
    }

    private string BuildTopic(string table)
    {
        if (!string.IsNullOrEmpty(_query))
        {
            return string.IsNullOrEmpty(_topicPrefix) ? "query_results" : $"{_topicPrefix}query_results";
        }

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

    private string BuildQuery(string table)
    {
        if (!string.IsNullOrEmpty(_query))
        {
            return _query;
        }

        if (string.IsNullOrEmpty(table))
        {
            return "";
        }

        // SQL Server has no LIMIT clause; every other supported provider does
        var top = _provider == "SqlClient" ? $"TOP ({_batchMaxRows}) " : "";
        var limit = _provider == "SqlClient" ? "" : $" LIMIT {_batchMaxRows}";
        var query = $"SELECT {top}* FROM {table}";

        switch (_mode)
        {
            case "incrementing":
                query += $" WHERE {_incrementingColumn} > @lastValue ORDER BY {_incrementingColumn}{limit}";
                break;
            case "timestamp":
                query += $" WHERE {_timestampColumn} > @lastTimestamp ORDER BY {_timestampColumn}{limit}";
                break;
            case "timestamp+incrementing":
                query += $" WHERE {_timestampColumn} > @lastTimestamp OR ({_timestampColumn} = @lastTimestamp AND {_incrementingColumn} > @lastValue) ORDER BY {_timestampColumn}, {_incrementingColumn}{limit}";
                break;
            default: // bulk
                query += limit;
                break;
        }

        return query;
    }

    private sealed class TableQueryState
    {
        public required Dictionary<string, object> SourcePartition { get; init; }
        public required string Topic { get; init; }
        public required string Table { get; init; }
        public long LastIncrementingValue { get; set; }
        public DateTimeOffset LastTimestampValue { get; set; } = DateTimeOffset.MinValue;
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
    private const string PkFieldsConfig = "pk.fields";
    private const string BatchSizeConfig = "batch.size";

    public override string Version => "1.0.0";

    private string _connectionString = "";
    private string _provider = "SqlClient";
    private string _tableNameFormat = "${topic}";
    private string _insertMode = "insert";
    private string[] _pkFields = [];
    private int _batchSize = 100;
    private DbConnection? _connection;
    private readonly List<SinkRecord> _buffer = new();

    public override void Start(IDictionary<string, string> config)
    {
        _connectionString = config[ConnectionStringConfig];
        _provider = config.GetOrDefault(ProviderConfig, "SqlClient");
        _tableNameFormat = config.GetOrDefault(TableNameFormatConfig, "${topic}");
        _insertMode = config.GetOrDefault(InsertModeConfig, "insert");
        _pkFields = config.GetOrDefault(PkFieldsConfig, "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        _batchSize = int.Parse(config.GetOrDefault(BatchSizeConfig, "100"));

        if (_insertMode == "update" && _pkFields.Length == 0)
        {
            throw new ArgumentException($"insert.mode 'update' requires {PkFieldsConfig}");
        }
        if (_insertMode == "upsert" && _pkFields.Length == 0 && _provider is "SqlClient" or "Npgsql")
        {
            throw new ArgumentException($"insert.mode 'upsert' with provider '{_provider}' requires {PkFieldsConfig}");
        }

        _connection = DbConnectionFactory.Create(_provider, _connectionString);
        _connection.Open();
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

                    var commandText = _insertMode switch
                    {
                        "upsert" => BuildUpsertStatement(tableName, columns),
                        "update" => BuildUpdateStatement(tableName, columns),
                        _ => BuildInsertStatement(tableName, columns)
                    };

                    if (commandText == null) continue;

                    using var command = _connection.CreateCommand();
                    command.Transaction = transaction;

#pragma warning disable CA2100 // Table/column names from trusted configuration, not user input
                    command.CommandText = commandText;
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

    private string BuildUpsertStatement(string tableName, List<string> columns)
    {
        var columnList = string.Join(", ", columns.Select(c => $"\"{c}\""));
        var paramList = string.Join(", ", columns.Select((_, i) => $"@p{i}"));
        var updateColumns = columns.Where(c => !IsKeyColumn(c)).ToList();

        switch (_provider)
        {
            case "Sqlite":
                return $"INSERT OR REPLACE INTO \"{tableName}\" ({columnList}) VALUES ({paramList})";

            case "MySql":
            {
                var setClause = updateColumns.Count > 0
                    ? string.Join(", ", updateColumns.Select(c => $"\"{c}\" = VALUES(\"{c}\")"))
                    : $"\"{columns[0]}\" = \"{columns[0]}\"";
                return $"INSERT INTO \"{tableName}\" ({columnList}) VALUES ({paramList}) ON DUPLICATE KEY UPDATE {setClause}";
            }

            case "Npgsql":
            {
                var conflictColumns = string.Join(", ", KeyColumns(tableName, columns).Select(c => $"\"{c}\""));
                var action = updateColumns.Count > 0
                    ? "DO UPDATE SET " + string.Join(", ", updateColumns.Select(c => $"\"{c}\" = EXCLUDED.\"{c}\""))
                    : "DO NOTHING";
                return $"INSERT INTO \"{tableName}\" ({columnList}) VALUES ({paramList}) ON CONFLICT ({conflictColumns}) {action}";
            }

            case "SqlClient":
            {
                var keyColumns = KeyColumns(tableName, columns);
                var sourceSelect = string.Join(", ", columns.Select((c, i) => $"@p{i} AS \"{c}\""));
                var onClause = string.Join(" AND ", keyColumns.Select(c => $"target.\"{c}\" = source.\"{c}\""));
                var matchedClause = updateColumns.Count > 0
                    ? " WHEN MATCHED THEN UPDATE SET " + string.Join(", ", updateColumns.Select(c => $"target.\"{c}\" = source.\"{c}\""))
                    : "";
                var insertValues = string.Join(", ", columns.Select(c => $"source.\"{c}\""));
                return $"MERGE INTO \"{tableName}\" AS target USING (SELECT {sourceSelect}) AS source ON {onClause}{matchedClause} WHEN NOT MATCHED THEN INSERT ({columnList}) VALUES ({insertValues});";
            }

            default:
                throw new NotSupportedException($"insert.mode 'upsert' is not supported for provider '{_provider}'.");
        }
    }

    private string? BuildUpdateStatement(string tableName, List<string> columns)
    {
        var setParts = new List<string>();
        var whereParts = new List<string>();
        for (int i = 0; i < columns.Count; i++)
        {
            if (IsKeyColumn(columns[i]))
            {
                whereParts.Add($"\"{columns[i]}\" = @p{i}");
            }
            else
            {
                setParts.Add($"\"{columns[i]}\" = @p{i}");
            }
        }

        if (whereParts.Count != _pkFields.Length)
        {
            throw new InvalidOperationException(
                $"Record for table '{tableName}' is missing primary key field(s): {MissingKeyFields(columns)}");
        }

        if (setParts.Count == 0)
        {
            // Key-only record: nothing to update
            return null;
        }

        return $"UPDATE \"{tableName}\" SET {string.Join(", ", setParts)} WHERE {string.Join(" AND ", whereParts)}";
    }

    private List<string> KeyColumns(string tableName, List<string> columns)
    {
        var keyColumns = columns.Where(IsKeyColumn).ToList();
        if (keyColumns.Count != _pkFields.Length)
        {
            throw new InvalidOperationException(
                $"Record for table '{tableName}' is missing primary key field(s): {MissingKeyFields(columns)}");
        }
        return keyColumns;
    }

    private bool IsKeyColumn(string column)
        => _pkFields.Contains(column, StringComparer.OrdinalIgnoreCase);

    private string MissingKeyFields(List<string> columns)
        => string.Join(", ", _pkFields.Where(f => !columns.Contains(f, StringComparer.OrdinalIgnoreCase)));

    private string GetTableName(string topic)
    {
        return _tableNameFormat.Replace("${topic}", topic);
    }
}
