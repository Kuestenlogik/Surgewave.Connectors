using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Snowflake.Data.Client;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Snowflake;

/// <summary>
/// Task that captures data from Snowflake tables, queries, or CDC streams.
/// Supports table polling, custom queries, and Snowflake Streams for real-time change data capture.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop()")]
[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Table/column names from configuration, not user input")]
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "SnowflakeDbConnection interface used")]
public sealed class SnowflakeSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private SnowflakeDbConnection? _connection;
    private string _account = "";
    private string _database = "";
    private string _schema = "PUBLIC";
    private string _table = "";
    private string _query = "";
    private string _streamName = "";
    private string _mode = SnowflakeConnectorConfig.DefaultMode;
    private string _topicPattern = SnowflakeConnectorConfig.DefaultTopicPattern;
    private long _pollIntervalMs = SnowflakeConnectorConfig.DefaultPollIntervalMs;
    private int _maxRowsPerPoll = SnowflakeConnectorConfig.DefaultMaxRowsPerPoll;
    private bool _includeMetadata = true;
    private string? _timestampColumn;
    private string? _incrementingColumn;

    private readonly Dictionary<string, object> _sourcePartition = new();
    private object? _lastTimestamp;
    private object? _lastIncrementingValue;
    private bool _streamCreated;

    public override void Start(IDictionary<string, string> config)
    {
        _account = config[SnowflakeConnectorConfig.AccountConfig];
        _database = config[SnowflakeConnectorConfig.DatabaseConfig];
        _schema = GetConfigValue(config, SnowflakeConnectorConfig.SchemaConfig, "PUBLIC");
        _table = GetConfigValue(config, SnowflakeConnectorConfig.TableConfig, "");
        _query = GetConfigValue(config, SnowflakeConnectorConfig.QueryConfig, "");
        _streamName = GetConfigValue(config, SnowflakeConnectorConfig.StreamNameConfig, "");
        _mode = GetConfigValue(config, SnowflakeConnectorConfig.ModeConfig, SnowflakeConnectorConfig.DefaultMode);
        _topicPattern = GetConfigValue(config, SnowflakeConnectorConfig.TopicPatternConfig, SnowflakeConnectorConfig.DefaultTopicPattern);
        _pollIntervalMs = long.Parse(GetConfigValue(config, SnowflakeConnectorConfig.PollIntervalMsConfig, SnowflakeConnectorConfig.DefaultPollIntervalMs.ToString()));
        _maxRowsPerPoll = int.Parse(GetConfigValue(config, SnowflakeConnectorConfig.MaxRowsPerPollConfig, SnowflakeConnectorConfig.DefaultMaxRowsPerPoll.ToString()));
        _includeMetadata = bool.Parse(GetConfigValue(config, SnowflakeConnectorConfig.IncludeMetadataConfig, "true"));
        _timestampColumn = GetConfigValue(config, SnowflakeConnectorConfig.TimestampColumnConfig, "");
        _incrementingColumn = GetConfigValue(config, SnowflakeConnectorConfig.IncrementingColumnConfig, "");

        if (string.IsNullOrEmpty(_timestampColumn)) _timestampColumn = null;
        if (string.IsNullOrEmpty(_incrementingColumn)) _incrementingColumn = null;

        _sourcePartition["account"] = _account;
        _sourcePartition["database"] = _database;
        _sourcePartition["schema"] = _schema;
        _sourcePartition["table"] = _table;
        _sourcePartition["mode"] = _mode;

        // Build connection string
        var connectionString = BuildConnectionString(config);
        _connection = new SnowflakeDbConnection(connectionString);
        _connection.Open();

        // Set warehouse if specified
        var warehouse = GetConfigValue(config, SnowflakeConnectorConfig.WarehouseConfig, "");
        if (!string.IsNullOrEmpty(warehouse))
        {
            ExecuteNonQuery($"USE WAREHOUSE {QuoteIdentifier(warehouse)}");
        }

        // Set database and schema
        ExecuteNonQuery($"USE DATABASE {QuoteIdentifier(_database)}");
        ExecuteNonQuery($"USE SCHEMA {QuoteIdentifier(_schema)}");

        RestoreOffset();

        // Create stream if in stream mode and stream name provided
        if (_mode.Equals("stream", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrEmpty(_table))
        {
            EnsureStreamExists();
        }
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static string BuildConnectionString(IDictionary<string, string> config)
    {
        var account = config[SnowflakeConnectorConfig.AccountConfig];
        var user = config[SnowflakeConnectorConfig.UserConfig];
        var password = GetConfigValue(config, SnowflakeConnectorConfig.PasswordConfig, "");
        var database = config[SnowflakeConnectorConfig.DatabaseConfig];
        var schema = GetConfigValue(config, SnowflakeConnectorConfig.SchemaConfig, "PUBLIC");
        var warehouse = GetConfigValue(config, SnowflakeConnectorConfig.WarehouseConfig, "");
        var role = GetConfigValue(config, SnowflakeConnectorConfig.RoleConfig, "");
        var authenticator = GetConfigValue(config, SnowflakeConnectorConfig.AuthenticatorConfig, "snowflake");

        var sb = new StringBuilder();
        sb.Append($"account={account};user={user};db={database};schema={schema}");

        if (!string.IsNullOrEmpty(password))
            sb.Append($";password={password}");

        if (!string.IsNullOrEmpty(warehouse))
            sb.Append($";warehouse={warehouse}");

        if (!string.IsNullOrEmpty(role))
            sb.Append($";role={role}");

        sb.Append($";authenticator={authenticator}");

        // Handle key-pair authentication
        var privateKeyFile = GetConfigValue(config, SnowflakeConnectorConfig.PrivateKeyFileConfig, "");
        if (!string.IsNullOrEmpty(privateKeyFile))
        {
            sb.Append($";private_key_file={privateKeyFile}");

            var passphrase = GetConfigValue(config, SnowflakeConnectorConfig.PrivateKeyPassphraseConfig, "");
            if (!string.IsNullOrEmpty(passphrase))
                sb.Append($";private_key_pwd={passphrase}");
        }

        // Handle OAuth
        var oauthToken = GetConfigValue(config, SnowflakeConnectorConfig.OAuthTokenConfig, "");
        if (!string.IsNullOrEmpty(oauthToken))
            sb.Append($";token={oauthToken}");

        return sb.ToString();
    }

    private void RestoreOffset()
    {
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return;

        if (storedOffset.TryGetValue(SnowflakeConnectorConfig.OffsetTimestamp, out var ts) && ts != null)
        {
            if (DateTime.TryParse(ts.ToString(), out var timestamp))
                _lastTimestamp = timestamp;
        }

        if (storedOffset.TryGetValue(SnowflakeConnectorConfig.OffsetIncrementingColumn, out var inc) && inc != null)
        {
            _lastIncrementingValue = inc;
        }
    }

    private void EnsureStreamExists()
    {
        if (_streamCreated)
            return;

        var streamName = string.IsNullOrEmpty(_streamName)
            ? $"{_table}_Surgewave_STREAM"
            : _streamName;

        _streamName = streamName;

        try
        {
            // Try to create stream if it doesn't exist
            var sql = $@"CREATE STREAM IF NOT EXISTS {QuoteIdentifier(streamName)}
                        ON TABLE {QuoteIdentifier(_table)}
                        SHOW_INITIAL_ROWS = FALSE";
            ExecuteNonQuery(sql);
            _streamCreated = true;
        }
        catch
        {
            // Stream might already exist, continue
            _streamCreated = true;
        }
    }

    private void ExecuteNonQuery(string sql)
    {
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    public override void Stop()
    {
        _connection?.Close();
        _connection?.Dispose();
        _connection = null;
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
        if (_connection == null || _connection.State != ConnectionState.Open)
            return [];

        var records = new List<SourceRecord>();

        try
        {
            var sql = BuildQuery();

            using var cmd = _connection.CreateCommand();
            cmd.CommandText = sql;

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var columnNames = GetColumnNames(reader);

            while (await reader.ReadAsync(cancellationToken) && records.Count < _maxRowsPerPoll)
            {
                var record = ConvertToSourceRecord(reader, columnNames);
                records.Add(record);

                // Update tracking columns
                UpdateTrackingValues(reader, columnNames);
            }

            // If stream mode, consume the stream data
            if (_mode.Equals("stream", StringComparison.OrdinalIgnoreCase) && records.Count > 0)
            {
                // Stream data is automatically consumed when read
            }

            if (records.Count == 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs), cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Cancelled, return what we have
        }

        return records;
    }

    private string BuildQuery()
    {
        return _mode.ToLowerInvariant() switch
        {
            "stream" => BuildStreamQuery(),
            "query" => BuildCustomQuery(),
            _ => BuildTableQuery()
        };
    }

    private string BuildStreamQuery()
    {
        return $"SELECT * FROM {QuoteIdentifier(_streamName)} LIMIT {_maxRowsPerPoll}";
    }

    private string BuildCustomQuery()
    {
        var sql = _query;

        // Add incremental conditions if configured
        var conditions = new List<string>();

        if (_timestampColumn != null && _lastTimestamp != null)
        {
            conditions.Add($"{QuoteIdentifier(_timestampColumn)} > '{_lastTimestamp:yyyy-MM-dd HH:mm:ss.fff}'");
        }

        if (_incrementingColumn != null && _lastIncrementingValue != null)
        {
            conditions.Add($"{QuoteIdentifier(_incrementingColumn)} > {_lastIncrementingValue}");
        }

        // Wrap query if conditions exist
        if (conditions.Count > 0)
        {
            var whereClause = string.Join(" AND ", conditions);
            sql = $"SELECT * FROM ({_query}) WHERE {whereClause}";
        }

        return $"{sql} LIMIT {_maxRowsPerPoll}";
    }

    private string BuildTableQuery()
    {
        var sql = $"SELECT * FROM {QuoteIdentifier(_table)}";

        // Add incremental conditions
        var conditions = new List<string>();

        if (_timestampColumn != null && _lastTimestamp != null)
        {
            conditions.Add($"{QuoteIdentifier(_timestampColumn)} > '{_lastTimestamp:yyyy-MM-dd HH:mm:ss.fff}'");
        }

        if (_incrementingColumn != null && _lastIncrementingValue != null)
        {
            conditions.Add($"{QuoteIdentifier(_incrementingColumn)} > {_lastIncrementingValue}");
        }

        if (conditions.Count > 0)
        {
            sql += " WHERE " + string.Join(" AND ", conditions);
        }

        // Add ordering for incremental tracking
        var orderColumns = new List<string>();
        if (_timestampColumn != null)
            orderColumns.Add(QuoteIdentifier(_timestampColumn));
        if (_incrementingColumn != null)
            orderColumns.Add(QuoteIdentifier(_incrementingColumn));

        if (orderColumns.Count > 0)
        {
            sql += " ORDER BY " + string.Join(", ", orderColumns);
        }

        return $"{sql} LIMIT {_maxRowsPerPoll}";
    }

    private static List<string> GetColumnNames(IDataReader reader)
    {
        var columns = new List<string>();
        for (var i = 0; i < reader.FieldCount; i++)
        {
            columns.Add(reader.GetName(i));
        }
        return columns;
    }

    private void UpdateTrackingValues(IDataReader reader, List<string> columnNames)
    {
        if (_timestampColumn != null)
        {
            var idx = columnNames.FindIndex(c => c.Equals(_timestampColumn, StringComparison.OrdinalIgnoreCase));
            if (idx >= 0 && !reader.IsDBNull(idx))
            {
                _lastTimestamp = reader.GetValue(idx);
            }
        }

        if (_incrementingColumn != null)
        {
            var idx = columnNames.FindIndex(c => c.Equals(_incrementingColumn, StringComparison.OrdinalIgnoreCase));
            if (idx >= 0 && !reader.IsDBNull(idx))
            {
                _lastIncrementingValue = reader.GetValue(idx);
            }
        }
    }

    private SourceRecord ConvertToSourceRecord(IDataReader reader, List<string> columnNames)
    {
        var data = new Dictionary<string, object?>();
        string? actionType = null;
        bool? isUpdate = null;
        string? rowId = null;

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var columnName = columnNames[i];
            var value = reader.IsDBNull(i) ? null : ConvertValue(reader.GetValue(i));

            // Extract stream metadata
            if (columnName.Equals(SnowflakeConnectorConfig.MetadataAction, StringComparison.OrdinalIgnoreCase))
            {
                actionType = value?.ToString();
                continue;
            }
            if (columnName.Equals(SnowflakeConnectorConfig.MetadataIsUpdate, StringComparison.OrdinalIgnoreCase))
            {
                isUpdate = value is bool b ? b : value?.ToString()?.Equals("TRUE", StringComparison.OrdinalIgnoreCase);
                continue;
            }
            if (columnName.Equals(SnowflakeConnectorConfig.MetadataRowId, StringComparison.OrdinalIgnoreCase))
            {
                rowId = value?.ToString();
                continue;
            }

            data[columnName] = value;
        }

        var timestamp = DateTimeOffset.UtcNow;

        // Determine operation type
        var op = DetermineOperationType(actionType, isUpdate);

        // Build key from key columns or row ID
        var key = BuildKey(data, rowId);

        // Build payload
        Dictionary<string, object?> payload;
        if (_includeMetadata)
        {
            payload = new Dictionary<string, object?>
            {
                ["op"] = op,
                ["source"] = new Dictionary<string, object>
                {
                    ["account"] = _account,
                    ["database"] = _database,
                    ["schema"] = _schema,
                    ["table"] = _table,
                    ["mode"] = _mode,
                    ["stream_name"] = _streamName ?? "",
                    ["timestamp"] = timestamp.ToString("O")
                },
                ["ts_ms"] = timestamp.ToUnixTimeMilliseconds()
            };

            if (op == "d")
            {
                payload["before"] = data;
            }
            else
            {
                payload["after"] = data;
            }
        }
        else
        {
            payload = new Dictionary<string, object?>
            {
                ["data"] = data
            };
        }

        // Build offset
        var offset = new Dictionary<string, object>
        {
            [SnowflakeConnectorConfig.OffsetTable] = _table
        };

        if (_lastTimestamp != null)
            offset[SnowflakeConnectorConfig.OffsetTimestamp] = _lastTimestamp.ToString()!;
        if (_lastIncrementingValue != null)
            offset[SnowflakeConnectorConfig.OffsetIncrementingColumn] = _lastIncrementingValue;
        if (rowId != null)
            offset[SnowflakeConnectorConfig.OffsetStreamPosition] = rowId;

        var headers = new Dictionary<string, byte[]>
        {
            [SnowflakeConnectorConfig.HeaderAccount] = Encoding.UTF8.GetBytes(_account),
            [SnowflakeConnectorConfig.HeaderDatabase] = Encoding.UTF8.GetBytes(_database),
            [SnowflakeConnectorConfig.HeaderSchema] = Encoding.UTF8.GetBytes(_schema),
            [SnowflakeConnectorConfig.HeaderTable] = Encoding.UTF8.GetBytes(_table),
            [SnowflakeConnectorConfig.HeaderTimestamp] = Encoding.UTF8.GetBytes(timestamp.ToString("O"))
        };

        if (!string.IsNullOrEmpty(_streamName))
            headers[SnowflakeConnectorConfig.HeaderStreamName] = Encoding.UTF8.GetBytes(_streamName);
        if (actionType != null)
            headers[SnowflakeConnectorConfig.HeaderActionType] = Encoding.UTF8.GetBytes(actionType);
        if (rowId != null)
            headers[SnowflakeConnectorConfig.HeaderRowId] = Encoding.UTF8.GetBytes(rowId);

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = offset,
            Topic = GetTopicName(),
            Key = JsonSerializer.SerializeToUtf8Bytes(key),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = timestamp,
            Headers = headers
        };
    }

    private static string DetermineOperationType(string? actionType, bool? isUpdate)
    {
        if (actionType == null)
            return "c"; // Default to create for table/query mode

        return actionType.ToUpperInvariant() switch
        {
            "INSERT" when isUpdate == true => "u", // Update (shown as INSERT with ISUPDATE=true)
            "INSERT" => "c", // Create
            "DELETE" => "d", // Delete
            _ => "c"
        };
    }

    private static Dictionary<string, object?> BuildKey(Dictionary<string, object?> data, string? rowId)
    {
        var key = new Dictionary<string, object?>();

        if (rowId != null)
        {
            key["row_id"] = rowId;
        }
        else if (data.TryGetValue("ID", out var id))
        {
            key["id"] = id;
        }
        else if (data.TryGetValue("id", out var idLower))
        {
            key["id"] = idLower;
        }
        else
        {
            // Use first column as key fallback
            var firstKey = data.Keys.FirstOrDefault();
            if (firstKey != null)
            {
                key[firstKey.ToLowerInvariant()] = data[firstKey];
            }
        }

        return key;
    }

    private static object? ConvertValue(object value)
    {
        return value switch
        {
            DBNull => null,
            DateTime dt => dt.ToString("O"),
            DateTimeOffset dto => dto.ToString("O"),
            byte[] bytes => Convert.ToBase64String(bytes),
            decimal d => d,
            _ => value
        };
    }

    private string GetTopicName()
    {
        return _topicPattern
            .Replace("${database}", _database)
            .Replace("${schema}", _schema)
            .Replace("${table}", _table);
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Position is tracked via offset storage automatically
        return Task.CompletedTask;
    }
}
