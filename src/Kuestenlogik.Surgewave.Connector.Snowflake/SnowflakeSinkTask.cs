using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Snowflake.Data.Client;

namespace Kuestenlogik.Surgewave.Connector.Snowflake;

/// <summary>
/// Task that writes records to Snowflake tables.
/// Supports INSERT, UPSERT (MERGE with INSERT/UPDATE), and MERGE operations.
/// Uses batch inserts for high throughput.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop()")]
[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Table/column names from configuration, not user input")]
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "SnowflakeDbConnection interface used")]
public sealed class SnowflakeSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private SnowflakeDbConnection? _connection;
    private string _database = "";
    private string _schema = "PUBLIC";
    private string _table = "";
    private string _writeMode = SnowflakeConnectorConfig.DefaultWriteMode;
    private int _batchSize = SnowflakeConnectorConfig.DefaultBatchSize;
    private string[] _keyColumns = [];
    private int _maxRetryCount = SnowflakeConnectorConfig.DefaultMaxRetryCount;
    private long _retryDelayMs = SnowflakeConnectorConfig.DefaultRetryDelayMs;
    private bool _autoCreateTable;
    private bool _tableVerified;
    private HashSet<string>? _tableColumns;

    public override void Start(IDictionary<string, string> config)
    {
        _database = config[SnowflakeConnectorConfig.DatabaseConfig];
        _schema = GetConfigValue(config, SnowflakeConnectorConfig.SchemaConfig, "PUBLIC");
        _table = config[SnowflakeConnectorConfig.TableConfig];
        _writeMode = GetConfigValue(config, SnowflakeConnectorConfig.WriteModeConfig, SnowflakeConnectorConfig.DefaultWriteMode);
        _batchSize = int.Parse(GetConfigValue(config, SnowflakeConnectorConfig.BatchSizeConfig, SnowflakeConnectorConfig.DefaultBatchSize.ToString()));
        _maxRetryCount = int.Parse(GetConfigValue(config, SnowflakeConnectorConfig.MaxRetryCountConfig, SnowflakeConnectorConfig.DefaultMaxRetryCount.ToString()));
        _retryDelayMs = long.Parse(GetConfigValue(config, SnowflakeConnectorConfig.RetryDelayMsConfig, SnowflakeConnectorConfig.DefaultRetryDelayMs.ToString()));
        _autoCreateTable = bool.Parse(GetConfigValue(config, SnowflakeConnectorConfig.AutoCreateTableConfig, "false"));

        var keyColumnsStr = GetConfigValue(config, SnowflakeConnectorConfig.KeyColumnsConfig, "");
        _keyColumns = string.IsNullOrEmpty(keyColumnsStr)
            ? []
            : keyColumnsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

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

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_connection == null || _connection.State != ConnectionState.Open || records.Count == 0)
            return;

        // Verify/create table on first batch
        if (!_tableVerified)
        {
            await EnsureTableExistsAsync(records, cancellationToken);
            _tableVerified = true;
        }

        // Process in batches
        var batches = records.Chunk(_batchSize);

        foreach (var batch in batches)
        {
            await ProcessBatchWithRetryAsync(batch, cancellationToken);
        }
    }

    private async Task ProcessBatchWithRetryAsync(SinkRecord[] records, CancellationToken cancellationToken)
    {
        var retryCount = 0;

        while (retryCount <= _maxRetryCount)
        {
            try
            {
                await ProcessBatchAsync(records, cancellationToken);
                return;
            }
            catch (SnowflakeDbException ex) when (IsRetriableException(ex) && retryCount < _maxRetryCount)
            {
                retryCount++;
                var delay = _retryDelayMs * Math.Pow(2, retryCount - 1);
                await Task.Delay(TimeSpan.FromMilliseconds(delay), cancellationToken);
            }
        }
    }

    private static bool IsRetriableException(SnowflakeDbException ex)
    {
        // Retry on transient errors (network issues, temporary failures)
        var errorCode = ex.ErrorCode;
        return errorCode == 390100 || // Authentication expired
               errorCode == 390144 || // Session invalid
               errorCode == 390154 || // Warehouse suspended
               errorCode >= 500000;   // Server errors
    }

    private async Task ProcessBatchAsync(SinkRecord[] records, CancellationToken cancellationToken)
    {
        var validRecords = new List<(Dictionary<string, object?> Data, bool IsDelete)>();

        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0)
            {
                // Tombstone - mark for delete if we can extract key
                if (record.Key != null && record.Key.Length > 0)
                {
                    try
                    {
                        var keyData = JsonSerializer.Deserialize<Dictionary<string, object?>>(record.Key);
                        if (keyData != null)
                        {
                            validRecords.Add((keyData, true));
                        }
                    }
                    catch (JsonException)
                    {
                        // Invalid key JSON, skip
                    }
                }
                continue;
            }

            try
            {
                var data = JsonSerializer.Deserialize<Dictionary<string, object?>>(record.Value);
                if (data != null)
                {
                    validRecords.Add((data, false));
                }
            }
            catch (JsonException)
            {
                // Invalid JSON, skip this record
            }
        }

        if (validRecords.Count == 0)
            return;

        // Separate inserts/updates and deletes
        var insertOrUpdate = validRecords.Where(r => !r.IsDelete).Select(r => r.Data).ToList();
        var deletes = validRecords.Where(r => r.IsDelete).Select(r => r.Data).ToList();

        // Process inserts/updates
        if (insertOrUpdate.Count > 0)
        {
            await WriteRecordsAsync(insertOrUpdate, cancellationToken);
        }

        // Process deletes
        if (deletes.Count > 0 && _keyColumns.Length > 0)
        {
            await DeleteRecordsAsync(deletes, cancellationToken);
        }
    }

    private async Task WriteRecordsAsync(List<Dictionary<string, object?>> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0)
            return;

        // Get all columns from all records
        var allColumns = records
            .SelectMany(r => r.Keys)
            .Where(c => _tableColumns?.Contains(c.ToUpperInvariant()) ?? true)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        switch (_writeMode.ToLowerInvariant())
        {
            case "upsert":
            case "merge":
                await MergeRecordsAsync(records, allColumns, cancellationToken);
                break;
            default: // insert
                await InsertRecordsAsync(records, allColumns, cancellationToken);
                break;
        }
    }

    private async Task InsertRecordsAsync(List<Dictionary<string, object?>> records, List<string> columns, CancellationToken cancellationToken)
    {
        var columnList = string.Join(", ", columns.Select(QuoteIdentifier));
        var valuePlaceholders = string.Join(", ", columns.Select((_, i) => $"?"));

        // Use multi-row INSERT VALUES
        var sql = new StringBuilder();
        sql.AppendLine($"INSERT INTO {QuoteIdentifier(_table)} ({columnList}) VALUES");

        var parameters = new List<object?>();
        var valueRows = new List<string>();

        foreach (var record in records)
        {
            var rowValues = new List<string>();
            foreach (var column in columns)
            {
                record.TryGetValue(column, out var value);
                parameters.Add(ConvertToSqlValue(value));
                rowValues.Add("?");
            }
            valueRows.Add($"({string.Join(", ", rowValues)})");
        }

        sql.AppendLine(string.Join(",\n", valueRows));

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = sql.ToString();

        for (var i = 0; i < parameters.Count; i++)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = $"p{i}";
            param.Value = parameters[i] ?? DBNull.Value;
            cmd.Parameters.Add(param);
        }

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task MergeRecordsAsync(List<Dictionary<string, object?>> records, List<string> columns, CancellationToken cancellationToken)
    {
        if (_keyColumns.Length == 0)
        {
            // Fall back to insert if no key columns
            await InsertRecordsAsync(records, columns, cancellationToken);
            return;
        }

        // Use MERGE statement
        foreach (var record in records)
        {
            var keyConditions = _keyColumns
                .Select(k => $"target.{QuoteIdentifier(k)} = source.{QuoteIdentifier(k)}")
                .ToList();

            var columnAssignments = columns
                .Where(c => !_keyColumns.Contains(c, StringComparer.OrdinalIgnoreCase))
                .Select(c => $"{QuoteIdentifier(c)} = source.{QuoteIdentifier(c)}")
                .ToList();

            var columnList = string.Join(", ", columns.Select(QuoteIdentifier));
            var sourceValues = string.Join(", ", columns.Select((c, i) => $"? AS {QuoteIdentifier(c)}"));

            var sql = $@"
MERGE INTO {QuoteIdentifier(_table)} AS target
USING (SELECT {sourceValues}) AS source
using Kuestenlogik.Surgewave.Connect;
ON {string.Join(" AND ", keyConditions)}
WHEN MATCHED THEN UPDATE SET {string.Join(", ", columnAssignments)}
WHEN NOT MATCHED THEN INSERT ({columnList}) VALUES ({string.Join(", ", columns.Select(c => $"source.{QuoteIdentifier(c)}"))})";

            using var cmd = _connection!.CreateCommand();
            cmd.CommandText = sql;

            for (var i = 0; i < columns.Count; i++)
            {
                record.TryGetValue(columns[i], out var value);
                var param = cmd.CreateParameter();
                param.ParameterName = $"p{i}";
                param.Value = ConvertToSqlValue(value) ?? DBNull.Value;
                cmd.Parameters.Add(param);
            }

            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task DeleteRecordsAsync(List<Dictionary<string, object?>> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var conditions = new List<string>();
            var parameters = new List<object?>();

            foreach (var keyColumn in _keyColumns)
            {
                if (record.TryGetValue(keyColumn, out var value))
                {
                    conditions.Add($"{QuoteIdentifier(keyColumn)} = ?");
                    parameters.Add(ConvertToSqlValue(value));
                }
            }

            if (conditions.Count == 0)
                continue;

            var sql = $"DELETE FROM {QuoteIdentifier(_table)} WHERE {string.Join(" AND ", conditions)}";

            using var cmd = _connection!.CreateCommand();
            cmd.CommandText = sql;

            for (var i = 0; i < parameters.Count; i++)
            {
                var param = cmd.CreateParameter();
                param.ParameterName = $"p{i}";
                param.Value = parameters[i] ?? DBNull.Value;
                cmd.Parameters.Add(param);
            }

            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static object? ConvertToSqlValue(object? value)
    {
        if (value == null)
            return DBNull.Value;

        if (value is JsonElement element)
        {
            return element.ValueKind switch
            {
                JsonValueKind.Null => DBNull.Value,
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Number when element.TryGetInt64(out var l) => l,
                JsonValueKind.Number when element.TryGetDouble(out var d) => d,
                JsonValueKind.String => element.GetString(),
                JsonValueKind.Array => element.GetRawText(),
                JsonValueKind.Object => element.GetRawText(),
                _ => element.GetRawText()
            };
        }

        return value;
    }

    private async Task EnsureTableExistsAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        // Get existing table columns
        _tableColumns = await GetTableColumnsAsync(cancellationToken);

        if (_tableColumns.Count > 0 || !_autoCreateTable)
            return;

        // Auto-create table from first record
        var firstRecord = records.FirstOrDefault(r => r.Value != null && r.Value.Length > 0);
        if (firstRecord?.Value == null)
            return;

        try
        {
            var data = JsonSerializer.Deserialize<Dictionary<string, object?>>(firstRecord.Value);
            if (data == null)
                return;

            var columns = new List<string>();
            foreach (var kvp in data)
            {
                var sqlType = InferSqlType(kvp.Value);
                columns.Add($"{QuoteIdentifier(kvp.Key)} {sqlType}");
            }

            var sql = $"CREATE TABLE IF NOT EXISTS {QuoteIdentifier(_table)} ({string.Join(", ", columns)})";
            using var cmd = _connection!.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(cancellationToken);

            // Refresh column list
            _tableColumns = await GetTableColumnsAsync(cancellationToken);
        }
        catch (JsonException)
        {
            // Invalid JSON, cannot create table
        }
    }

    private async Task<HashSet<string>> GetTableColumnsAsync(CancellationToken cancellationToken)
    {
        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        try
        {
            var sql = $"DESCRIBE TABLE {QuoteIdentifier(_table)}";
            using var cmd = _connection!.CreateCommand();
            cmd.CommandText = sql;

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var columnName = reader.GetString(0);
                columns.Add(columnName.ToUpperInvariant());
            }
        }
        catch
        {
            // Table might not exist
        }

        return columns;
    }

    private static string InferSqlType(object? value)
    {
        if (value == null)
            return "VARIANT";

        if (value is JsonElement element)
        {
            return element.ValueKind switch
            {
                JsonValueKind.True or JsonValueKind.False => "BOOLEAN",
                JsonValueKind.Number when element.TryGetInt64(out _) => "NUMBER",
                JsonValueKind.Number => "FLOAT",
                JsonValueKind.String => "VARCHAR",
                JsonValueKind.Array or JsonValueKind.Object => "VARIANT",
                _ => "VARIANT"
            };
        }

        return value switch
        {
            bool => "BOOLEAN",
            int or long => "NUMBER",
            float or double or decimal => "FLOAT",
            string => "VARCHAR",
            DateTime or DateTimeOffset => "TIMESTAMP_TZ",
            _ => "VARIANT"
        };
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // Snowflake writes are committed in PutAsync
        return Task.CompletedTask;
    }
}
