using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Oracle.ManagedDataAccess.Client;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Oracle;

/// <summary>
/// Task that captures changes from Oracle using LogMiner.
/// Reads redo logs for INSERT, UPDATE, and DELETE events.
/// Produces Debezium-compatible JSON output with operation types and row data.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class OracleCdcSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _connectionString = "";
    private string _host = OracleConnectorConfig.DefaultHost;
    private int _port = OracleConnectorConfig.DefaultPort;
    private string _serviceName = "";
    private string _sid = "";
    private string _username = "";
    private string _password = "";
    private string _walletLocation = "";
    private string[] _tables = [];
    private string _topicPrefix = "";
    private string _topicPattern = OracleConnectorConfig.DefaultTopicPattern;
    private bool _includeSchema = true;
    private bool _includeBeforeValues = true;
    private string _snapshotMode = OracleConnectorConfig.SnapshotModeInitial;
    private long _pollIntervalMs = OracleConnectorConfig.DefaultPollIntervalMs;
    private int _batchMaxRecords = OracleConnectorConfig.DefaultBatchMaxRecords;
    private string _logMinerMode = OracleConnectorConfig.LogMinerModeOnline;
    private string _dictionaryMode = OracleConnectorConfig.DictionaryModeOnline;
    private bool _startFromBeginning;

    private long? _lastScn;
    private bool _snapshotCompleted;
    private readonly Dictionary<string, List<string>> _tableColumns = new();
    private readonly HashSet<string> _tableOwners = new();
    private readonly Dictionary<string, object> _sourcePartition = new();

    // Regex patterns for parsing SQL_REDO
    private static readonly Regex InsertPattern = new(
        @"insert into ""([^""]+)"".""([^""]+)""\s*\(([^)]+)\)\s*values\s*\((.+)\);?",
        RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);

    private static readonly Regex UpdatePattern = new(
        @"update ""([^""]+)"".""([^""]+)""\s*set\s*(.+?)\s*where\s*(.+);?",
        RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);

    private static readonly Regex DeletePattern = new(
        @"delete from ""([^""]+)"".""([^""]+)""\s*where\s*(.+);?",
        RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);

    public override void Start(IDictionary<string, string> config)
    {
        // Build connection string
        if (config.TryGetValue(OracleConnectorConfig.ConnectionString, out var connStr) && !string.IsNullOrEmpty(connStr))
        {
            _connectionString = connStr;
        }
        else
        {
            _host = GetConfigValue(config, OracleConnectorConfig.Host, OracleConnectorConfig.DefaultHost);
            _port = int.Parse(GetConfigValue(config, OracleConnectorConfig.Port, OracleConnectorConfig.DefaultPort.ToString()));
            _serviceName = GetConfigValue(config, OracleConnectorConfig.ServiceName, "");
            _sid = GetConfigValue(config, OracleConnectorConfig.Sid, "");
            _username = GetConfigValue(config, OracleConnectorConfig.Username, "");
            _password = GetConfigValue(config, OracleConnectorConfig.Password, "");
            _walletLocation = GetConfigValue(config, OracleConnectorConfig.WalletLocation, "");

            _connectionString = BuildConnectionString();
        }

        _tables = config[OracleConnectorConfig.Tables]
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(t => t.Trim().ToUpperInvariant()) // Oracle uses uppercase identifiers
            .ToArray();

        // Extract owners for filtering
        foreach (var table in _tables)
        {
            var (owner, _) = ParseTableName(table);
            _tableOwners.Add(owner);
        }

        _topicPrefix = GetConfigValue(config, OracleConnectorConfig.TopicPrefix, "");
        _topicPattern = GetConfigValue(config, OracleConnectorConfig.TopicPattern, OracleConnectorConfig.DefaultTopicPattern);
        _includeSchema = bool.Parse(GetConfigValue(config, OracleConnectorConfig.IncludeSchema, "true"));
        _includeBeforeValues = bool.Parse(GetConfigValue(config, OracleConnectorConfig.IncludeBeforeValues, "true"));
        _snapshotMode = GetConfigValue(config, OracleConnectorConfig.SnapshotMode, OracleConnectorConfig.SnapshotModeInitial);
        _pollIntervalMs = long.Parse(GetConfigValue(config, OracleConnectorConfig.PollIntervalMs, OracleConnectorConfig.DefaultPollIntervalMs.ToString()));
        _batchMaxRecords = int.Parse(GetConfigValue(config, OracleConnectorConfig.BatchMaxRecords, OracleConnectorConfig.DefaultBatchMaxRecords.ToString()));
        _logMinerMode = GetConfigValue(config, OracleConnectorConfig.LogMinerMode, OracleConnectorConfig.LogMinerModeOnline);
        _dictionaryMode = GetConfigValue(config, OracleConnectorConfig.DictionaryMode, OracleConnectorConfig.DictionaryModeOnline);
        _startFromBeginning = bool.Parse(GetConfigValue(config, OracleConnectorConfig.StartFromBeginning, "false"));

        _sourcePartition["host"] = _host;
        _sourcePartition["service"] = !string.IsNullOrEmpty(_serviceName) ? _serviceName : _sid;

        RestoreOffset();
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    private void RestoreOffset()
    {
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return;

        if (storedOffset.TryGetValue(OracleConnectorConfig.OffsetScn, out var scnValue) && scnValue != null)
        {
            _lastScn = long.Parse(scnValue.ToString()!);
        }

        if (storedOffset.TryGetValue(OracleConnectorConfig.OffsetSnapshotCompleted, out var snapshotValue))
        {
            _snapshotCompleted = snapshotValue?.ToString() == "true";
        }
    }

    public override void Stop()
    {
        // No persistent connections to clean up
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
        // Initialize table metadata on first poll
        if (_tableColumns.Count == 0)
        {
            await InitializeTableMetadataAsync(cancellationToken);
        }

        // Check if we need to do initial snapshot
        if (_snapshotMode != OracleConnectorConfig.SnapshotModeNever && !_snapshotCompleted)
        {
            if (_snapshotMode != OracleConnectorConfig.SnapshotModeSchemaOnly)
            {
                var snapshotRecords = await PerformSnapshotAsync(cancellationToken);
                if (snapshotRecords.Count > 0)
                    return snapshotRecords;
            }
            _snapshotCompleted = true;
        }

        // Initialize SCN if not set
        if (_lastScn == null)
        {
            _lastScn = _startFromBeginning
                ? await GetMinScnAsync(cancellationToken)
                : await GetCurrentScnAsync(cancellationToken);
        }

        // Poll for LogMiner changes
        return await PollLogMinerChangesAsync(cancellationToken);
    }

    private async Task InitializeTableMetadataAsync(CancellationToken cancellationToken)
    {
        await using var conn = new OracleConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        foreach (var table in _tables)
        {
            var (owner, tableName) = ParseTableName(table);

            // Get column names for this table
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                SELECT column_name
                FROM all_tab_columns
                WHERE owner = :owner AND table_name = :table_name
                ORDER BY column_id";
            cmd.Parameters.Add(new OracleParameter("owner", owner));
            cmd.Parameters.Add(new OracleParameter("table_name", tableName));

            var columns = new List<string>();
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                columns.Add(reader.GetString(0));
            }

            if (columns.Count == 0)
            {
                throw new InvalidOperationException(
                    $"Table {owner}.{tableName} not found or not accessible. " +
                    "Ensure the table exists and the user has SELECT privilege.");
            }

            _tableColumns[$"{owner}.{tableName}"] = columns;
        }
    }

    private async Task<long> GetMinScnAsync(CancellationToken cancellationToken)
    {
        await using var conn = new OracleConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT MIN(first_change#) FROM v$archived_log WHERE deleted = 'NO'";

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        if (result != null && result != DBNull.Value)
        {
            return Convert.ToInt64(result);
        }

        // Fallback to current SCN if no archived logs
        return await GetCurrentScnAsync(cancellationToken);
    }

    private async Task<long> GetCurrentScnAsync(CancellationToken cancellationToken)
    {
        await using var conn = new OracleConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT current_scn FROM v$database";

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(result);
    }

    private async Task<List<SourceRecord>> PerformSnapshotAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        await using var conn = new OracleConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        foreach (var table in _tables)
        {
            var (owner, tableName) = ParseTableName(table);

            await using var cmd = conn.CreateCommand();
#pragma warning disable CA2100 // SQL injection - table names validated via all_tab_columns
            cmd.CommandText = $"SELECT * FROM \"{owner}\".\"{tableName}\"";
#pragma warning restore CA2100
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var columns = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                columns.Add(reader.GetName(i));
            }

            while (await reader.ReadAsync(cancellationToken))
            {
                var row = new Dictionary<string, object?>();
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    row[columns[i]] = reader.IsDBNull(i) ? null : ConvertValue(reader.GetValue(i));
                }

                var payload = new Dictionary<string, object?>
                {
                    ["op"] = "r", // read/snapshot
                    ["source"] = new Dictionary<string, object>
                    {
                        ["owner"] = owner,
                        ["table"] = tableName,
                        ["snapshot"] = true,
                        ["host"] = _host
                    },
                    ["before"] = null,
                    ["after"] = row,
                    ["ts_ms"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                records.Add(new SourceRecord
                {
                    SourcePartition = _sourcePartition,
                    SourceOffset = new Dictionary<string, object>
                    {
                        [OracleConnectorConfig.OffsetScn] = _lastScn?.ToString() ?? "0",
                        [OracleConnectorConfig.OffsetSnapshotCompleted] = "false"
                    },
                    Topic = GetTopicName(owner, tableName),
                    Key = GetPrimaryKeyBytes(row),
                    Value = JsonSerializer.SerializeToUtf8Bytes(payload),
                    Timestamp = DateTimeOffset.UtcNow,
                    Headers = new Dictionary<string, byte[]>
                    {
                        [OracleConnectorConfig.HeaderSchema] = Encoding.UTF8.GetBytes(owner),
                        [OracleConnectorConfig.HeaderTable] = Encoding.UTF8.GetBytes(tableName),
                        [OracleConnectorConfig.HeaderOperation] = Encoding.UTF8.GetBytes("r")
                    }
                });

                if (records.Count >= _batchMaxRecords)
                    return records;
            }
        }

        return records;
    }

    private async Task<List<SourceRecord>> PollLogMinerChangesAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        await using var conn = new OracleConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        // Get current SCN
        var currentScn = await GetCurrentScnAsync(cancellationToken);

        // Skip if no new changes
        if (_lastScn.HasValue && _lastScn.Value >= currentScn)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs), cancellationToken);
            return records;
        }

        try
        {
            // Start LogMiner session
            await StartLogMinerAsync(conn, _lastScn!.Value, currentScn, cancellationToken);

            // Query changes
            records = await QueryLogMinerContentsAsync(conn, cancellationToken);

            // End LogMiner session
            await EndLogMinerAsync(conn, cancellationToken);

            // Update last SCN
            if (records.Count > 0)
            {
                _lastScn = currentScn;
            }
        }
        catch (OracleException ex) when (ex.Number == 1291) // ORA-01291: missing log file
        {
            // Log gap detected, skip to current SCN
            _lastScn = currentScn;
        }

        return records;
    }

    private async Task StartLogMinerAsync(OracleConnection conn, long startScn, long endScn, CancellationToken cancellationToken)
    {
        await using var cmd = conn.CreateCommand();

        // Add online redo logs
        if (_logMinerMode == OracleConnectorConfig.LogMinerModeOnline)
        {
            cmd.CommandText = @"
                BEGIN
                    DBMS_LOGMNR.ADD_LOGFILE(
                        LOGFILENAME => (SELECT member FROM v$logfile WHERE rownum = 1),
                        OPTIONS => DBMS_LOGMNR.NEW
                    );
                    FOR rec IN (SELECT member FROM v$logfile WHERE rownum > 1) LOOP
                        DBMS_LOGMNR.ADD_LOGFILE(
                            LOGFILENAME => rec.member,
                            OPTIONS => DBMS_LOGMNR.ADDFILE
                        );
                    END LOOP;
                END;";
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        // Start LogMiner
        var dictOption = _dictionaryMode == OracleConnectorConfig.DictionaryModeOnline
            ? "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"
            : "DBMS_LOGMNR.DICT_FROM_REDO_LOGS";

        cmd.CommandText = $@"
            BEGIN
                DBMS_LOGMNR.START_LOGMNR(
                    STARTSCN => :start_scn,
                    ENDSCN => :end_scn,
                    OPTIONS => {dictOption} + DBMS_LOGMNR.COMMITTED_DATA_ONLY
                );
            END;";
        cmd.Parameters.Clear();
        cmd.Parameters.Add(new OracleParameter("start_scn", startScn));
        cmd.Parameters.Add(new OracleParameter("end_scn", endScn));

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task EndLogMinerAsync(OracleConnection conn, CancellationToken cancellationToken)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;";

        try
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (OracleException)
        {
            // Ignore errors when ending LogMiner session
        }
    }

    private async Task<List<SourceRecord>> QueryLogMinerContentsAsync(OracleConnection conn, CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        await using var cmd = conn.CreateCommand();

        // Build owner filter
        var ownerFilter = string.Join(" OR ", _tableOwners.Select((_, i) => $"seg_owner = :owner{i}"));

#pragma warning disable CA2100 // SQL injection - owners validated from _tableOwners set
        cmd.CommandText = $@"
            SELECT scn, commit_scn, timestamp, operation_code, seg_owner, table_name, sql_redo, row_id
            FROM v$logmnr_contents
            WHERE operation_code IN (1, 2, 3)
              AND seg_type = 2
              AND ({ownerFilter})
            ORDER BY scn";
#pragma warning restore CA2100

        var idx = 0;
        foreach (var owner in _tableOwners)
        {
            cmd.Parameters.Add(new OracleParameter($"owner{idx++}", owner));
        }

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var scn = reader.GetInt64(0);
            var commitScn = reader.IsDBNull(1) ? scn : reader.GetInt64(1);
            var timestamp = reader.GetDateTime(2);
            var operationCode = reader.GetInt32(3);
            var segOwner = reader.GetString(4);
            var tableName = reader.GetString(5);
            var sqlRedo = reader.IsDBNull(6) ? "" : reader.GetString(6);

            // Check if this table is in our list
            var fullTableName = $"{segOwner}.{tableName}";
            if (!_tableColumns.ContainsKey(fullTableName))
                continue;

            SourceRecord? record = null;

            switch (operationCode)
            {
                case OracleConnectorConfig.OperationInsert:
                    record = ParseInsertRecord(segOwner, tableName, sqlRedo, scn, timestamp);
                    break;

                case OracleConnectorConfig.OperationDelete:
                    record = ParseDeleteRecord(segOwner, tableName, sqlRedo, scn, timestamp);
                    break;

                case OracleConnectorConfig.OperationUpdate:
                    record = ParseUpdateRecord(segOwner, tableName, sqlRedo, scn, timestamp);
                    break;
            }

            if (record != null)
            {
                records.Add(record);
                if (records.Count >= _batchMaxRecords)
                    break;
            }
        }

        return records;
    }

    private SourceRecord? ParseInsertRecord(string owner, string tableName, string sqlRedo, long scn, DateTime timestamp)
    {
        var match = InsertPattern.Match(sqlRedo);
        if (!match.Success)
            return null;

        var columns = ParseColumnList(match.Groups[3].Value);
        var values = ParseValueList(match.Groups[4].Value);

        var row = new Dictionary<string, object?>();
        for (var i = 0; i < Math.Min(columns.Count, values.Count); i++)
        {
            row[columns[i]] = ParseSqlValue(values[i]);
        }

        var payload = new Dictionary<string, object?>
        {
            ["op"] = "c", // create/insert
            ["source"] = CreateSourceInfo(owner, tableName, scn, timestamp),
            ["before"] = null,
            ["after"] = row,
            ["ts_ms"] = new DateTimeOffset(timestamp).ToUnixTimeMilliseconds()
        };

        return CreateSourceRecord(owner, tableName, payload, "c", row, scn);
    }

    private SourceRecord? ParseDeleteRecord(string owner, string tableName, string sqlRedo, long scn, DateTime timestamp)
    {
        var match = DeletePattern.Match(sqlRedo);
        if (!match.Success)
            return null;

        var whereClause = match.Groups[3].Value;
        var row = ParseWhereClause(whereClause);

        var payload = new Dictionary<string, object?>
        {
            ["op"] = "d", // delete
            ["source"] = CreateSourceInfo(owner, tableName, scn, timestamp),
            ["before"] = row,
            ["after"] = null,
            ["ts_ms"] = new DateTimeOffset(timestamp).ToUnixTimeMilliseconds()
        };

        return CreateSourceRecord(owner, tableName, payload, "d", row, scn);
    }

    private SourceRecord? ParseUpdateRecord(string owner, string tableName, string sqlRedo, long scn, DateTime timestamp)
    {
        var match = UpdatePattern.Match(sqlRedo);
        if (!match.Success)
            return null;

        var setClause = match.Groups[3].Value;
        var whereClause = match.Groups[4].Value;

        var afterRow = ParseSetClause(setClause);
        var beforeRow = _includeBeforeValues ? ParseWhereClause(whereClause) : null;

        // Merge WHERE clause into after row for complete picture
        if (beforeRow != null)
        {
            foreach (var kvp in beforeRow)
            {
                if (!afterRow.ContainsKey(kvp.Key))
                {
                    afterRow[kvp.Key] = kvp.Value;
                }
            }
        }

        var payload = new Dictionary<string, object?>
        {
            ["op"] = "u", // update
            ["source"] = CreateSourceInfo(owner, tableName, scn, timestamp),
            ["before"] = beforeRow,
            ["after"] = afterRow,
            ["ts_ms"] = new DateTimeOffset(timestamp).ToUnixTimeMilliseconds()
        };

        return CreateSourceRecord(owner, tableName, payload, "u", afterRow, scn);
    }

    private static List<string> ParseColumnList(string columnList)
    {
        return columnList
            .Split(',')
            .Select(c => c.Trim().Trim('"'))
            .ToList();
    }

    private static List<string> ParseValueList(string valueList)
    {
        var values = new List<string>();
        var current = new StringBuilder();
        var inQuote = false;
        var quoteChar = '\0';
        var depth = 0;

        foreach (var c in valueList)
        {
            if (!inQuote)
            {
                if (c == '\'' || c == '"')
                {
                    inQuote = true;
                    quoteChar = c;
                    current.Append(c);
                }
                else if (c == '(')
                {
                    depth++;
                    current.Append(c);
                }
                else if (c == ')')
                {
                    depth--;
                    current.Append(c);
                }
                else if (c == ',' && depth == 0)
                {
                    values.Add(current.ToString().Trim());
                    current.Clear();
                }
                else
                {
                    current.Append(c);
                }
            }
            else
            {
                current.Append(c);
                if (c == quoteChar)
                {
                    inQuote = false;
                }
            }
        }

        if (current.Length > 0)
        {
            values.Add(current.ToString().Trim());
        }

        return values;
    }

    private static object? ParseSqlValue(string value)
    {
        if (string.IsNullOrEmpty(value) || value.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            return null;

        // String literal
        if (value.StartsWith('\'') && value.EndsWith('\''))
        {
            return value[1..^1].Replace("''", "'");
        }

        // Number
        if (decimal.TryParse(value, out var num))
        {
            return num;
        }

        // TIMESTAMP or DATE function
        if (value.StartsWith("TO_TIMESTAMP", StringComparison.OrdinalIgnoreCase) ||
            value.StartsWith("TO_DATE", StringComparison.OrdinalIgnoreCase))
        {
            var match = Regex.Match(value, @"'([^']+)'");
            return match.Success ? match.Groups[1].Value : value;
        }

        return value;
    }

    private static Dictionary<string, object?> ParseWhereClause(string whereClause)
    {
        var row = new Dictionary<string, object?>();

        // Split by AND
        var conditions = Regex.Split(whereClause, @"\s+AND\s+", RegexOptions.IgnoreCase);

        foreach (var condition in conditions)
        {
            // Parse "COLUMN" = value or "COLUMN" IS NULL
            var match = Regex.Match(condition, @"""([^""]+)""\s*(?:=\s*(.+)|IS\s+NULL)", RegexOptions.IgnoreCase);
            if (match.Success)
            {
                var column = match.Groups[1].Value;
                var value = match.Groups[2].Success ? ParseSqlValue(match.Groups[2].Value.Trim()) : null;
                row[column] = value;
            }
        }

        return row;
    }

    private static Dictionary<string, object?> ParseSetClause(string setClause)
    {
        var row = new Dictionary<string, object?>();

        // Match "COLUMN" = value patterns
        var matches = Regex.Matches(setClause, @"""([^""]+)""\s*=\s*([^,]+?)(?=\s*,\s*""|$)", RegexOptions.IgnoreCase);

        foreach (Match match in matches)
        {
            var column = match.Groups[1].Value;
            var value = ParseSqlValue(match.Groups[2].Value.Trim());
            row[column] = value;
        }

        return row;
    }

    private Dictionary<string, object> CreateSourceInfo(string owner, string tableName, long scn, DateTime timestamp)
    {
        return new Dictionary<string, object>
        {
            ["owner"] = owner,
            ["table"] = tableName,
            ["host"] = _host,
            ["scn"] = scn,
            ["timestamp"] = timestamp.ToString("O")
        };
    }

    private SourceRecord CreateSourceRecord(string owner, string tableName, Dictionary<string, object?> payload, string op, Dictionary<string, object?> keySource, long scn)
    {
        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = new Dictionary<string, object>
            {
                [OracleConnectorConfig.OffsetScn] = scn.ToString(),
                [OracleConnectorConfig.OffsetSnapshotCompleted] = _snapshotCompleted ? "true" : "false"
            },
            Topic = GetTopicName(owner, tableName),
            Key = GetPrimaryKeyBytes(keySource),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                [OracleConnectorConfig.HeaderSchema] = Encoding.UTF8.GetBytes(owner),
                [OracleConnectorConfig.HeaderTable] = Encoding.UTF8.GetBytes(tableName),
                [OracleConnectorConfig.HeaderOperation] = Encoding.UTF8.GetBytes(op),
                [OracleConnectorConfig.HeaderScn] = Encoding.UTF8.GetBytes(scn.ToString())
            }
        };
    }

    private static object? ConvertValue(object value)
    {
        return value switch
        {
            null => null,
            DateTime dt => dt.ToString("O"),
            DateTimeOffset dto => dto.ToString("O"),
            TimeSpan ts => ts.ToString(),
            byte[] bytes => Convert.ToBase64String(bytes),
            Guid g => g.ToString(),
            decimal d => d,
            _ => value
        };
    }

    private string GetTopicName(string owner, string table)
    {
        var topic = _topicPattern
            .Replace("${owner}", _includeSchema ? owner : "")
            .Replace("${schema}", _includeSchema ? owner : "") // Alias for owner
            .Replace("${table}", table);

        // Clean up double dots if schema is not included
        if (!_includeSchema)
        {
            topic = topic.Replace("..", ".").TrimStart('.');
        }

        return string.IsNullOrEmpty(_topicPrefix) ? topic : $"{_topicPrefix}{topic}";
    }

    private static (string owner, string table) ParseTableName(string fullName)
    {
        var parts = fullName.Split('.');
        return parts.Length == 2
            ? (parts[0].ToUpperInvariant(), parts[1].ToUpperInvariant())
            : (Environment.UserName.ToUpperInvariant(), parts[0].ToUpperInvariant()); // Default to current user's schema
    }

    private static byte[] GetPrimaryKeyBytes(Dictionary<string, object?> row)
    {
        // Try common primary key column names (Oracle style)
        foreach (var keyName in new[] { "ID", "Id", "id", "ROWID", "PK", "KEY" })
        {
            if (row.TryGetValue(keyName, out var value) && value != null)
            {
                return Encoding.UTF8.GetBytes(value.ToString() ?? "");
            }
        }

        // Fallback to first column
        var firstValue = row.Values.FirstOrDefault();
        return firstValue != null
            ? Encoding.UTF8.GetBytes(firstValue.ToString() ?? "")
            : [];
    }

    private string BuildConnectionString()
    {
        var builder = new OracleConnectionStringBuilder();

        // Build data source
        if (!string.IsNullOrEmpty(_serviceName))
        {
            builder.DataSource = $"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={_host})(PORT={_port}))(CONNECT_DATA=(SERVICE_NAME={_serviceName})))";
        }
        else if (!string.IsNullOrEmpty(_sid))
        {
            builder.DataSource = $"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={_host})(PORT={_port}))(CONNECT_DATA=(SID={_sid})))";
        }

        if (!string.IsNullOrEmpty(_username))
        {
            builder.UserID = _username;
            builder.Password = _password;
        }

        // Note: Wallet authentication is configured via external TNS configuration
        // or by including wallet path in the connection string directly

        return builder.ConnectionString;
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Oracle LogMiner doesn't require acknowledgment feedback
        // Position tracking is done locally via offsets
        return Task.CompletedTask;
    }
}
