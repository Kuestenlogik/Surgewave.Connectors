using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.SqlServer;

/// <summary>
/// Task that captures changes from SQL Server using Change Data Capture (CDC).
/// Polls CDC change tables for new changes since the last recorded LSN.
/// Produces Debezium-compatible JSON output with operation types and row data.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class SqlServerCdcSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _connectionString = "";
    private string _server = SqlServerConnectorConfig.DefaultServer;
    private string _database = "";
    private string _username = "";
    private string _password = "";
    private bool _trustServerCertificate;
    private bool _encrypt = true;
    private string[] _tables = [];
    private string _topicPrefix = "";
    private string _topicPattern = SqlServerConnectorConfig.DefaultTopicPattern;
    private bool _includeSchema = true;
    private bool _includeBeforeValues = true;
    private string _snapshotMode = SqlServerConnectorConfig.SnapshotModeInitial;
    private long _pollIntervalMs = SqlServerConnectorConfig.DefaultPollIntervalMs;
    private int _batchMaxRecords = SqlServerConnectorConfig.DefaultBatchMaxRecords;
    private bool _startFromBeginning;

    private byte[]? _lastLsn;
    private bool _snapshotCompleted;
    private readonly Dictionary<string, string> _captureInstances = new();
    private readonly Dictionary<string, List<string>> _tableColumns = new();

    private readonly Dictionary<string, object> _sourcePartition = new();

    public override void Start(IDictionary<string, string> config)
    {
        // Build connection string
        if (config.TryGetValue(SqlServerConnectorConfig.ConnectionString, out var connStr) && !string.IsNullOrEmpty(connStr))
        {
            _connectionString = connStr;
        }
        else
        {
            _server = GetConfigValue(config, SqlServerConnectorConfig.Server, SqlServerConnectorConfig.DefaultServer);
            _database = config[SqlServerConnectorConfig.Database];
            _username = GetConfigValue(config, SqlServerConnectorConfig.Username, "");
            _password = GetConfigValue(config, SqlServerConnectorConfig.Password, "");
            _trustServerCertificate = bool.Parse(GetConfigValue(config, SqlServerConnectorConfig.TrustServerCertificate, "false"));
            _encrypt = bool.Parse(GetConfigValue(config, SqlServerConnectorConfig.Encrypt, "true"));

            _connectionString = BuildConnectionString();
        }

        _tables = config[SqlServerConnectorConfig.Tables]
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(t => t.Trim())
            .ToArray();

        _topicPrefix = GetConfigValue(config, SqlServerConnectorConfig.TopicPrefix, "");
        _topicPattern = GetConfigValue(config, SqlServerConnectorConfig.TopicPattern, SqlServerConnectorConfig.DefaultTopicPattern);
        _includeSchema = bool.Parse(GetConfigValue(config, SqlServerConnectorConfig.IncludeSchema, "true"));
        _includeBeforeValues = bool.Parse(GetConfigValue(config, SqlServerConnectorConfig.IncludeBeforeValues, "true"));
        _snapshotMode = GetConfigValue(config, SqlServerConnectorConfig.SnapshotMode, SqlServerConnectorConfig.SnapshotModeInitial);
        _pollIntervalMs = long.Parse(GetConfigValue(config, SqlServerConnectorConfig.PollIntervalMs, SqlServerConnectorConfig.DefaultPollIntervalMs.ToString()));
        _batchMaxRecords = int.Parse(GetConfigValue(config, SqlServerConnectorConfig.BatchMaxRecords, SqlServerConnectorConfig.DefaultBatchMaxRecords.ToString()));
        _startFromBeginning = bool.Parse(GetConfigValue(config, SqlServerConnectorConfig.StartFromBeginning, "false"));

        _sourcePartition["server"] = _server;
        _sourcePartition["database"] = _database;

        RestoreOffset();
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    private void RestoreOffset()
    {
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return;

        if (storedOffset.TryGetValue(SqlServerConnectorConfig.OffsetLsn, out var lsnStr) && lsnStr != null)
        {
            _lastLsn = Convert.FromHexString(lsnStr.ToString()!);
        }

        if (storedOffset.TryGetValue(SqlServerConnectorConfig.OffsetSnapshotCompleted, out var snapshotValue))
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
        // Initialize CDC metadata on first poll
        if (_captureInstances.Count == 0)
        {
            await InitializeCdcMetadataAsync(cancellationToken);
        }

        // Check if we need to do initial snapshot
        if (_snapshotMode != SqlServerConnectorConfig.SnapshotModeNever && !_snapshotCompleted)
        {
            if (_snapshotMode != SqlServerConnectorConfig.SnapshotModeSchemaOnly)
            {
                var snapshotRecords = await PerformSnapshotAsync(cancellationToken);
                if (snapshotRecords.Count > 0)
                    return snapshotRecords;
            }
            _snapshotCompleted = true;
        }

        // Initialize LSN if not set
        if (_lastLsn == null)
        {
            _lastLsn = _startFromBeginning
                ? await GetMinLsnAsync(cancellationToken)
                : await GetMaxLsnAsync(cancellationToken);
        }

        // Poll for CDC changes
        return await PollCdcChangesAsync(cancellationToken);
    }

    private async Task InitializeCdcMetadataAsync(CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        foreach (var table in _tables)
        {
            var (schema, tableName) = ParseTableName(table);

            // Get capture instance name for this table
#pragma warning disable CA2000 // Disposed via await using
            await using var cmd = new SqlCommand(@"
                SELECT capture_instance
                FROM cdc.change_tables
                WHERE source_schema = @schema AND source_table = @table", conn);
#pragma warning restore CA2000
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@table", tableName);

            var captureInstance = await cmd.ExecuteScalarAsync(cancellationToken) as string;
            if (string.IsNullOrEmpty(captureInstance))
            {
                throw new InvalidOperationException(
                    $"CDC is not enabled for table {schema}.{tableName}. " +
                    $"Enable CDC with: EXEC sys.sp_cdc_enable_table @source_schema='{schema}', @source_name='{tableName}', @role_name=NULL");
            }

            _captureInstances[$"{schema}.{tableName}"] = captureInstance;

            // Get column names for this capture instance
            await using var colCmd = new SqlCommand(@"
                SELECT column_name
                FROM cdc.captured_columns
                WHERE capture_instance = @capture_instance
                ORDER BY column_ordinal", conn);
            colCmd.Parameters.AddWithValue("@capture_instance", captureInstance);

            var columns = new List<string>();
            await using var reader = await colCmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                columns.Add(reader.GetString(0));
            }
            _tableColumns[$"{schema}.{tableName}"] = columns;
        }
    }

    private async Task<byte[]> GetMinLsnAsync(CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        // Get minimum LSN across all capture instances
        byte[]? minLsn = null;
        foreach (var captureInstance in _captureInstances.Values)
        {
#pragma warning disable CA2100 // Capture instance validated from cdc.change_tables
            await using var cmd = new SqlCommand(
                $"SELECT sys.fn_cdc_get_min_lsn('{captureInstance}')", conn);
#pragma warning restore CA2100
            var lsn = await cmd.ExecuteScalarAsync(cancellationToken) as byte[];
            if (lsn != null && (minLsn == null || CompareLsn(lsn, minLsn) < 0))
            {
                minLsn = lsn;
            }
        }

        return minLsn ?? new byte[10];
    }

    private async Task<byte[]> GetMaxLsnAsync(CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new SqlCommand("SELECT sys.fn_cdc_get_max_lsn()", conn);
        return await cmd.ExecuteScalarAsync(cancellationToken) as byte[] ?? new byte[10];
    }

    private async Task<List<SourceRecord>> PerformSnapshotAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        foreach (var table in _tables)
        {
            var (schema, tableName) = ParseTableName(table);

#pragma warning disable CA2100 // SQL injection - table names validated
            await using var cmd = new SqlCommand($"SELECT * FROM [{schema}].[{tableName}]", conn);
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
                        ["schema"] = schema,
                        ["table"] = tableName,
                        ["snapshot"] = true,
                        ["server"] = _server
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
                        [SqlServerConnectorConfig.OffsetLsn] = _lastLsn != null ? Convert.ToHexString(_lastLsn) : "",
                        [SqlServerConnectorConfig.OffsetSnapshotCompleted] = "false"
                    },
                    Topic = GetTopicName(schema, tableName),
                    Key = GetPrimaryKeyBytes(row),
                    Value = JsonSerializer.SerializeToUtf8Bytes(payload),
                    Timestamp = DateTimeOffset.UtcNow,
                    Headers = new Dictionary<string, byte[]>
                    {
                        ["sqlserver.schema"] = Encoding.UTF8.GetBytes(schema),
                        ["sqlserver.table"] = Encoding.UTF8.GetBytes(tableName),
                        ["sqlserver.op"] = Encoding.UTF8.GetBytes("r")
                    }
                });

                if (records.Count >= _batchMaxRecords)
                    return records;
            }
        }

        return records;
    }

    private async Task<List<SourceRecord>> PollCdcChangesAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        // Get current max LSN
        var maxLsn = await GetMaxLsnAsync(cancellationToken);

        // Skip if no new changes
        if (_lastLsn != null && CompareLsn(_lastLsn, maxLsn) >= 0)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs), cancellationToken);
            return records;
        }

        foreach (var table in _tables)
        {
            var (schema, tableName) = ParseTableName(table);
            var fullTableName = $"{schema}.{tableName}";

            if (!_captureInstances.TryGetValue(fullTableName, out var captureInstance))
                continue;

            if (!_tableColumns.TryGetValue(fullTableName, out var columns))
                continue;

            // Query CDC changes for this table
            var tableRecords = await QueryCdcChangesAsync(
                conn, schema, tableName, captureInstance, columns, _lastLsn!, maxLsn, cancellationToken);

            records.AddRange(tableRecords);

            if (records.Count >= _batchMaxRecords)
                break;
        }

        // Update last LSN if we got any records
        if (records.Count > 0)
        {
            _lastLsn = maxLsn;
        }

        return records;
    }

    private async Task<List<SourceRecord>> QueryCdcChangesAsync(
        SqlConnection conn,
        string schema,
        string tableName,
        string captureInstance,
        List<string> columns,
        byte[] fromLsn,
        byte[] toLsn,
        CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        // Build column list for select
        var columnList = string.Join(", ", columns.Select(c => $"[{c}]"));

        // Use fn_cdc_get_all_changes to get all changes (including before/after for updates)
        var sql = $@"
            SELECT __$start_lsn, __$seqval, __$operation, __$update_mask, {columnList}
            FROM cdc.fn_cdc_get_all_changes_{captureInstance}(@from_lsn, @to_lsn, 'all update old')
            ORDER BY __$start_lsn, __$seqval";

#pragma warning disable CA2100 // Capture instance and columns validated from cdc system tables
        await using var cmd = new SqlCommand(sql, conn);
#pragma warning restore CA2100
        cmd.Parameters.AddWithValue("@from_lsn", fromLsn);
        cmd.Parameters.AddWithValue("@to_lsn", toLsn);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        Dictionary<string, object?>? pendingBeforeRow = null;

        while (await reader.ReadAsync(cancellationToken))
        {
            var lsn = (byte[])reader["__$start_lsn"];
            var seqval = (byte[])reader["__$seqval"];
            var operation = (int)reader["__$operation"];

            var row = new Dictionary<string, object?>();
            foreach (var col in columns)
            {
                var ordinal = reader.GetOrdinal(col);
                row[col] = reader.IsDBNull(ordinal) ? null : ConvertValue(reader.GetValue(ordinal));
            }

            SourceRecord? record = null;

            switch (operation)
            {
                case SqlServerConnectorConfig.CdcOperationInsert:
                    record = CreateInsertRecord(schema, tableName, row, lsn);
                    break;

                case SqlServerConnectorConfig.CdcOperationDelete:
                    record = CreateDeleteRecord(schema, tableName, row, lsn);
                    break;

                case SqlServerConnectorConfig.CdcOperationUpdateBefore:
                    // Store before row for the following update after
                    if (_includeBeforeValues)
                    {
                        pendingBeforeRow = row;
                    }
                    break;

                case SqlServerConnectorConfig.CdcOperationUpdateAfter:
                    record = CreateUpdateRecord(schema, tableName, pendingBeforeRow, row, lsn);
                    pendingBeforeRow = null;
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

    private SourceRecord CreateInsertRecord(string schema, string tableName, Dictionary<string, object?> row, byte[] lsn)
    {
        var payload = new Dictionary<string, object?>
        {
            ["op"] = "c", // create/insert
            ["source"] = CreateSourceInfo(schema, tableName, lsn),
            ["before"] = null,
            ["after"] = row,
            ["ts_ms"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        return CreateSourceRecord(schema, tableName, payload, "c", row, lsn);
    }

    private SourceRecord CreateUpdateRecord(string schema, string tableName, Dictionary<string, object?>? beforeRow, Dictionary<string, object?> afterRow, byte[] lsn)
    {
        var payload = new Dictionary<string, object?>
        {
            ["op"] = "u", // update
            ["source"] = CreateSourceInfo(schema, tableName, lsn),
            ["before"] = beforeRow,
            ["after"] = afterRow,
            ["ts_ms"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        return CreateSourceRecord(schema, tableName, payload, "u", afterRow, lsn);
    }

    private SourceRecord CreateDeleteRecord(string schema, string tableName, Dictionary<string, object?> row, byte[] lsn)
    {
        var payload = new Dictionary<string, object?>
        {
            ["op"] = "d", // delete
            ["source"] = CreateSourceInfo(schema, tableName, lsn),
            ["before"] = row,
            ["after"] = null,
            ["ts_ms"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        return CreateSourceRecord(schema, tableName, payload, "d", row, lsn);
    }

    private Dictionary<string, object> CreateSourceInfo(string schema, string tableName, byte[] lsn)
    {
        return new Dictionary<string, object>
        {
            ["schema"] = schema,
            ["table"] = tableName,
            ["server"] = _server,
            ["lsn"] = Convert.ToHexString(lsn)
        };
    }

    private SourceRecord CreateSourceRecord(string schema, string tableName, Dictionary<string, object?> payload, string op, Dictionary<string, object?> keySource, byte[] lsn)
    {
        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = new Dictionary<string, object>
            {
                [SqlServerConnectorConfig.OffsetLsn] = Convert.ToHexString(lsn),
                [SqlServerConnectorConfig.OffsetSnapshotCompleted] = _snapshotCompleted ? "true" : "false"
            },
            Topic = GetTopicName(schema, tableName),
            Key = GetPrimaryKeyBytes(keySource),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["sqlserver.schema"] = Encoding.UTF8.GetBytes(schema),
                ["sqlserver.table"] = Encoding.UTF8.GetBytes(tableName),
                ["sqlserver.op"] = Encoding.UTF8.GetBytes(op)
            }
        };
    }

    private static int CompareLsn(byte[] lsn1, byte[] lsn2)
    {
        for (var i = 0; i < Math.Min(lsn1.Length, lsn2.Length); i++)
        {
            if (lsn1[i] < lsn2[i]) return -1;
            if (lsn1[i] > lsn2[i]) return 1;
        }
        return lsn1.Length.CompareTo(lsn2.Length);
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

    private string GetTopicName(string schema, string table)
    {
        var topic = _topicPattern
            .Replace("${schema}", _includeSchema ? schema : "")
            .Replace("${table}", table);

        // Clean up double dots if schema is not included
        if (!_includeSchema)
        {
            topic = topic.Replace("..", ".").TrimStart('.');
        }

        return string.IsNullOrEmpty(_topicPrefix) ? topic : $"{_topicPrefix}{topic}";
    }

    private static (string schema, string table) ParseTableName(string fullName)
    {
        var parts = fullName.Split('.');
        return parts.Length == 2
            ? (parts[0], parts[1])
            : ("dbo", parts[0]);
    }

    private static byte[] GetPrimaryKeyBytes(Dictionary<string, object?> row)
    {
        // Try common primary key column names
        foreach (var keyName in new[] { "Id", "id", "ID", "_id", "PK" })
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
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = _server,
            InitialCatalog = _database,
            TrustServerCertificate = _trustServerCertificate,
            Encrypt = _encrypt
        };

        if (!string.IsNullOrEmpty(_username))
        {
            builder.UserID = _username;
            builder.Password = _password;
        }
        else
        {
            builder.IntegratedSecurity = true;
        }

        return builder.ConnectionString;
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // SQL Server CDC doesn't require acknowledgment feedback
        // Position tracking is done locally via offsets
        return Task.CompletedTask;
    }
}
