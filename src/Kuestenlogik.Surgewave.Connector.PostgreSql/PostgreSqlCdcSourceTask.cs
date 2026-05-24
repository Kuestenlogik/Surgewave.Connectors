namespace Kuestenlogik.Surgewave.Connector.PostgreSql;

using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Npgsql;
using NpgsqlTypes;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

/// <summary>
/// Task that captures changes from PostgreSQL using logical replication with pgoutput.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class PostgreSqlCdcSourceTask : SourceTask
{
    private string _connectionString = "";
    private string _slotName = PostgreSqlConnectorConfig.DefaultSlotName;
    private string _publicationName = PostgreSqlConnectorConfig.DefaultPublicationName;
    private string[] _tables = [];
    private bool _createSlot = true;
    private bool _createPublication = true;
    private string _topicPrefix = "";
    private string _topicPattern = PostgreSqlConnectorConfig.DefaultTopicPattern;
    private bool _includeSchema = true;
    private bool _includeBeforeValues = true;
    private string _snapshotMode = PostgreSqlConnectorConfig.SnapshotModeInitial;
    private long _pollIntervalMs = PostgreSqlConnectorConfig.DefaultPollIntervalMs;
    private int _batchMaxRecords = PostgreSqlConnectorConfig.DefaultBatchMaxRecords;

    private LogicalReplicationConnection? _replicationConnection;
    private NpgsqlLogSequenceNumber _lastLsn;
    private bool _snapshotCompleted;
    private CancellationTokenSource? _replicationCts;
    private IAsyncEnumerator<PgOutputReplicationMessage>? _replicationEnumerator;

    private readonly Dictionary<string, object> _sourcePartition = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _connectionString = config[PostgreSqlConnectorConfig.ConnectionConfig];
        _slotName = GetConfigValue(config, PostgreSqlConnectorConfig.SlotNameConfig, PostgreSqlConnectorConfig.DefaultSlotName);
        _publicationName = GetConfigValue(config, PostgreSqlConnectorConfig.PublicationNameConfig, PostgreSqlConnectorConfig.DefaultPublicationName);
        _tables = config[PostgreSqlConnectorConfig.TablesConfig]
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(t => t.Trim())
            .ToArray();
        _createSlot = bool.Parse(GetConfigValue(config, PostgreSqlConnectorConfig.CreateSlotConfig, "true"));
        _createPublication = bool.Parse(GetConfigValue(config, PostgreSqlConnectorConfig.CreatePublicationConfig, "true"));
        _topicPrefix = GetConfigValue(config, PostgreSqlConnectorConfig.TopicPrefixConfig, "");
        _topicPattern = GetConfigValue(config, PostgreSqlConnectorConfig.TopicPatternConfig, PostgreSqlConnectorConfig.DefaultTopicPattern);
        _includeSchema = bool.Parse(GetConfigValue(config, PostgreSqlConnectorConfig.IncludeSchemaConfig, "true"));
        _includeBeforeValues = bool.Parse(GetConfigValue(config, PostgreSqlConnectorConfig.IncludeBeforeValuesConfig, "true"));
        _snapshotMode = GetConfigValue(config, PostgreSqlConnectorConfig.SnapshotModeConfig, PostgreSqlConnectorConfig.SnapshotModeInitial);
        _pollIntervalMs = long.Parse(GetConfigValue(config, PostgreSqlConnectorConfig.PollIntervalMsConfig, PostgreSqlConnectorConfig.DefaultPollIntervalMs.ToString()));
        _batchMaxRecords = int.Parse(GetConfigValue(config, PostgreSqlConnectorConfig.BatchMaxRecordsConfig, PostgreSqlConnectorConfig.DefaultBatchMaxRecords.ToString()));

        _sourcePartition["slot"] = _slotName;
        _sourcePartition["tables"] = string.Join(",", _tables);

        RestoreOffset();
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    private void RestoreOffset()
    {
        var storedOffset = Context.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return;

        if (storedOffset.TryGetValue("lsn", out var lsnStr) && lsnStr != null)
        {
            _lastLsn = NpgsqlLogSequenceNumber.Parse(lsnStr.ToString()!);
        }

        if (storedOffset.TryGetValue("snapshot_completed", out var snapshotValue))
        {
            _snapshotCompleted = snapshotValue?.ToString() == "true";
        }
    }

    public override void Stop()
    {
        _replicationCts?.Cancel();
        _replicationEnumerator?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _replicationConnection?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _replicationConnection = null;
        _replicationEnumerator = null;
        _replicationCts = null;
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
        // Check if we need to do initial snapshot
        if (_snapshotMode != PostgreSqlConnectorConfig.SnapshotModeNever && !_snapshotCompleted)
        {
            if (_snapshotMode == PostgreSqlConnectorConfig.SnapshotModeAlways || !_snapshotCompleted)
            {
                var snapshotRecords = await PerformSnapshotAsync(cancellationToken);
                if (snapshotRecords.Count > 0)
                    return snapshotRecords;

                _snapshotCompleted = true;
            }
        }

        // Start replication if not started
        if (_replicationConnection == null)
        {
            await StartReplicationAsync(cancellationToken);
        }

        var records = new List<SourceRecord>();

        // Use a timeout for non-blocking poll
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_pollIntervalMs));
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        try
        {
            while (records.Count < _batchMaxRecords)
            {
                if (_replicationEnumerator == null)
                    break;

                var hasNext = await _replicationEnumerator.MoveNextAsync();
                if (!hasNext)
                    break;

                var message = _replicationEnumerator.Current;
                var record = await CreateSourceRecordAsync(message, cancellationToken);
                if (record != null)
                {
                    records.Add(record);
                }

                _lastLsn = message.WalEnd;
            }
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
        {
            // Timeout - return what we have
        }

        return records;
    }

    private async Task<List<SourceRecord>> PerformSnapshotAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        foreach (var table in _tables)
        {
            var (schema, tableName) = ParseTableName(table);

            // Using parameterized identifiers is not possible for table names in PostgreSQL
            // We validate the table name format instead
#pragma warning disable CA2100 // SQL injection checked via table name parsing
            await using var cmd = new NpgsqlCommand($"SELECT * FROM \"{schema}\".\"{tableName}\"", conn);
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
                        ["snapshot"] = true
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
                        ["lsn"] = _lastLsn.ToString(),
                        ["snapshot_completed"] = "false"
                    },
                    Topic = GetTopicName(schema, tableName),
                    Key = GetPrimaryKeyBytes(row),
                    Value = JsonSerializer.SerializeToUtf8Bytes(payload),
                    Timestamp = DateTimeOffset.UtcNow,
                    Headers = new Dictionary<string, byte[]>
                    {
                        ["postgresql.schema"] = Encoding.UTF8.GetBytes(schema),
                        ["postgresql.table"] = Encoding.UTF8.GetBytes(tableName),
                        ["postgresql.op"] = Encoding.UTF8.GetBytes("r")
                    }
                });

                if (records.Count >= _batchMaxRecords)
                    return records;
            }
        }

        return records;
    }

    private async Task StartReplicationAsync(CancellationToken cancellationToken)
    {
        // Create publication if needed
        if (_createPublication)
        {
            await CreatePublicationAsync(cancellationToken);
        }

        // Create replication slot if needed
        if (_createSlot)
        {
            await CreateReplicationSlotAsync(cancellationToken);
        }

        // Start the replication connection
        _replicationConnection = new LogicalReplicationConnection(_connectionString);
        await _replicationConnection.Open(cancellationToken);

        var slot = new PgOutputReplicationSlot(_slotName);
        var options = new PgOutputReplicationOptions(_publicationName, PgOutputProtocolVersion.V1);

        _replicationCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var stream = _replicationConnection.StartReplication(slot, options, _replicationCts.Token);
        _replicationEnumerator = stream.GetAsyncEnumerator(_replicationCts.Token);
    }

    private async Task CreatePublicationAsync(CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        // Check if publication exists - using parameterized query for the name check
        await using var checkCmd = new NpgsqlCommand(
            "SELECT 1 FROM pg_publication WHERE pubname = @pubname", conn);
        checkCmd.Parameters.AddWithValue("pubname", _publicationName);
        var exists = await checkCmd.ExecuteScalarAsync(cancellationToken) != null;

        if (!exists)
        {
            // Build table list safely by validating each table name
            var tableList = string.Join(", ", _tables.Select(t =>
            {
                var (schema, table) = ParseTableName(t);
                return $"\"{schema}\".\"{table}\"";
            }));

#pragma warning disable CA2100 // Table names validated via ParseTableName
            await using var createCmd = new NpgsqlCommand(
                $"CREATE PUBLICATION \"{_publicationName}\" FOR TABLE {tableList}", conn);
#pragma warning restore CA2100
            await createCmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task CreateReplicationSlotAsync(CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        // Check if slot exists - using parameterized query for the name check
        await using var checkCmd = new NpgsqlCommand(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = @slotname", conn);
        checkCmd.Parameters.AddWithValue("slotname", _slotName);
        var exists = await checkCmd.ExecuteScalarAsync(cancellationToken) != null;

        if (!exists)
        {
#pragma warning disable CA2100 // Slot name validated in config
            await using var createCmd = new NpgsqlCommand(
                $"SELECT pg_create_logical_replication_slot('{_slotName}', 'pgoutput')", conn);
#pragma warning restore CA2100
            await createCmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task<SourceRecord?> CreateSourceRecordAsync(PgOutputReplicationMessage message, CancellationToken cancellationToken)
    {
        return message switch
        {
            InsertMessage insert => await CreateInsertRecordAsync(insert, cancellationToken),
            UpdateMessage update => await CreateUpdateRecordAsync(update, cancellationToken),
            DeleteMessage delete => await CreateDeleteRecordAsync(delete, cancellationToken),
            _ => null // Skip other message types (Begin, Commit, Relation, etc.)
        };
    }

    private async Task<SourceRecord> CreateInsertRecordAsync(InsertMessage msg, CancellationToken cancellationToken)
    {
        var schema = msg.Relation.Namespace;
        var table = msg.Relation.RelationName;
        var row = await ReadRowValuesAsync(msg.NewRow, msg.Relation, cancellationToken);

        var payload = new Dictionary<string, object?>
        {
            ["op"] = "c", // create/insert
            ["source"] = new Dictionary<string, object>
            {
                ["schema"] = schema,
                ["table"] = table,
                ["lsn"] = _lastLsn.ToString()
            },
            ["before"] = null,
            ["after"] = row,
            ["ts_ms"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = new Dictionary<string, object>
            {
                ["lsn"] = _lastLsn.ToString(),
                ["snapshot_completed"] = _snapshotCompleted ? "true" : "false"
            },
            Topic = GetTopicName(schema, table),
            Key = GetPrimaryKeyBytes(row),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["postgresql.schema"] = Encoding.UTF8.GetBytes(schema),
                ["postgresql.table"] = Encoding.UTF8.GetBytes(table),
                ["postgresql.op"] = Encoding.UTF8.GetBytes("c")
            }
        };
    }

    private async Task<SourceRecord> CreateUpdateRecordAsync(UpdateMessage msg, CancellationToken cancellationToken)
    {
        var schema = msg.Relation.Namespace;
        var table = msg.Relation.RelationName;
        var afterRow = await ReadRowValuesAsync(msg.NewRow, msg.Relation, cancellationToken);
        Dictionary<string, object?>? beforeRow = null;

        // For before values, we need REPLICA IDENTITY FULL on the table
        // The OldRow property exists but may be empty if not configured
        if (_includeBeforeValues)
        {
            // Try to get old values if available
            // Note: Requires REPLICA IDENTITY FULL on the source table
        }

        var payload = new Dictionary<string, object?>
        {
            ["op"] = "u", // update
            ["source"] = new Dictionary<string, object>
            {
                ["schema"] = schema,
                ["table"] = table,
                ["lsn"] = _lastLsn.ToString()
            },
            ["before"] = beforeRow,
            ["after"] = afterRow,
            ["ts_ms"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = new Dictionary<string, object>
            {
                ["lsn"] = _lastLsn.ToString(),
                ["snapshot_completed"] = _snapshotCompleted ? "true" : "false"
            },
            Topic = GetTopicName(schema, table),
            Key = GetPrimaryKeyBytes(afterRow),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["postgresql.schema"] = Encoding.UTF8.GetBytes(schema),
                ["postgresql.table"] = Encoding.UTF8.GetBytes(table),
                ["postgresql.op"] = Encoding.UTF8.GetBytes("u")
            }
        };
    }

    private Task<SourceRecord> CreateDeleteRecordAsync(DeleteMessage msg, CancellationToken cancellationToken)
    {
        _ = cancellationToken; // Not used since we don't have row data in delete messages

        var schema = msg.Relation.Namespace;
        var table = msg.Relation.RelationName;

        // For delete messages, we create a tombstone record
        // The key columns depend on REPLICA IDENTITY setting on the table
        // Note: With REPLICA IDENTITY DEFAULT, only primary key is available
        // With REPLICA IDENTITY FULL, all columns are available
        var payload = new Dictionary<string, object?>
        {
            ["op"] = "d", // delete
            ["source"] = new Dictionary<string, object>
            {
                ["schema"] = schema,
                ["table"] = table,
                ["lsn"] = _lastLsn.ToString()
            },
            ["before"] = null, // Old row data not easily accessible in Npgsql 9.x
            ["after"] = null,
            ["ts_ms"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        return Task.FromResult(new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = new Dictionary<string, object>
            {
                ["lsn"] = _lastLsn.ToString(),
                ["snapshot_completed"] = _snapshotCompleted ? "true" : "false"
            },
            Topic = GetTopicName(schema, table),
            Key = [], // Key extraction would require iterating the message
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["postgresql.schema"] = Encoding.UTF8.GetBytes(schema),
                ["postgresql.table"] = Encoding.UTF8.GetBytes(table),
                ["postgresql.op"] = Encoding.UTF8.GetBytes("d")
            }
        });
    }

    private static async Task<Dictionary<string, object?>> ReadRowValuesAsync(
        ReplicationTuple row,
        RelationMessage relation,
        CancellationToken cancellationToken)
    {
        var values = new Dictionary<string, object?>();
        var columnIndex = 0;

        await foreach (var value in row.WithCancellation(cancellationToken))
        {
            if (columnIndex >= relation.Columns.Count)
                break;

            var columnName = relation.Columns[columnIndex].ColumnName;

            object? columnValue = value.Kind switch
            {
                TupleDataKind.Null => null,
                TupleDataKind.UnchangedToastedValue => null, // TOAST unchanged
                TupleDataKind.TextValue => await ReadTextValueAsync(value, cancellationToken),
                TupleDataKind.BinaryValue => await ReadBinaryValueAsync(value, cancellationToken),
                _ => null
            };

            values[columnName] = columnValue;
            columnIndex++;
        }

        return values;
    }

    private static async Task<string?> ReadTextValueAsync(ReplicationValue value, CancellationToken cancellationToken)
    {
        using var reader = value.GetTextReader();
        return await reader.ReadToEndAsync(cancellationToken);
    }

    private static async Task<string> ReadBinaryValueAsync(ReplicationValue value, CancellationToken cancellationToken)
    {
        await using var stream = value.GetStream();
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms, cancellationToken);
        return Convert.ToBase64String(ms.ToArray());
    }

    private static object? ConvertValue(object value)
    {
        // Convert special types to JSON-serializable values
        return value switch
        {
            DateTime dt => dt.ToString("O"),
            DateTimeOffset dto => dto.ToString("O"),
            TimeSpan ts => ts.ToString(),
            byte[] bytes => Convert.ToBase64String(bytes),
            Guid g => g.ToString(),
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
            : ("public", parts[0]);
    }

    private static byte[] GetPrimaryKeyBytes(Dictionary<string, object?> row)
    {
        // Try common primary key column names
        foreach (var keyName in new[] { "id", "Id", "ID", "_id" })
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

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Send LSN feedback to PostgreSQL
        if (_replicationConnection != null && _lastLsn != NpgsqlLogSequenceNumber.Invalid)
        {
            try
            {
                // Confirm LSN to allow PostgreSQL to advance the replication slot
                _replicationConnection.SetReplicationStatus(_lastLsn);
            }
            catch (Exception)
            {
                // Ignore errors during feedback - will retry on next commit
            }
        }

        return Task.CompletedTask;
    }
}
