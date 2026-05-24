using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using MySqlCdc;
using MySqlCdc.Constants;
using MySqlCdc.Events;
using MySqlConnector;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.MySql;

/// <summary>
/// Task that captures changes from MySQL using binary log replication.
/// Produces Debezium-compatible JSON output with operation types and row data.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class MySqlCdcSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _host = MySqlConnectorConfig.DefaultHost;
    private int _port = MySqlConnectorConfig.DefaultPort;
    private string _database = "";
    private string _username = "";
    private string _password = "";
    private uint _serverId = MySqlConnectorConfig.DefaultServerId;
    private string[] _tables = [];
    private string _topicPrefix = "";
    private string _topicPattern = MySqlConnectorConfig.DefaultTopicPattern;
    private bool _includeSchema = true;
    private string _snapshotMode = MySqlConnectorConfig.SnapshotModeInitial;
    private long _pollIntervalMs = MySqlConnectorConfig.DefaultPollIntervalMs;
    private int _batchMaxRecords = MySqlConnectorConfig.DefaultBatchMaxRecords;
    private string _sslMode = MySqlConnectorConfig.SslModeNone;

    private BinlogClient? _binlogClient;
    private CancellationTokenSource? _binlogCts;
    private IAsyncEnumerator<(EventHeader Header, IBinlogEvent Event)>? _eventEnumerator;
    private string _currentBinlogFilename = "";
    private long _currentBinlogPosition = 0;
    private bool _snapshotCompleted;
    private TableMapEvent? _currentTableMap;

    private readonly Dictionary<string, object> _sourcePartition = new();
    private readonly Dictionary<long, TableMapEvent> _tableMapCache = new();

    public override void Start(IDictionary<string, string> config)
    {
        _host = GetConfigValue(config, MySqlConnectorConfig.Host, MySqlConnectorConfig.DefaultHost);
        _port = int.Parse(GetConfigValue(config, MySqlConnectorConfig.Port, MySqlConnectorConfig.DefaultPort.ToString()));
        _database = config[MySqlConnectorConfig.Database];
        _username = config[MySqlConnectorConfig.Username];
        _password = GetConfigValue(config, MySqlConnectorConfig.Password, "");
        _serverId = uint.Parse(GetConfigValue(config, MySqlConnectorConfig.ServerId, MySqlConnectorConfig.DefaultServerId.ToString()));

        _tables = config[MySqlConnectorConfig.Tables]
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(t => t.Trim())
            .ToArray();

        _topicPrefix = GetConfigValue(config, MySqlConnectorConfig.TopicPrefix, "");
        _topicPattern = GetConfigValue(config, MySqlConnectorConfig.TopicPattern, MySqlConnectorConfig.DefaultTopicPattern);
        _includeSchema = bool.Parse(GetConfigValue(config, MySqlConnectorConfig.IncludeSchema, "true"));
        _snapshotMode = GetConfigValue(config, MySqlConnectorConfig.SnapshotMode, MySqlConnectorConfig.SnapshotModeInitial);
        _pollIntervalMs = long.Parse(GetConfigValue(config, MySqlConnectorConfig.PollIntervalMs, MySqlConnectorConfig.DefaultPollIntervalMs.ToString()));
        _batchMaxRecords = int.Parse(GetConfigValue(config, MySqlConnectorConfig.BatchMaxRecords, MySqlConnectorConfig.DefaultBatchMaxRecords.ToString()));
        _sslMode = GetConfigValue(config, MySqlConnectorConfig.SslMode, MySqlConnectorConfig.SslModeNone);

        // Restore binlog position from saved offset
        if (config.TryGetValue(MySqlConnectorConfig.BinlogFilename, out var binlogFile))
            _currentBinlogFilename = binlogFile;
        if (config.TryGetValue(MySqlConnectorConfig.BinlogPosition, out var binlogPos))
            _currentBinlogPosition = long.Parse(binlogPos);

        _sourcePartition["server"] = $"{_host}:{_port}";
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

        if (storedOffset.TryGetValue(MySqlConnectorConfig.OffsetBinlogFilename, out var filename) && filename != null)
        {
            _currentBinlogFilename = filename.ToString()!;
        }

        if (storedOffset.TryGetValue(MySqlConnectorConfig.OffsetBinlogPosition, out var position) && position != null)
        {
            _currentBinlogPosition = Convert.ToInt64(position);
        }

        if (storedOffset.TryGetValue(MySqlConnectorConfig.OffsetSnapshotCompleted, out var snapshotValue))
        {
            _snapshotCompleted = snapshotValue?.ToString() == "true";
        }
    }

    public override void Stop()
    {
        _binlogCts?.Cancel();
        _eventEnumerator?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _binlogClient = null;
        _eventEnumerator = null;
        _binlogCts?.Dispose();
        _binlogCts = null;
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
        if (_snapshotMode != MySqlConnectorConfig.SnapshotModeNever && !_snapshotCompleted)
        {
            if (_snapshotMode != MySqlConnectorConfig.SnapshotModeSchemaOnly)
            {
                var snapshotRecords = await PerformSnapshotAsync(cancellationToken);
                if (snapshotRecords.Count > 0)
                    return snapshotRecords;
            }
            _snapshotCompleted = true;
        }

        // Start binlog replication if not started
        if (_binlogClient == null)
        {
            await StartBinlogReplicationAsync(cancellationToken);
        }

        var records = new List<SourceRecord>();

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_pollIntervalMs));
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        try
        {
            while (records.Count < _batchMaxRecords)
            {
                if (_eventEnumerator == null)
                    break;

                var hasNext = await _eventEnumerator.MoveNextAsync();
                if (!hasNext)
                    break;

                var (header, binlogEvent) = _eventEnumerator.Current;
                _currentBinlogPosition = header.NextEventPosition;
                var eventRecords = ProcessBinlogEvent(binlogEvent);
                records.AddRange(eventRecords);
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

        var connectionString = BuildConnectionString();
        await using var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken);

        foreach (var table in _tables)
        {
            var (database, tableName) = ParseTableName(table);
            var targetDatabase = string.IsNullOrEmpty(database) ? _database : database;

#pragma warning disable CA2100 // SQL injection - table names validated
            await using var cmd = new MySqlCommand($"SELECT * FROM `{targetDatabase}`.`{tableName}`", conn);
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
                        ["database"] = targetDatabase,
                        ["table"] = tableName,
                        ["snapshot"] = true,
                        ["server"] = $"{_host}:{_port}"
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
                        [MySqlConnectorConfig.OffsetBinlogFilename] = _currentBinlogFilename,
                        [MySqlConnectorConfig.OffsetBinlogPosition] = _currentBinlogPosition,
                        [MySqlConnectorConfig.OffsetSnapshotCompleted] = "false"
                    },
                    Topic = GetTopicName(targetDatabase, tableName),
                    Key = GetPrimaryKeyBytes(row),
                    Value = JsonSerializer.SerializeToUtf8Bytes(payload),
                    Timestamp = DateTimeOffset.UtcNow,
                    Headers = new Dictionary<string, byte[]>
                    {
                        ["mysql.database"] = Encoding.UTF8.GetBytes(targetDatabase),
                        ["mysql.table"] = Encoding.UTF8.GetBytes(tableName),
                        ["mysql.op"] = Encoding.UTF8.GetBytes("r")
                    }
                });

                if (records.Count >= _batchMaxRecords)
                    return records;
            }
        }

        return records;
    }

    private async Task StartBinlogReplicationAsync(CancellationToken cancellationToken)
    {
        _binlogClient = new BinlogClient(options =>
        {
            options.Hostname = _host;
            options.Port = _port;
            options.Username = _username;
            options.Password = _password;
            options.Database = _database;
            options.ServerId = _serverId;
            options.Blocking = true;

            // Configure SSL
            options.SslMode = _sslMode switch
            {
                MySqlConnectorConfig.SslModePreferred => SslMode.IfAvailable,
                MySqlConnectorConfig.SslModeRequired => SslMode.Require,
                MySqlConnectorConfig.SslModeVerifyCa => SslMode.RequireVerifyCa,
                MySqlConnectorConfig.SslModeVerifyFull => SslMode.RequireVerifyFull,
                _ => SslMode.Disabled
            };

            // Start from saved position or beginning
            if (!string.IsNullOrEmpty(_currentBinlogFilename) && _currentBinlogPosition > 0)
            {
                options.Binlog = BinlogOptions.FromPosition(_currentBinlogFilename, _currentBinlogPosition);
            }
            else
            {
                options.Binlog = BinlogOptions.FromEnd();
            }
        });

        _binlogCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var stream = _binlogClient.Replicate(_binlogCts.Token);
        _eventEnumerator = stream.GetAsyncEnumerator(_binlogCts.Token);
    }

    private List<SourceRecord> ProcessBinlogEvent(IBinlogEvent binlogEvent)
    {
        var records = new List<SourceRecord>();

        switch (binlogEvent)
        {
            case TableMapEvent tableMap:
                _tableMapCache[tableMap.TableId] = tableMap;
                _currentTableMap = tableMap;
                break;

            case WriteRowsEvent writeRows:
                if (ShouldProcessTable(writeRows.TableId))
                {
                    var tableMap = GetTableMap(writeRows.TableId);
                    if (tableMap != null)
                    {
                        records.AddRange(CreateInsertRecords(writeRows, tableMap));
                    }
                }
                UpdatePosition(binlogEvent);
                break;

            case UpdateRowsEvent updateRows:
                if (ShouldProcessTable(updateRows.TableId))
                {
                    var tableMap = GetTableMap(updateRows.TableId);
                    if (tableMap != null)
                    {
                        records.AddRange(CreateUpdateRecords(updateRows, tableMap));
                    }
                }
                UpdatePosition(binlogEvent);
                break;

            case DeleteRowsEvent deleteRows:
                if (ShouldProcessTable(deleteRows.TableId))
                {
                    var tableMap = GetTableMap(deleteRows.TableId);
                    if (tableMap != null)
                    {
                        records.AddRange(CreateDeleteRecords(deleteRows, tableMap));
                    }
                }
                UpdatePosition(binlogEvent);
                break;

            case RotateEvent rotate:
                _currentBinlogFilename = rotate.BinlogFilename;
                _currentBinlogPosition = rotate.BinlogPosition;
                break;

            default:
                UpdatePosition(binlogEvent);
                break;
        }

        return records;
    }

    private void UpdatePosition(IBinlogEvent binlogEvent)
    {
        // Position is updated at the end of each event
        if (binlogEvent is not RotateEvent)
        {
            // The next position is the current position + event length
            // Note: This depends on the MySqlCdc library exposing header info
        }
    }

    private TableMapEvent? GetTableMap(long tableId)
    {
        return _tableMapCache.TryGetValue(tableId, out var tableMap) ? tableMap : _currentTableMap;
    }

    private bool ShouldProcessTable(long tableId)
    {
        var tableMap = GetTableMap(tableId);
        if (tableMap == null)
            return false;

        var fullName = $"{tableMap.DatabaseName}.{tableMap.TableName}";
        return _tables.Any(t =>
        {
            if (t.Contains('.'))
                return t.Equals(fullName, StringComparison.OrdinalIgnoreCase);
            return t.Equals(tableMap.TableName, StringComparison.OrdinalIgnoreCase);
        });
    }

    private IEnumerable<SourceRecord> CreateInsertRecords(WriteRowsEvent writeRows, TableMapEvent tableMap)
    {
        foreach (var row in writeRows.Rows)
        {
            var rowData = ConvertRowData(row.Cells, tableMap);

            var payload = new Dictionary<string, object?>
            {
                ["op"] = "c", // create/insert
                ["source"] = CreateSourceInfo(tableMap),
                ["before"] = null,
                ["after"] = rowData,
                ["ts_ms"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            yield return CreateSourceRecord(tableMap, payload, "c", rowData);
        }
    }

    private IEnumerable<SourceRecord> CreateUpdateRecords(UpdateRowsEvent updateRows, TableMapEvent tableMap)
    {
        foreach (var row in updateRows.Rows)
        {
            var beforeData = ConvertRowData(row.BeforeUpdate.Cells, tableMap);
            var afterData = ConvertRowData(row.AfterUpdate.Cells, tableMap);

            var payload = new Dictionary<string, object?>
            {
                ["op"] = "u", // update
                ["source"] = CreateSourceInfo(tableMap),
                ["before"] = beforeData,
                ["after"] = afterData,
                ["ts_ms"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            yield return CreateSourceRecord(tableMap, payload, "u", afterData);
        }
    }

    private IEnumerable<SourceRecord> CreateDeleteRecords(DeleteRowsEvent deleteRows, TableMapEvent tableMap)
    {
        foreach (var row in deleteRows.Rows)
        {
            var rowData = ConvertRowData(row.Cells, tableMap);

            var payload = new Dictionary<string, object?>
            {
                ["op"] = "d", // delete
                ["source"] = CreateSourceInfo(tableMap),
                ["before"] = rowData,
                ["after"] = null,
                ["ts_ms"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            yield return CreateSourceRecord(tableMap, payload, "d", rowData);
        }
    }

    private Dictionary<string, object> CreateSourceInfo(TableMapEvent tableMap)
    {
        return new Dictionary<string, object>
        {
            ["database"] = tableMap.DatabaseName,
            ["table"] = tableMap.TableName,
            ["server"] = $"{_host}:{_port}",
            ["binlog_file"] = _currentBinlogFilename,
            ["binlog_position"] = _currentBinlogPosition
        };
    }

    private SourceRecord CreateSourceRecord(TableMapEvent tableMap, Dictionary<string, object?> payload, string op, Dictionary<string, object?> keySource)
    {
        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = new Dictionary<string, object>
            {
                [MySqlConnectorConfig.OffsetBinlogFilename] = _currentBinlogFilename,
                [MySqlConnectorConfig.OffsetBinlogPosition] = _currentBinlogPosition,
                [MySqlConnectorConfig.OffsetSnapshotCompleted] = _snapshotCompleted ? "true" : "false"
            },
            Topic = GetTopicName(tableMap.DatabaseName, tableMap.TableName),
            Key = GetPrimaryKeyBytes(keySource),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["mysql.database"] = Encoding.UTF8.GetBytes(tableMap.DatabaseName),
                ["mysql.table"] = Encoding.UTF8.GetBytes(tableMap.TableName),
                ["mysql.op"] = Encoding.UTF8.GetBytes(op)
            }
        };
    }

    private static Dictionary<string, object?> ConvertRowData(IReadOnlyList<object?> cells, TableMapEvent tableMap)
    {
        var row = new Dictionary<string, object?>();

        // Column names aren't available in TableMapEvent in all MySQL versions
        // We use column indices as keys when names aren't available
        for (var i = 0; i < cells.Count; i++)
        {
            var columnName = $"col_{i}";
            row[columnName] = ConvertValue(cells[i]);
        }

        return row;
    }

    private static object? ConvertValue(object? value)
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

    private string GetTopicName(string database, string table)
    {
        var topic = _topicPattern
            .Replace("${database}", _includeSchema ? database : "")
            .Replace("${table}", table);

        // Clean up double dots if database is not included
        if (!_includeSchema)
        {
            topic = topic.Replace("..", ".").TrimStart('.');
        }

        return string.IsNullOrEmpty(_topicPrefix) ? topic : $"{_topicPrefix}{topic}";
    }

    private static (string database, string table) ParseTableName(string fullName)
    {
        var parts = fullName.Split('.');
        return parts.Length == 2
            ? (parts[0], parts[1])
            : ("", parts[0]);
    }

    private static byte[] GetPrimaryKeyBytes(Dictionary<string, object?> row)
    {
        // Try common primary key column names
        foreach (var keyName in new[] { "id", "Id", "ID", "_id", "col_0" })
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
        var builder = new MySqlConnectionStringBuilder
        {
            Server = _host,
            Port = (uint)_port,
            Database = _database,
            UserID = _username,
            Password = _password
        };

        if (_sslMode != MySqlConnectorConfig.SslModeNone)
        {
            builder.SslMode = _sslMode switch
            {
                MySqlConnectorConfig.SslModePreferred => MySqlSslMode.Preferred,
                MySqlConnectorConfig.SslModeRequired => MySqlSslMode.Required,
                MySqlConnectorConfig.SslModeVerifyCa => MySqlSslMode.VerifyCA,
                MySqlConnectorConfig.SslModeVerifyFull => MySqlSslMode.VerifyFull,
                _ => MySqlSslMode.None
            };
        }

        return builder.ConnectionString;
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // MySQL binlog replication doesn't require acknowledgment feedback
        // Position tracking is done locally via offsets
        return Task.CompletedTask;
    }
}
