using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Npgsql;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.TimescaleDB;

/// <summary>
/// Task that reads time-series data from TimescaleDB.
/// </summary>
[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Table/column names from configuration, not user input; SQL does not support parameterized identifiers")]
public sealed class TimescaleSourceTask : SourceTask
{
    private NpgsqlDataSource? _dataSource;
    private string _topic = null!;
    private string? _query;
    private string? _table;
    private string _timeColumn = null!;
    private string[]? _columns;
    private int _pollIntervalMs;
    private int _lookbackSeconds;
    private int _rowLimit;
    private DateTime _lastPoll = DateTime.MinValue;
    private DateTime _lastTimestamp;
    private long _messageId;
    private bool _initialized;
    private Dictionary<string, object> _sourcePartition = null!;

    public override string Version => "1.0.0";

    /// <summary>The incremental cursor the next query reads from.</summary>
    internal DateTime LastTimestamp => _lastTimestamp;

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[TimescaleConnectorConfig.Topic];
        _query = config.GetValueOrDefault(TimescaleConnectorConfig.Query, null);
        _table = config.GetValueOrDefault(TimescaleConnectorConfig.Table, null);
        _timeColumn = config.GetValueOrDefault(TimescaleConnectorConfig.TimeColumn, "time")!;
        _sourcePartition = new Dictionary<string, object>
        {
            ["source"] = "timescale",
            ["table"] = _table ?? "query"
        };

        _pollIntervalMs = int.Parse(config.GetValueOrDefault(TimescaleConnectorConfig.PollIntervalMs,
            TimescaleConnectorConfig.DefaultPollIntervalMs.ToString())!);
        _lookbackSeconds = int.Parse(config.GetValueOrDefault(TimescaleConnectorConfig.LookbackSeconds,
            TimescaleConnectorConfig.DefaultLookbackSeconds.ToString())!);
        _rowLimit = int.Parse(config.GetValueOrDefault(TimescaleConnectorConfig.RowLimit,
            TimescaleConnectorConfig.DefaultRowLimit.ToString())!);

        var columnsStr = config.GetValueOrDefault(TimescaleConnectorConfig.Columns, "");
        if (!string.IsNullOrWhiteSpace(columnsStr))
        {
            _columns = columnsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        // Build connection string
        var connectionString = config.GetValueOrDefault(TimescaleConnectorConfig.ConnectionString, "");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            var host = config.GetValueOrDefault(TimescaleConnectorConfig.Host, "localhost")!;
            var port = int.Parse(config.GetValueOrDefault(TimescaleConnectorConfig.Port,
                TimescaleConnectorConfig.DefaultPort.ToString())!);
            var database = config[TimescaleConnectorConfig.Database];
            var username = config.GetValueOrDefault(TimescaleConnectorConfig.Username, "")!;
            var password = config.GetValueOrDefault(TimescaleConnectorConfig.Password, "")!;
            var sslMode = config.GetValueOrDefault(TimescaleConnectorConfig.SslMode,
                TimescaleConnectorConfig.DefaultSslMode)!;

            var builder = new NpgsqlConnectionStringBuilder
            {
                Host = host,
                Port = port,
                Database = database,
                Username = username,
                Password = password
            };

            builder.SslMode = sslMode.ToLowerInvariant() switch
            {
                "disable" => SslMode.Disable,
                "require" => SslMode.Require,
                _ => SslMode.Prefer
            };

            connectionString = builder.ConnectionString;
        }

        _dataSource = NpgsqlDataSource.Create(connectionString);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if ((DateTime.UtcNow - _lastPoll).TotalMilliseconds < _pollIntervalMs)
        {
            return [];
        }

        _lastPoll = DateTime.UtcNow;

        InitializeCursor();

        var records = new List<SourceRecord>();

        try
        {
            await using var conn = await _dataSource!.OpenConnectionAsync(cancellationToken);

            var sql = BuildQuery();
            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("lastTime", NpgsqlTypes.NpgsqlDbType.TimestampTz, _lastTimestamp);
            cmd.Parameters.AddWithValue("limit", _rowLimit);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                // Advance the cursor first so the record's offset covers its own row
                var timeOrdinal = reader.GetOrdinal(_timeColumn);
                if (!reader.IsDBNull(timeOrdinal))
                {
                    var rowTime = reader.GetDateTime(timeOrdinal);
                    if (rowTime > _lastTimestamp)
                    {
                        _lastTimestamp = rowTime;
                    }
                }

                var record = CreateRecord(reader);
                records.Add(record);
            }
        }
        catch (Exception ex)
        {
            // Surface the error; the rows read so far are still returned
            Context?.RaiseError?.Invoke(ex);
        }

        return records;
    }

    /// <summary>
    /// Positions the incremental cursor on the first poll: resume from the offset the previous
    /// run stored, and fall back to the configured lookback window for a fresh connector.
    /// </summary>
    internal void InitializeCursor()
    {
        if (_initialized)
        {
            return;
        }

        _lastTimestamp = DateTime.UtcNow.AddSeconds(-_lookbackSeconds);

        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null &&
            storedOffset.TryGetValue("last_time", out var lastTime) &&
            DateTime.TryParse(lastTime?.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var stored))
        {
            _lastTimestamp = stored;
        }

        _initialized = true;
    }

    internal string BuildQuery()
    {
        if (!string.IsNullOrWhiteSpace(_query))
        {
            // Inject time filter into custom query
            var sql = _query!;
            if (sql.Contains("WHERE", StringComparison.OrdinalIgnoreCase))
            {
                sql = sql.Replace("WHERE", $"WHERE {_timeColumn} > @lastTime AND", StringComparison.OrdinalIgnoreCase);
            }
            else if (sql.Contains("ORDER BY", StringComparison.OrdinalIgnoreCase))
            {
                sql = sql.Replace("ORDER BY", $"WHERE {_timeColumn} > @lastTime ORDER BY", StringComparison.OrdinalIgnoreCase);
            }
            else
            {
                sql += $" WHERE {_timeColumn} > @lastTime";
            }

            if (!sql.Contains("ORDER BY", StringComparison.OrdinalIgnoreCase))
            {
                sql += $" ORDER BY {_timeColumn} ASC";
            }

            if (!sql.Contains("LIMIT", StringComparison.OrdinalIgnoreCase))
            {
                sql += " LIMIT @limit";
            }

            return sql;
        }

        // Build query from table
        var columns = _columns != null && _columns.Length > 0
            ? string.Join(", ", _columns)
            : "*";

        return $@"
            SELECT {columns}
            FROM {_table}
            WHERE {_timeColumn} > @lastTime
            ORDER BY {_timeColumn} ASC
            LIMIT @limit";
    }

    private SourceRecord CreateRecord(NpgsqlDataReader reader)
    {
        var msgId = Interlocked.Increment(ref _messageId);
        var row = new Dictionary<string, object?>();

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            var value = reader.IsDBNull(i) ? null : reader.GetValue(i);

            // Handle special types
            if (value is NpgsqlTypes.NpgsqlInterval interval)
            {
                value = interval.ToString();
            }
            else if (value is byte[] bytes)
            {
                value = Convert.ToBase64String(bytes);
            }
            else if (value is DateTime dt)
            {
                value = dt.ToString("O");
            }
            else if (value is DateTimeOffset dto)
            {
                value = dto.ToString("O");
            }

            row[name] = value;
        }

        var payload = new
        {
            table = _table ?? "query",
            data = row,
            timestamp = DateTime.UtcNow
        };

        // Use time column as key
        string? keyStr = null;
        if (row.TryGetValue(_timeColumn, out var timeVal) && timeVal != null)
        {
            keyStr = timeVal.ToString();
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["last_time"] = _lastTimestamp.ToString("O")
            },
            Topic = _topic,
            Key = keyStr != null ? Encoding.UTF8.GetBytes(keyStr) : null,
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["timescale.table"] = Encoding.UTF8.GetBytes(_table ?? "query")
            }
        };
    }

    public override void Stop()
    {
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _dataSource?.Dispose();
        }
        base.Dispose(disposing);
    }
}
