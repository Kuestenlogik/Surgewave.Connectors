using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.TimescaleDB;

/// <summary>
/// Task that writes time-series data to TimescaleDB.
/// </summary>
[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Table/column names from configuration, not user input; SQL does not support parameterized identifiers")]
public sealed class TimescaleSinkTask : SinkTask
{
    private NpgsqlDataSource? _dataSource;
    private string _targetTable = null!;
    private string _timeColumnField = null!;
    private string _insertMode = null!;
    private string[]? _conflictColumns;
    private int _batchSize;
    private string[]? _columnOrder;
    private bool _schemaDetected;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _targetTable = config[TimescaleConnectorConfig.TargetTable];
        _timeColumnField = config.GetValueOrDefault(TimescaleConnectorConfig.TimeColumnField, "time")!;
        _insertMode = config.GetValueOrDefault(TimescaleConnectorConfig.InsertMode,
            TimescaleConnectorConfig.DefaultInsertMode)!.ToLowerInvariant();
        _batchSize = int.Parse(config.GetValueOrDefault(TimescaleConnectorConfig.BatchSize,
            TimescaleConnectorConfig.DefaultBatchSize.ToString())!);

        var conflictStr = config.GetValueOrDefault(TimescaleConnectorConfig.ConflictColumns, "");
        if (!string.IsNullOrWhiteSpace(conflictStr))
        {
            _conflictColumns = conflictStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
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

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        var batch = new List<Dictionary<string, object?>>();

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                using var doc = JsonDocument.Parse(record.Value);
                var root = doc.RootElement;

                // Handle nested data structure
                var dataElement = root.TryGetProperty("data", out var data) ? data : root;

                var row = new Dictionary<string, object?>();
                foreach (var prop in dataElement.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value, prop.Name);
                }

                if (row.Count > 0)
                {
                    batch.Add(row);
                }

                // Flush batch if full
                if (batch.Count >= _batchSize)
                {
                    await FlushBatchAsync(batch, cancellationToken);
                    batch.Clear();
                }
            }
            catch (Exception)
            {
                // Log and continue
            }
        }

        // Flush remaining
        if (batch.Count > 0)
        {
            await FlushBatchAsync(batch, cancellationToken);
        }
    }

    private object? ConvertJsonValue(JsonElement element, string columnName)
    {
        // Handle time column specially
        if (columnName == _timeColumnField)
        {
            if (element.ValueKind == JsonValueKind.String)
            {
                if (DateTime.TryParse(element.GetString(), out var dt))
                {
                    return dt.ToUniversalTime();
                }
                return element.GetString();
            }
            if (element.ValueKind == JsonValueKind.Number)
            {
                // Unix timestamp
                var ts = element.GetInt64();
                return DateTimeOffset.FromUnixTimeMilliseconds(ts).UtcDateTime;
            }
        }

        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number when element.TryGetInt64(out var l) => l,
            JsonValueKind.Number => element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            JsonValueKind.Array => element.GetRawText(),  // Store as JSONB
            JsonValueKind.Object => element.GetRawText(), // Store as JSONB
            _ => element.GetRawText()
        };
    }

    private async Task FlushBatchAsync(List<Dictionary<string, object?>> batch, CancellationToken ct)
    {
        if (batch.Count == 0) return;

        try
        {
            await using var conn = await _dataSource!.OpenConnectionAsync(ct);

            // Detect schema on first batch
            if (!_schemaDetected)
            {
                await DetectSchemaAsync(conn, batch[0].Keys, ct);
                _schemaDetected = true;
            }

            // Use COPY for high-performance bulk insert
            if (_insertMode == "insert")
            {
                await BulkCopyAsync(conn, batch, ct);
            }
            else
            {
                await UpsertAsync(conn, batch, ct);
            }
        }
        catch (Exception)
        {
            // Log and continue
        }
    }

    private async Task DetectSchemaAsync(NpgsqlConnection conn, IEnumerable<string> columns, CancellationToken ct)
    {
        // Get column order from table schema
        var sql = @"
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = @table
            ORDER BY ordinal_position";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("table", _targetTable);

        var tableColumns = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            tableColumns.Add(reader.GetString(0));
        }

        // Filter to columns we have data for
        _columnOrder = tableColumns.Where(c => columns.Contains(c, StringComparer.OrdinalIgnoreCase)).ToArray();
    }

    private async Task BulkCopyAsync(NpgsqlConnection conn, List<Dictionary<string, object?>> batch, CancellationToken ct)
    {
        if (_columnOrder == null || _columnOrder.Length == 0) return;

        var columns = string.Join(", ", _columnOrder);
        var copyCommand = $"COPY {_targetTable} ({columns}) FROM STDIN (FORMAT BINARY)";

        await using var writer = await conn.BeginBinaryImportAsync(copyCommand, ct);

        foreach (var row in batch)
        {
            await writer.StartRowAsync(ct);
            foreach (var col in _columnOrder)
            {
                row.TryGetValue(col, out var value);
                if (value == null)
                {
                    await writer.WriteNullAsync(ct);
                }
                else
                {
                    await writer.WriteAsync(value, ct);
                }
            }
        }

        await writer.CompleteAsync(ct);
    }

    private async Task UpsertAsync(NpgsqlConnection conn, List<Dictionary<string, object?>> batch, CancellationToken ct)
    {
        if (_columnOrder == null || _columnOrder.Length == 0) return;

        var columns = string.Join(", ", _columnOrder);
        var values = string.Join(", ", _columnOrder.Select((_, i) => $"@p{i}"));
        var updates = string.Join(", ", _columnOrder.Where(c => !IsConflictColumn(c))
            .Select(c => $"{c} = EXCLUDED.{c}"));

        var conflictCols = _conflictColumns != null && _conflictColumns.Length > 0
            ? string.Join(", ", _conflictColumns)
            : _timeColumnField;

        var sql = $@"
            INSERT INTO {_targetTable} ({columns})
            VALUES ({values})
            ON CONFLICT ({conflictCols}) DO UPDATE SET {updates}";

        foreach (var row in batch)
        {
            await using var cmd = new NpgsqlCommand(sql, conn);

            for (var i = 0; i < _columnOrder.Length; i++)
            {
                row.TryGetValue(_columnOrder[i], out var value);
                cmd.Parameters.AddWithValue($"p{i}", value ?? DBNull.Value);
            }

            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    private bool IsConflictColumn(string column)
    {
        if (_conflictColumns == null || _conflictColumns.Length == 0)
        {
            return column.Equals(_timeColumnField, StringComparison.OrdinalIgnoreCase);
        }
        return _conflictColumns.Any(c => c.Equals(column, StringComparison.OrdinalIgnoreCase));
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
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
