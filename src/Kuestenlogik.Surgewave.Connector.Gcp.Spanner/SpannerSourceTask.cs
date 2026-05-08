using System.Text;
using System.Text.Json;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Spanner;

/// <summary>
/// Task that reads rows from Google Cloud Spanner.
/// </summary>
public sealed class SpannerSourceTask : SourceTask
{
    private SpannerConnection? _connection;
    private string _topic = null!;
    private string? _query;
    private string? _table;
    private string[]? _columns;
    private string? _incrementalColumn;
    private int _pollIntervalMs;
    private int _rowLimit;
    private string _timestampBound = null!;
    private int _maxStalenessSeconds;
    private DateTime _lastPoll = DateTime.MinValue;
    private object? _lastIncrementalValue;
    private long _messageId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var projectId = config[SpannerConnectorConfig.ProjectId];
        var instanceId = config[SpannerConnectorConfig.InstanceId];
        var databaseId = config[SpannerConnectorConfig.DatabaseId];
        _topic = config[SpannerConnectorConfig.Topic];

        _query = config.GetValueOrDefault(SpannerConnectorConfig.Query, null);
        _table = config.GetValueOrDefault(SpannerConnectorConfig.Table, null);
        _incrementalColumn = config.GetValueOrDefault(SpannerConnectorConfig.IncrementalColumn, null);

        _pollIntervalMs = int.Parse(config.GetValueOrDefault(SpannerConnectorConfig.PollIntervalMs,
            SpannerConnectorConfig.DefaultPollIntervalMs.ToString())!);
        _rowLimit = int.Parse(config.GetValueOrDefault(SpannerConnectorConfig.RowLimit,
            SpannerConnectorConfig.DefaultRowLimit.ToString())!);
        _timestampBound = config.GetValueOrDefault(SpannerConnectorConfig.TimestampBound,
            SpannerConnectorConfig.DefaultTimestampBound)!;
        _maxStalenessSeconds = int.Parse(config.GetValueOrDefault(SpannerConnectorConfig.MaxStalenessSeconds,
            SpannerConnectorConfig.DefaultMaxStalenessSeconds.ToString())!);

        var columnsStr = config.GetValueOrDefault(SpannerConnectorConfig.Columns, "");
        if (!string.IsNullOrWhiteSpace(columnsStr))
        {
            _columns = columnsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        // Build connection string
        var connectionStringBuilder = new SpannerConnectionStringBuilder
        {
            DataSource = $"projects/{projectId}/instances/{instanceId}/databases/{databaseId}"
        };

        var emulatorHost = config.GetValueOrDefault(SpannerConnectorConfig.EmulatorHost, null);
        if (!string.IsNullOrWhiteSpace(emulatorHost))
        {
            connectionStringBuilder.EmulatorDetection = Google.Api.Gax.EmulatorDetection.EmulatorOnly;
            Environment.SetEnvironmentVariable("SPANNER_EMULATOR_HOST", emulatorHost);
        }
        else
        {
            var credentialsJson = config.GetValueOrDefault(SpannerConnectorConfig.CredentialsJson, null);
            var credentialsFile = config.GetValueOrDefault(SpannerConnectorConfig.CredentialsFile, null);

            if (!string.IsNullOrWhiteSpace(credentialsJson))
            {
                // Write credentials to temp file for Spanner client
                var tempFile = Path.GetTempFileName();
                File.WriteAllText(tempFile, credentialsJson);
                Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", tempFile);
            }
            else if (!string.IsNullOrWhiteSpace(credentialsFile))
            {
                Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", credentialsFile);
            }
        }

        _connection = new SpannerConnection(connectionStringBuilder);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if ((DateTime.UtcNow - _lastPoll).TotalMilliseconds < _pollIntervalMs)
        {
            return [];
        }

        _lastPoll = DateTime.UtcNow;
        var records = new List<SourceRecord>();

        try
        {
            await _connection!.OpenAsync(cancellationToken);

            var sql = BuildQuery();
            using var cmd = _connection.CreateSelectCommand(sql);

            // Add incremental parameter if needed
            if (_lastIncrementalValue != null && !string.IsNullOrEmpty(_incrementalColumn))
            {
                cmd.Parameters.Add("lastValue", GetSpannerDbType(_lastIncrementalValue), _lastIncrementalValue);
            }

            // Set timestamp bound
            var txnOptions = GetTimestampBoundOptions();

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                var record = CreateRecord(reader);
                records.Add(record);

                // Track incremental value
                if (!string.IsNullOrEmpty(_incrementalColumn))
                {
                    var ordinal = reader.GetOrdinal(_incrementalColumn);
                    if (!reader.IsDBNull(ordinal))
                    {
                        _lastIncrementalValue = reader.GetValue(ordinal);
                    }
                }

                if (records.Count >= _rowLimit) break;
            }
        }
        catch (Exception)
        {
            // Log and continue
        }
        finally
        {
            if (_connection?.State == System.Data.ConnectionState.Open)
            {
                await _connection.CloseAsync();
            }
        }

        return records;
    }

    private string BuildQuery()
    {
        if (!string.IsNullOrWhiteSpace(_query))
        {
            var sql = _query!;

            // Add incremental filter if specified
            if (_lastIncrementalValue != null && !string.IsNullOrEmpty(_incrementalColumn))
            {
                if (sql.Contains("WHERE", StringComparison.OrdinalIgnoreCase))
                {
                    sql += $" AND {_incrementalColumn} > @lastValue";
                }
                else
                {
                    sql += $" WHERE {_incrementalColumn} > @lastValue";
                }
            }

            // Add limit
            if (!sql.Contains("LIMIT", StringComparison.OrdinalIgnoreCase))
            {
                sql += $" LIMIT {_rowLimit}";
            }

            return sql;
        }

        // Build query from table
        var columns = _columns != null && _columns.Length > 0
            ? string.Join(", ", _columns)
            : "*";

        var query = $"SELECT {columns} FROM {_table}";

        if (_lastIncrementalValue != null && !string.IsNullOrEmpty(_incrementalColumn))
        {
            query += $" WHERE {_incrementalColumn} > @lastValue";
        }

        if (!string.IsNullOrEmpty(_incrementalColumn))
        {
            query += $" ORDER BY {_incrementalColumn}";
        }

        query += $" LIMIT {_rowLimit}";

        return query;
    }

    private SpannerDbType GetSpannerDbType(object value)
    {
        return value switch
        {
            long => SpannerDbType.Int64,
            int => SpannerDbType.Int64,
            DateTime => SpannerDbType.Timestamp,
            DateTimeOffset => SpannerDbType.Timestamp,
            string => SpannerDbType.String,
            double => SpannerDbType.Float64,
            float => SpannerDbType.Float64,
            bool => SpannerDbType.Bool,
            byte[] => SpannerDbType.Bytes,
            _ => SpannerDbType.String
        };
    }

    private TimestampBoundMode GetTimestampBoundOptions()
    {
        return _timestampBound.ToLowerInvariant() switch
        {
            "exact" => TimestampBoundMode.Exact,
            "bounded_staleness" => TimestampBoundMode.BoundedStaleness,
            _ => TimestampBoundMode.Strong
        };
    }

    private SourceRecord CreateRecord(SpannerDataReader reader)
    {
        var msgId = Interlocked.Increment(ref _messageId);
        var row = new Dictionary<string, object?>();

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            var value = reader.IsDBNull(i) ? null : reader.GetValue(i);

            // Convert Spanner-specific types
            if (value is SpannerNumeric numeric)
            {
                value = numeric.ToDecimal(LossOfPrecisionHandling.Truncate);
            }
            else if (value is byte[] bytes)
            {
                value = Convert.ToBase64String(bytes);
            }

            row[name] = value;
        }

        var payload = new
        {
            table = _table ?? "query",
            data = row,
            timestamp = DateTime.UtcNow
        };

        // Use first column as key if available
        string? keyStr = null;
        if (reader.FieldCount > 0 && !reader.IsDBNull(0))
        {
            keyStr = reader.GetValue(0)?.ToString();
        }

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["source"] = "spanner",
                ["table"] = _table ?? "query"
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["incremental_value"] = _lastIncrementalValue?.ToString() ?? ""
            },
            Topic = _topic,
            Key = keyStr != null ? Encoding.UTF8.GetBytes(keyStr) : null,
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["spanner.table"] = Encoding.UTF8.GetBytes(_table ?? "query")
            }
        };
    }

    public override void Stop()
    {
        _connection?.Close();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _connection?.Dispose();
        }
        base.Dispose(disposing);
    }
}

internal enum TimestampBoundMode
{
    Strong,
    Exact,
    BoundedStaleness
}
