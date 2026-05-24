// Uncomment the following line when SAP HANA client is installed:
// #define SAP_HANA_AVAILABLE

using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
#if SAP_HANA_AVAILABLE
using Sap.Data.Hana;
#else
using System.Data.Common;
#endif
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sap.Hana;

/// <summary>
/// Task that reads data from SAP HANA.
/// Requires SAP HANA client installation and Sap.Data.Hana.Core.v2.1 package.
/// </summary>
[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Table/column names from configuration, not user input")]
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "DbConnection used for conditional compilation compatibility")]
public sealed class HanaSourceTask : SourceTask
{
#if SAP_HANA_AVAILABLE
    private HanaConnection? _connection;
#else
    private DbConnection? _connection;
#endif
    private string _connectionString = null!;
    private string _topic = null!;
    private string? _query;
    private string? _table;
    private string? _schema;
    private string[]? _columns;
    private string? _incrementalColumn;
    private int _pollIntervalMs;
    private int _rowLimit;
    private DateTime _lastPoll = DateTime.MinValue;
    private object? _lastIncrementalValue;
    private long _messageId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[HanaConnectorConfig.Topic];
        _query = config.GetValueOrDefault(HanaConnectorConfig.Query, null);
        _table = config.GetValueOrDefault(HanaConnectorConfig.Table, null);
        _schema = config.GetValueOrDefault(HanaConnectorConfig.Schema, null);
        _incrementalColumn = config.GetValueOrDefault(HanaConnectorConfig.IncrementalColumn, null);

        _pollIntervalMs = int.Parse(config.GetValueOrDefault(HanaConnectorConfig.PollIntervalMs,
            HanaConnectorConfig.DefaultPollIntervalMs.ToString())!);
        _rowLimit = int.Parse(config.GetValueOrDefault(HanaConnectorConfig.RowLimit,
            HanaConnectorConfig.DefaultRowLimit.ToString())!);

        var columnsStr = config.GetValueOrDefault(HanaConnectorConfig.Columns, "");
        if (!string.IsNullOrWhiteSpace(columnsStr))
        {
            _columns = columnsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        _connectionString = BuildConnectionString(config);
#if SAP_HANA_AVAILABLE
        _connection = new HanaConnection(_connectionString);
#else
        throw new NotSupportedException("SAP HANA driver not installed. Install the SAP HANA client and add the Sap.Data.Hana.Core.v2.1 package.");
#endif
    }

    private string BuildConnectionString(IDictionary<string, string> config)
    {
        var connStr = config.GetValueOrDefault(HanaConnectorConfig.ConnectionString, "");
        if (!string.IsNullOrWhiteSpace(connStr))
        {
            return connStr;
        }

        var host = config.GetValueOrDefault(HanaConnectorConfig.Host, "")!;
        var port = int.Parse(config.GetValueOrDefault(HanaConnectorConfig.Port,
            HanaConnectorConfig.DefaultPort.ToString())!);
        var database = config.GetValueOrDefault(HanaConnectorConfig.Database, "");
        var username = config.GetValueOrDefault(HanaConnectorConfig.Username, "")!;
        var password = config.GetValueOrDefault(HanaConnectorConfig.Password, "")!;
        var useSsl = config.GetValueOrDefault(HanaConnectorConfig.UseSsl, "true") == "true";
        var validateCert = config.GetValueOrDefault(HanaConnectorConfig.ValidateCertificate, "true") == "true";

#if SAP_HANA_AVAILABLE
        var builder = new HanaConnectionStringBuilder
        {
            Server = $"{host}:{port}",
            UserName = username,
            Password = password
        };

        if (!string.IsNullOrWhiteSpace(database))
        {
            builder.DatabaseName = database;
        }

        if (useSsl)
        {
            builder.Encrypt = "TRUE";
            if (!validateCert)
            {
                builder.ValidateCertificate = "FALSE";
            }
        }

        return builder.ConnectionString;
#else
        // Build a simple connection string when SAP driver not available
        return $"Server={host}:{port};UserName={username};Password={password};Database={database}";
#endif
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

            using var cmd = _connection.CreateCommand();
            cmd.CommandText = BuildQuery();

            if (_lastIncrementalValue != null && !string.IsNullOrEmpty(_incrementalColumn))
            {
                var param = cmd.CreateParameter();
                param.ParameterName = "lastValue";
                param.Value = _lastIncrementalValue;
                cmd.Parameters.Add(param);
            }

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                var record = CreateRecord(reader);
                records.Add(record);

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
            if (_connection?.State == ConnectionState.Open)
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

            if (_lastIncrementalValue != null && !string.IsNullOrEmpty(_incrementalColumn))
            {
                if (sql.Contains("WHERE", StringComparison.OrdinalIgnoreCase))
                {
                    sql += $" AND \"{_incrementalColumn}\" > :lastValue";
                }
                else
                {
                    sql += $" WHERE \"{_incrementalColumn}\" > :lastValue";
                }
            }

            if (!sql.Contains("LIMIT", StringComparison.OrdinalIgnoreCase))
            {
                sql += $" LIMIT {_rowLimit}";
            }

            return sql;
        }

        var columns = _columns != null && _columns.Length > 0
            ? string.Join(", ", _columns.Select(c => $"\"{c}\""))
            : "*";

        var tableName = !string.IsNullOrEmpty(_schema)
            ? $"\"{_schema}\".\"{_table}\""
            : $"\"{_table}\"";

        var query = $"SELECT {columns} FROM {tableName}";

        if (_lastIncrementalValue != null && !string.IsNullOrEmpty(_incrementalColumn))
        {
            query += $" WHERE \"{_incrementalColumn}\" > :lastValue";
        }

        if (!string.IsNullOrEmpty(_incrementalColumn))
        {
            query += $" ORDER BY \"{_incrementalColumn}\"";
        }

        query += $" LIMIT {_rowLimit}";

        return query;
    }

    private SourceRecord CreateRecord(IDataReader reader)
    {
        var msgId = Interlocked.Increment(ref _messageId);
        var row = new Dictionary<string, object?>();

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            var value = reader.IsDBNull(i) ? null : reader.GetValue(i);

            if (value is byte[] bytes)
            {
                value = Convert.ToBase64String(bytes);
            }
            else if (value is DateTime dt)
            {
                value = dt.ToString("O");
            }

            row[name] = value;
        }

        var payload = new
        {
            table = _table ?? "query",
            schema = _schema,
            data = row,
            timestamp = DateTime.UtcNow
        };

        string? keyStr = null;
        if (reader.FieldCount > 0 && !reader.IsDBNull(0))
        {
            keyStr = reader.GetValue(0)?.ToString();
        }

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["source"] = "hana",
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
                ["hana.table"] = Encoding.UTF8.GetBytes(_table ?? "query")
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
