// Uncomment the following line when SAP HANA client is installed:
// #define SAP_HANA_AVAILABLE

using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
#if SAP_HANA_AVAILABLE
using Sap.Data.Hana;
#else
using System.Data.Common;
#endif
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sap.Hana;

/// <summary>
/// Task that writes data to SAP HANA.
/// Requires SAP HANA client installation and Sap.Data.Hana.Core.v2.1 package.
/// </summary>
[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Table/column names from configuration, not user input")]
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "DbConnection used for conditional compilation compatibility")]
public sealed class HanaSinkTask : SinkTask
{
#if SAP_HANA_AVAILABLE
    private HanaConnection? _connection;
#else
    private DbConnection? _connection;
#endif
    private string _connectionString = null!;
    private string _targetTable = null!;
    private string? _schema;
    private string _writeMode = null!;
    private string[]? _keyColumns;
    private int _batchSize;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _targetTable = config[HanaConnectorConfig.TargetTable];
        _schema = config.GetValueOrDefault(HanaConnectorConfig.Schema, null);
        _writeMode = config.GetValueOrDefault(HanaConnectorConfig.WriteMode,
            HanaConnectorConfig.DefaultWriteMode)!.ToLowerInvariant();
        _batchSize = int.Parse(config.GetValueOrDefault(HanaConnectorConfig.BatchSize,
            HanaConnectorConfig.DefaultBatchSize.ToString())!);

        var keyColumnsStr = config.GetValueOrDefault(HanaConnectorConfig.KeyColumns, "");
        if (!string.IsNullOrWhiteSpace(keyColumnsStr))
        {
            _keyColumns = keyColumnsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
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

                var dataElement = root.TryGetProperty("data", out var data) ? data : root;

                var row = new Dictionary<string, object?>();
                foreach (var prop in dataElement.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
                }

                if (row.Count > 0)
                {
                    batch.Add(row);
                }

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

        if (batch.Count > 0)
        {
            await FlushBatchAsync(batch, cancellationToken);
        }
    }

    private object? ConvertJsonValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number when element.TryGetInt64(out var l) => l,
            JsonValueKind.Number => element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => element.GetRawText()
        };
    }

    private async Task FlushBatchAsync(List<Dictionary<string, object?>> batch, CancellationToken ct)
    {
        if (batch.Count == 0) return;

        try
        {
            await _connection!.OpenAsync(ct);

            using var transaction = await _connection.BeginTransactionAsync(ct);

            foreach (var row in batch)
            {
                using var cmd = _connection.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandText = BuildCommand(row, cmd);

                await cmd.ExecuteNonQueryAsync(ct);
            }

            await transaction.CommitAsync(ct);
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
    }

#if SAP_HANA_AVAILABLE
    private string BuildCommand(Dictionary<string, object?> row, HanaCommand cmd)
#else
    private string BuildCommand(Dictionary<string, object?> row, DbCommand cmd)
#endif
    {
        var tableName = !string.IsNullOrEmpty(_schema)
            ? $"\"{_schema}\".\"{_targetTable}\""
            : $"\"{_targetTable}\"";

        var columns = row.Keys.ToList();
        var columnList = string.Join(", ", columns.Select(c => $"\"{c}\""));
        var paramList = string.Join(", ", columns.Select((_, i) => $":p{i}"));

        // Add parameters
        for (var i = 0; i < columns.Count; i++)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = $"p{i}";
            param.Value = row[columns[i]] ?? DBNull.Value;
            cmd.Parameters.Add(param);
        }

        switch (_writeMode)
        {
            case "upsert":
            case "merge":
                return BuildMergeCommand(tableName, columns, columnList, paramList);

            case "insert":
            default:
                return $"INSERT INTO {tableName} ({columnList}) VALUES ({paramList})";
        }
    }

    private string BuildMergeCommand(string tableName, List<string> columns, string columnList, string paramList)
    {
        if (_keyColumns == null || _keyColumns.Length == 0)
        {
            return $"INSERT INTO {tableName} ({columnList}) VALUES ({paramList})";
        }

        var keyConditions = string.Join(" AND ", _keyColumns.Select(k =>
        {
            var idx = columns.IndexOf(k);
            return $"t.\"{k}\" = :p{idx}";
        }));

        var updateCols = columns.Where(c => !_keyColumns.Contains(c, StringComparer.OrdinalIgnoreCase)).ToList();
        var updates = string.Join(", ", updateCols.Select(c =>
        {
            var idx = columns.IndexOf(c);
            return $"t.\"{c}\" = :p{idx}";
        }));

        return $@"
            MERGE INTO {tableName} AS t
            USING (SELECT 1 FROM DUMMY) AS s
            ON {keyConditions}
            WHEN MATCHED THEN UPDATE SET {updates}
            WHEN NOT MATCHED THEN INSERT ({columnList}) VALUES ({paramList})";
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
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
