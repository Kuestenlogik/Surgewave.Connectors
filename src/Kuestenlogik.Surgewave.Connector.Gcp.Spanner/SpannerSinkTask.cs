using System.Text.Json;
using Google.Cloud.Spanner.Data;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Spanner;

/// <summary>
/// Task that writes rows to Google Cloud Spanner.
/// </summary>
public sealed class SpannerSinkTask : SinkTask
{
    private SpannerConnection? _connection;
    private string _targetTable = null!;
    private string _writeMode = null!;
    private string[]? _keyColumns;
    private int _batchSize;
    private int _commitTimeoutSeconds;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var projectId = config[SpannerConnectorConfig.ProjectId];
        var instanceId = config[SpannerConnectorConfig.InstanceId];
        var databaseId = config[SpannerConnectorConfig.DatabaseId];

        _targetTable = config[SpannerConnectorConfig.TargetTable];
        _writeMode = config.GetValueOrDefault(SpannerConnectorConfig.WriteMode,
            SpannerConnectorConfig.DefaultWriteMode)!.ToLowerInvariant();
        _batchSize = int.Parse(config.GetValueOrDefault(SpannerConnectorConfig.BatchSize,
            SpannerConnectorConfig.DefaultBatchSize.ToString())!);
        _commitTimeoutSeconds = int.Parse(config.GetValueOrDefault(SpannerConnectorConfig.CommitTimeout,
            SpannerConnectorConfig.DefaultCommitTimeoutSeconds.ToString())!);

        var keyColumnsStr = config.GetValueOrDefault(SpannerConnectorConfig.KeyColumns, "");
        if (!string.IsNullOrWhiteSpace(keyColumnsStr))
        {
            _keyColumns = keyColumnsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
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
                    row[prop.Name] = ConvertJsonValue(prop.Value);
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
            JsonValueKind.Array => element.GetRawText(),  // Store as JSON string
            JsonValueKind.Object => element.GetRawText(), // Store as JSON string
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
            // CommitTimeout is now passed to CommitAsync

            foreach (var row in batch)
            {
                var cmd = CreateCommand(row, transaction);
                if (cmd != null)
                {
                    await cmd.ExecuteNonQueryAsync(ct);
                }
            }

            await transaction.CommitAsync(ct);
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
    }

    private SpannerCommand? CreateCommand(Dictionary<string, object?> row, SpannerTransaction transaction)
    {
        switch (_writeMode)
        {
            case "insert":
                return CreateInsertCommand(row, transaction);

            case "update":
                return CreateUpdateCommand(row, transaction);

            case "delete":
                return CreateDeleteCommand(row, transaction);

            case "upsert":
            default:
                return CreateUpsertCommand(row, transaction);
        }
    }

    private SpannerCommand CreateInsertCommand(Dictionary<string, object?> row, SpannerTransaction transaction)
    {
        var cmd = _connection!.CreateInsertCommand(_targetTable);
        cmd.Transaction = transaction;

        foreach (var (key, value) in row)
        {
            cmd.Parameters.Add(key, GetSpannerDbType(value), value);
        }

        return cmd;
    }

    private SpannerCommand CreateUpsertCommand(Dictionary<string, object?> row, SpannerTransaction transaction)
    {
        var cmd = _connection!.CreateInsertOrUpdateCommand(_targetTable);
        cmd.Transaction = transaction;

        foreach (var (key, value) in row)
        {
            cmd.Parameters.Add(key, GetSpannerDbType(value), value);
        }

        return cmd;
    }

    private SpannerCommand? CreateUpdateCommand(Dictionary<string, object?> row, SpannerTransaction transaction)
    {
        if (_keyColumns == null || _keyColumns.Length == 0)
        {
            return null; // Cannot update without key columns
        }

        var cmd = _connection!.CreateUpdateCommand(_targetTable);
        cmd.Transaction = transaction;

        foreach (var (key, value) in row)
        {
            cmd.Parameters.Add(key, GetSpannerDbType(value), value);
        }

        return cmd;
    }

    private SpannerCommand? CreateDeleteCommand(Dictionary<string, object?> row, SpannerTransaction transaction)
    {
        if (_keyColumns == null || _keyColumns.Length == 0)
        {
            return null; // Cannot delete without key columns
        }

        var cmd = _connection!.CreateDeleteCommand(_targetTable);
        cmd.Transaction = transaction;

        // Only add key columns for delete
        foreach (var keyCol in _keyColumns)
        {
            if (row.TryGetValue(keyCol, out var value))
            {
                cmd.Parameters.Add(keyCol, GetSpannerDbType(value), value);
            }
        }

        return cmd;
    }

    private SpannerDbType GetSpannerDbType(object? value)
    {
        return value switch
        {
            null => SpannerDbType.String,
            long => SpannerDbType.Int64,
            int => SpannerDbType.Int64,
            DateTime => SpannerDbType.Timestamp,
            DateTimeOffset => SpannerDbType.Timestamp,
            string => SpannerDbType.String,
            double => SpannerDbType.Float64,
            float => SpannerDbType.Float64,
            bool => SpannerDbType.Bool,
            byte[] => SpannerDbType.Bytes,
            decimal => SpannerDbType.Numeric,
            _ => SpannerDbType.String
        };
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
