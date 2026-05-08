namespace Kuestenlogik.Surgewave.Connector.PostgreSql;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Npgsql;
using Pgvector;

/// <summary>
/// Task that writes records to PostgreSQL using batch inserts or upserts.
/// </summary>
public sealed class PostgreSqlSinkTask : SinkTask
{
    private string _connectionString = "";
    private string _table = "";
    private string _schema = PostgreSqlConnectorConfig.DefaultSchema;
    private string _insertMode = PostgreSqlConnectorConfig.InsertModeInsert;
    private string _pkMode = PostgreSqlConnectorConfig.PkModeRecordKey;
    private string[] _pkFields = [];
    private int _batchSize = PostgreSqlConnectorConfig.DefaultBatchSize;
    private int _retryMax = PostgreSqlConnectorConfig.DefaultRetryMax;
    private long _retryBackoffMs = PostgreSqlConnectorConfig.DefaultRetryBackoffMs;
    private readonly List<SinkRecord> _buffer = [];

    // pgvector configuration
    private string _vectorField = "";
    private int _vectorDimensions = PostgreSqlConnectorConfig.DefaultVectorDimensions;
    private bool _vectorCreateExtension = true;
    private string _vectorIndexType = PostgreSqlConnectorConfig.VectorIndexNone;
    private string _vectorDistanceMetric = PostgreSqlConnectorConfig.VectorDistanceCosine;
    private bool _vectorExtensionEnsured;
    private NpgsqlDataSource? _dataSource;

    // Cached column information
    private List<string>? _columns;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _connectionString = config[PostgreSqlConnectorConfig.ConnectionConfig];
        _table = config[PostgreSqlConnectorConfig.TableConfig];
        _schema = GetConfigValue(config, PostgreSqlConnectorConfig.SchemaConfig, PostgreSqlConnectorConfig.DefaultSchema);
        _insertMode = GetConfigValue(config, PostgreSqlConnectorConfig.InsertModeConfig, PostgreSqlConnectorConfig.InsertModeInsert);
        _pkMode = GetConfigValue(config, PostgreSqlConnectorConfig.PkModeConfig, PostgreSqlConnectorConfig.PkModeRecordKey);
        _pkFields = GetConfigValue(config, PostgreSqlConnectorConfig.PkFieldsConfig, "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(f => f.Trim())
            .ToArray();
        _batchSize = int.Parse(GetConfigValue(config, PostgreSqlConnectorConfig.BatchSizeConfig, PostgreSqlConnectorConfig.DefaultBatchSize.ToString()));
        _retryMax = int.Parse(GetConfigValue(config, PostgreSqlConnectorConfig.RetryMaxConfig, PostgreSqlConnectorConfig.DefaultRetryMax.ToString()));
        _retryBackoffMs = long.Parse(GetConfigValue(config, PostgreSqlConnectorConfig.RetryBackoffMsConfig, PostgreSqlConnectorConfig.DefaultRetryBackoffMs.ToString()));

        // pgvector configuration
        _vectorField = GetConfigValue(config, PostgreSqlConnectorConfig.VectorFieldConfig, "");
        _vectorDimensions = int.Parse(GetConfigValue(config, PostgreSqlConnectorConfig.VectorDimensionsConfig, PostgreSqlConnectorConfig.DefaultVectorDimensions.ToString()));
        _vectorCreateExtension = bool.Parse(GetConfigValue(config, PostgreSqlConnectorConfig.VectorCreateExtensionConfig, "true"));
        _vectorIndexType = GetConfigValue(config, PostgreSqlConnectorConfig.VectorIndexTypeConfig, PostgreSqlConnectorConfig.VectorIndexNone);
        _vectorDistanceMetric = GetConfigValue(config, PostgreSqlConnectorConfig.VectorDistanceMetricConfig, PostgreSqlConnectorConfig.VectorDistanceCosine);

        // Create data source with pgvector type mapping if vector field is configured
        if (!string.IsNullOrEmpty(_vectorField))
        {
            var dataSourceBuilder = new NpgsqlDataSourceBuilder(_connectionString);
            dataSourceBuilder.UseVector();
            _dataSource = dataSourceBuilder.Build();
        }
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        FlushBuffer();
        _dataSource?.Dispose();
        _dataSource = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _buffer.Clear();
            _dataSource?.Dispose();
            _dataSource = null;
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        _buffer.AddRange(records);
        if (_buffer.Count >= _batchSize)
        {
            await FlushBufferAsync(cancellationToken);
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        await FlushBufferAsync(cancellationToken);
    }

    private void FlushBuffer() => FlushBufferAsync(CancellationToken.None).GetAwaiter().GetResult();

    private async Task FlushBufferAsync(CancellationToken cancellationToken)
    {
        if (_buffer.Count == 0)
            return;

        var attempt = 0;
        while (true)
        {
            try
            {
                // Use data source for vector operations, otherwise direct connection
                await using var conn = await GetConnectionAsync(cancellationToken);

                // Ensure pgvector extension if configured
                if (!_vectorExtensionEnsured && !string.IsNullOrEmpty(_vectorField) && _vectorCreateExtension)
                {
                    await EnsureVectorExtensionAsync(conn, cancellationToken);
                    _vectorExtensionEnsured = true;
                }

                if (_insertMode == PostgreSqlConnectorConfig.InsertModeUpsert && _pkFields.Length > 0)
                {
                    await UpsertRecordsAsync(conn, cancellationToken);
                }
                else
                {
                    await InsertRecordsAsync(conn, cancellationToken);
                }

                _buffer.Clear();
                return;
            }
            catch (NpgsqlException) when (attempt < _retryMax)
            {
                attempt++;
                await Task.Delay((int)(_retryBackoffMs * Math.Pow(2, attempt - 1)), cancellationToken);
            }
        }
    }

    private async Task<NpgsqlConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_dataSource != null)
        {
            return await _dataSource.OpenConnectionAsync(cancellationToken);
        }

        var conn = new NpgsqlConnection(_connectionString);
        try
        {
            await conn.OpenAsync(cancellationToken);
            return conn;
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    private async Task EnsureVectorExtensionAsync(NpgsqlConnection conn, CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS vector", conn);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task InsertRecordsAsync(NpgsqlConnection conn, CancellationToken cancellationToken)
    {
        // Get columns from first record
        var columns = GetColumnsFromBuffer();
        if (columns.Count == 0)
            return;

        // Build parameterized INSERT statement
        var columnList = string.Join(", ", columns.Select(c => $"\"{c}\""));
        var paramList = string.Join(", ", columns.Select((_, i) => $"@p{i}"));
        var sql = $"INSERT INTO \"{_schema}\".\"{_table}\" ({columnList}) VALUES ({paramList})";

        await using var batch = new NpgsqlBatch(conn);

        foreach (var record in _buffer)
        {
            var values = ParseRecordValues(record);
            if (values == null)
                continue;

            var cmd = new NpgsqlBatchCommand(sql);
            for (var i = 0; i < columns.Count; i++)
            {
                var col = columns[i];
                cmd.Parameters.AddWithValue($"p{i}", values.TryGetValue(col, out var val) ? GetNpgsqlValue(col, val) : DBNull.Value);
            }
            batch.BatchCommands.Add(cmd);
        }

        if (batch.BatchCommands.Count > 0)
        {
            await batch.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task UpsertRecordsAsync(NpgsqlConnection conn, CancellationToken cancellationToken)
    {
        // Get columns from first record
        var columns = GetColumnsFromBuffer();
        if (columns.Count == 0)
            return;

        var nonPkCols = columns.Except(_pkFields).ToList();

        // Build parameterized UPSERT statement
        var columnList = string.Join(", ", columns.Select(c => $"\"{c}\""));
        var paramList = string.Join(", ", columns.Select((_, i) => $"@p{i}"));
        var pkList = string.Join(", ", _pkFields.Select(pk => $"\"{pk}\""));

        string sql;
        if (nonPkCols.Count > 0)
        {
            var updateList = string.Join(", ", nonPkCols.Select(c => $"\"{c}\" = EXCLUDED.\"{c}\""));
            sql = $"INSERT INTO \"{_schema}\".\"{_table}\" ({columnList}) VALUES ({paramList}) ON CONFLICT ({pkList}) DO UPDATE SET {updateList}";
        }
        else
        {
            // All columns are PKs, just do nothing on conflict
            sql = $"INSERT INTO \"{_schema}\".\"{_table}\" ({columnList}) VALUES ({paramList}) ON CONFLICT ({pkList}) DO NOTHING";
        }

        await using var batch = new NpgsqlBatch(conn);

        foreach (var record in _buffer)
        {
            var values = ParseRecordValues(record);
            if (values == null)
                continue;

            var cmd = new NpgsqlBatchCommand(sql);
            for (var i = 0; i < columns.Count; i++)
            {
                var col = columns[i];
                cmd.Parameters.AddWithValue($"p{i}", values.TryGetValue(col, out var val) ? GetNpgsqlValue(col, val) : DBNull.Value);
            }
            batch.BatchCommands.Add(cmd);
        }

        if (batch.BatchCommands.Count > 0)
        {
            await batch.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private List<string> GetColumnsFromBuffer()
    {
        if (_columns != null)
            return _columns;

        var firstRecord = _buffer.FirstOrDefault();
        if (firstRecord == null)
            return [];

        var values = ParseRecordValues(firstRecord);
        if (values == null)
            return [];

        _columns = [.. values.Keys];
        return _columns;
    }

    private Dictionary<string, JsonElement>? ParseRecordValues(SinkRecord record)
    {
        try
        {
            return JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(record.Value);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private object GetNpgsqlValue(string columnName, JsonElement element)
    {
        // Handle vector field specially
        if (!string.IsNullOrEmpty(_vectorField) && string.Equals(columnName, _vectorField, StringComparison.OrdinalIgnoreCase))
        {
            if (element.ValueKind == JsonValueKind.Array)
            {
                var floats = new float[element.GetArrayLength()];
                var idx = 0;
                foreach (var item in element.EnumerateArray())
                {
                    floats[idx++] = item.GetSingle();
                }
                return new Vector(floats);
            }
            return DBNull.Value;
        }

        return element.ValueKind switch
        {
            JsonValueKind.Null => DBNull.Value,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number when element.TryGetInt32(out var i) => i,
            JsonValueKind.Number when element.TryGetInt64(out var l) => l,
            JsonValueKind.Number when element.TryGetDouble(out var d) => d,
            JsonValueKind.Number => element.GetDecimal(),
            JsonValueKind.String when DateTime.TryParse(element.GetString(), out var dt) => dt,
            JsonValueKind.String when Guid.TryParse(element.GetString(), out var g) => g,
            JsonValueKind.String => element.GetString() ?? "",
            JsonValueKind.Array => element.GetRawText(),
            JsonValueKind.Object => element.GetRawText(),
            _ => DBNull.Value
        };
    }
}
