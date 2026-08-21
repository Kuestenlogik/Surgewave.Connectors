using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.BigQuery;

/// <summary>
/// Task that reads data from BigQuery tables or custom queries.
/// Supports incremental polling with timestamp-based tracking.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "BigQueryClient is stateless")]
public sealed class BigQuerySourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private BigQueryClient? _client;
    private string _projectId = "";
    private string _dataset = "";
    private string _table = "";
    private string _query = "";
    private string _mode = BigQueryConnectorConfig.DefaultMode;
    private string _topicPattern = BigQueryConnectorConfig.DefaultTopicPattern;
    private long _pollIntervalMs = BigQueryConnectorConfig.DefaultPollIntervalMs;
    private int _maxRowsPerPoll = BigQueryConnectorConfig.DefaultMaxRowsPerPoll;
    private bool _includeMetadata = true;
    private string _timestampColumn = "";
    private bool _useStandardSql = true;
    private string _location = BigQueryConnectorConfig.DefaultLocation;

    private DateTimeOffset? _lastTimestamp;
    private DateTime _lastPollTime = DateTime.MinValue;
    private IDictionary<string, object> _sourcePartition = new Dictionary<string, object>();

    public override void Start(IDictionary<string, string> config)
    {
        _projectId = config[BigQueryConnectorConfig.ProjectIdConfig];
        _dataset = config[BigQueryConnectorConfig.DatasetConfig];
        _mode = GetConfigValue(config, BigQueryConnectorConfig.ModeConfig, BigQueryConnectorConfig.DefaultMode);
        _table = GetConfigValue(config, BigQueryConnectorConfig.TableConfig, "");
        _query = GetConfigValue(config, BigQueryConnectorConfig.QueryConfig, "");
        _topicPattern = GetConfigValue(config, BigQueryConnectorConfig.TopicPatternConfig, BigQueryConnectorConfig.DefaultTopicPattern);
        _pollIntervalMs = long.Parse(GetConfigValue(config, BigQueryConnectorConfig.PollIntervalMsConfig, BigQueryConnectorConfig.DefaultPollIntervalMs.ToString()));
        _maxRowsPerPoll = int.Parse(GetConfigValue(config, BigQueryConnectorConfig.MaxRowsPerPollConfig, BigQueryConnectorConfig.DefaultMaxRowsPerPoll.ToString()));
        _includeMetadata = bool.Parse(GetConfigValue(config, BigQueryConnectorConfig.IncludeMetadataConfig, "true"));
        _timestampColumn = GetConfigValue(config, BigQueryConnectorConfig.TimestampColumnConfig, "");
        _useStandardSql = bool.Parse(GetConfigValue(config, BigQueryConnectorConfig.UseStandardSqlConfig, "true"));
        _location = GetConfigValue(config, BigQueryConnectorConfig.LocationConfig, BigQueryConnectorConfig.DefaultLocation);

        _sourcePartition = new Dictionary<string, object>
        {
            [BigQueryConnectorConfig.OffsetTable] = _table
        };

        RestoreOffset();

        _client = CreateClient(config);
    }

    private void RestoreOffset()
    {
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return;

        if (storedOffset.TryGetValue(BigQueryConnectorConfig.OffsetTimestamp, out var timestamp) &&
            DateTimeOffset.TryParse(timestamp?.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var restored))
        {
            _lastTimestamp = restored;
        }
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static BigQueryClient CreateClient(IDictionary<string, string> config)
    {
        var projectId = config[BigQueryConnectorConfig.ProjectIdConfig];
        var credentialsJson = GetConfigValue(config, BigQueryConnectorConfig.CredentialsJsonConfig, "");
        var credentialsFile = GetConfigValue(config, BigQueryConnectorConfig.CredentialsFileConfig, "");

        if (!string.IsNullOrEmpty(credentialsJson))
        {
            #pragma warning disable CS0618 // FromJson/FromFile: operator-supplied credentials, not untrusted input
            var credential = GoogleCredential.FromJson(credentialsJson);
            #pragma warning restore CS0618
            return BigQueryClient.Create(projectId, credential);
        }
        else if (!string.IsNullOrEmpty(credentialsFile))
        {
            #pragma warning disable CS0618 // FromJson/FromFile: operator-supplied credentials, not untrusted input
            var credential = GoogleCredential.FromFile(credentialsFile);
            #pragma warning restore CS0618
            return BigQueryClient.Create(projectId, credential);
        }
        else
        {
            // Use Application Default Credentials
            return BigQueryClient.Create(projectId);
        }
    }

    public override void Stop()
    {
        _client?.Dispose();
        _client = null;
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
        if (_client == null)
            return [];

        // Respect poll interval
        var elapsed = DateTime.UtcNow - _lastPollTime;
        if (elapsed.TotalMilliseconds < _pollIntervalMs)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs - elapsed.TotalMilliseconds), cancellationToken);
        }

        _lastPollTime = DateTime.UtcNow;

        var records = new List<SourceRecord>();

        try
        {
            var query = BuildQuery();
            var results = await ExecuteQueryAsync(query, cancellationToken);

            foreach (var row in results)
            {
                var record = CreateSourceRecord(row);
                records.Add(record);

                // Track timestamp for incremental polling
                if (!string.IsNullOrEmpty(_timestampColumn) && row.TryGetValue(_timestampColumn, out var tsValue) && tsValue != null)
                {
                    if (tsValue is DateTime dt)
                    {
                        var offset = new DateTimeOffset(dt, TimeSpan.Zero);
                        if (!_lastTimestamp.HasValue || offset > _lastTimestamp.Value)
                            _lastTimestamp = offset;
                    }
                    else if (tsValue is DateTimeOffset dto)
                    {
                        if (!_lastTimestamp.HasValue || dto > _lastTimestamp.Value)
                            _lastTimestamp = dto;
                    }
                }

                if (records.Count >= _maxRowsPerPoll)
                    break;
            }
        }
        catch (Exception ex)
        {
            Context?.RaiseError?.Invoke(ex);
        }

        return records;
    }

    private string BuildQuery()
        => BuildQuery(_mode, _query, _projectId, _dataset, _table, _timestampColumn, _lastTimestamp, _maxRowsPerPoll);

    internal static string BuildQuery(
        string mode,
        string userQuery,
        string projectId,
        string dataset,
        string table,
        string timestampColumn,
        DateTimeOffset? lastTimestamp,
        int maxRowsPerPoll)
    {
        if (mode.Equals("query", StringComparison.OrdinalIgnoreCase))
        {
            // Wrap the user query as a subquery so the incremental filter and the row limit
            // never interfere with its own clauses (WHERE in subqueries, GROUP BY, LIMIT, ...)
            var query = $"SELECT * FROM ({userQuery.Trim().TrimEnd(';').TrimEnd()})";

            if (!string.IsNullOrEmpty(timestampColumn) && lastTimestamp.HasValue)
            {
                query += $" WHERE {timestampColumn} > {FormatTimestampLiteral(lastTimestamp.Value)}";
            }

            return query + $" LIMIT {maxRowsPerPoll}";
        }
        else
        {
            // Table mode
            var sb = new StringBuilder();
            sb.Append($"SELECT * FROM `{projectId}.{dataset}.{table}`");

            if (!string.IsNullOrEmpty(timestampColumn) && lastTimestamp.HasValue)
            {
                sb.Append($" WHERE {timestampColumn} > {FormatTimestampLiteral(lastTimestamp.Value)}");
            }

            if (!string.IsNullOrEmpty(timestampColumn))
            {
                sb.Append($" ORDER BY {timestampColumn}");
            }

            sb.Append($" LIMIT {maxRowsPerPoll}");

            return sb.ToString();
        }
    }

    private static string FormatTimestampLiteral(DateTimeOffset timestamp)
        => $"TIMESTAMP('{timestamp.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)}')";

    private async Task<List<Dictionary<string, object?>>> ExecuteQueryAsync(string sql, CancellationToken cancellationToken)
    {
        var results = new List<Dictionary<string, object?>>();

        var options = new QueryOptions
        {
            UseQueryCache = true,
            UseLegacySql = !_useStandardSql
        };

        var queryResults = await _client!.ExecuteQueryAsync(sql, null, options, cancellationToken: cancellationToken);

        foreach (var row in queryResults)
        {
            var rowDict = new Dictionary<string, object?>();

            for (var i = 0; i < row.Schema.Fields.Count; i++)
            {
                var field = row.Schema.Fields[i];
                var value = row[field.Name];
                rowDict[field.Name] = ConvertBigQueryValue(value, field);
            }

            results.Add(rowDict);
        }

        return results;
    }

    private static object? ConvertBigQueryValue(object? value, TableFieldSchema field)
    {
        if (value == null)
            return null;

        return field.Type switch
        {
            "TIMESTAMP" or "DATETIME" => value is DateTime dt ? dt : value,
            "DATE" => value is DateTime dt ? dt.Date : value,
            "TIME" => value is TimeSpan ts ? ts : value,
            "INTEGER" or "INT64" => value is long l ? l : Convert.ToInt64(value),
            "FLOAT" or "FLOAT64" => value is double d ? d : Convert.ToDouble(value),
            "BOOLEAN" or "BOOL" => value is bool b ? b : Convert.ToBoolean(value),
            "BYTES" => value is byte[] bytes ? bytes : value,
            "RECORD" or "STRUCT" => value, // Keep as-is, will be serialized as nested object
            "GEOGRAPHY" => value.ToString(),
            "NUMERIC" or "BIGNUMERIC" => value is decimal dec ? dec : Convert.ToDecimal(value),
            _ => value
        };
    }

    private SourceRecord CreateSourceRecord(Dictionary<string, object?> row)
    {
        var topic = BuildTopic();
        var key = BuildKey(row);
        var value = JsonSerializer.SerializeToUtf8Bytes(row, JsonSerializerOptions);
        var headers = BuildHeaders();

        var sourceOffset = new Dictionary<string, object>();
        if (!string.IsNullOrEmpty(_timestampColumn) && row.TryGetValue(_timestampColumn, out var ts) && ts != null)
        {
            // Round-trippable so RestoreOffset can parse it back after a task restart
            sourceOffset[BigQueryConnectorConfig.OffsetTimestamp] = ts switch
            {
                DateTime dt => new DateTimeOffset(dt, TimeSpan.Zero).ToString("O"),
                DateTimeOffset dto => dto.ToString("O"),
                _ => Convert.ToString(ts, CultureInfo.InvariantCulture)!
            };
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = sourceOffset,
            Topic = topic,
            Key = key,
            Value = value,
            Timestamp = DateTimeOffset.UtcNow,
            Headers = headers
        };
    }

    private string BuildTopic()
    {
        return _topicPattern
            .Replace("${project}", _projectId)
            .Replace("${dataset}", _dataset)
            .Replace("${table}", _table);
    }

    private byte[]? BuildKey(Dictionary<string, object?> row)
    {
        // Use first column as key if available
        if (row.Count > 0)
        {
            var firstKey = row.Keys.First();
            var firstValue = row[firstKey];
            if (firstValue != null)
            {
                return Encoding.UTF8.GetBytes(firstValue.ToString()!);
            }
        }
        return null;
    }

    private Dictionary<string, byte[]> BuildHeaders()
    {
        var headers = new Dictionary<string, byte[]>();

        if (_includeMetadata)
        {
            headers[BigQueryConnectorConfig.HeaderProjectId] = Encoding.UTF8.GetBytes(_projectId);
            headers[BigQueryConnectorConfig.HeaderDataset] = Encoding.UTF8.GetBytes(_dataset);

            if (!string.IsNullOrEmpty(_table))
                headers[BigQueryConnectorConfig.HeaderTable] = Encoding.UTF8.GetBytes(_table);

            headers[BigQueryConnectorConfig.HeaderLocation] = Encoding.UTF8.GetBytes(_location);
            headers[BigQueryConnectorConfig.HeaderTimestamp] = Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O"));
        }

        return headers;
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Offsets tracked internally
        return Task.CompletedTask;
    }

    public override void CommitRecord(SourceRecord record, RecordMetadata metadata)
    {
        // Individual record commit - nothing to do
    }

    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };
}
