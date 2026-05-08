using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.BigQuery;

/// <summary>
/// Task that writes records to BigQuery tables.
/// Supports streaming inserts and batch load jobs.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "BigQueryClient is stateless")]
public sealed class BigQuerySinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private BigQueryClient? _client;
    private string _projectId = "";
    private string _dataset = "";
    private string _table = "";
    private string _location = BigQueryConnectorConfig.DefaultLocation;
    private string _writeMode = BigQueryConnectorConfig.DefaultWriteMode;
    private int _batchSize = BigQueryConnectorConfig.DefaultBatchSize;
    private int _maxRetryCount = BigQueryConnectorConfig.DefaultMaxRetryCount;
    private long _retryDelayMs = BigQueryConnectorConfig.DefaultRetryDelayMs;
    private bool _autoCreateTable;
    private bool _autoCreateDataset;
    private bool _useStreaming = true;
    private string _timePartitioning = "";
    private string[] _clusteringFields = [];
    private bool _tableVerified;
    private TableSchema? _tableSchema;

    public override void Start(IDictionary<string, string> config)
    {
        _projectId = config[BigQueryConnectorConfig.ProjectIdConfig];
        _dataset = config[BigQueryConnectorConfig.DatasetConfig];
        _table = config[BigQueryConnectorConfig.TableConfig];
        _location = GetConfigValue(config, BigQueryConnectorConfig.LocationConfig, BigQueryConnectorConfig.DefaultLocation);
        _writeMode = GetConfigValue(config, BigQueryConnectorConfig.WriteModeConfig, BigQueryConnectorConfig.DefaultWriteMode);
        _batchSize = int.Parse(GetConfigValue(config, BigQueryConnectorConfig.BatchSizeConfig, BigQueryConnectorConfig.DefaultBatchSize.ToString()));
        _maxRetryCount = int.Parse(GetConfigValue(config, BigQueryConnectorConfig.MaxRetryCountConfig, BigQueryConnectorConfig.DefaultMaxRetryCount.ToString()));
        _retryDelayMs = long.Parse(GetConfigValue(config, BigQueryConnectorConfig.RetryDelayMsConfig, BigQueryConnectorConfig.DefaultRetryDelayMs.ToString()));
        _autoCreateTable = bool.Parse(GetConfigValue(config, BigQueryConnectorConfig.AutoCreateTableConfig, "false"));
        _autoCreateDataset = bool.Parse(GetConfigValue(config, BigQueryConnectorConfig.AutoCreateDatasetConfig, "false"));
        _useStreaming = bool.Parse(GetConfigValue(config, BigQueryConnectorConfig.UseStreamingConfig, "true"));
        _timePartitioning = GetConfigValue(config, BigQueryConnectorConfig.TimePartitioningConfig, "");

        var clusteringStr = GetConfigValue(config, BigQueryConnectorConfig.ClusteringFieldsConfig, "");
        _clusteringFields = string.IsNullOrEmpty(clusteringStr)
            ? []
            : clusteringStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        _client = CreateClient(config);
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
            var credential = GoogleCredential.FromJson(credentialsJson);
            return BigQueryClient.Create(projectId, credential);
        }
        else if (!string.IsNullOrEmpty(credentialsFile))
        {
            var credential = GoogleCredential.FromFile(credentialsFile);
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

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null || records.Count == 0)
            return;

        // Verify/create table on first batch
        if (!_tableVerified)
        {
            await EnsureTableExistsAsync(records, cancellationToken);
            _tableVerified = true;
        }

        // Process in batches
        var batches = records.Chunk(_batchSize);

        foreach (var batch in batches)
        {
            await ProcessBatchWithRetryAsync(batch, cancellationToken);
        }
    }

    private async Task ProcessBatchWithRetryAsync(SinkRecord[] records, CancellationToken cancellationToken)
    {
        var retryCount = 0;

        while (retryCount <= _maxRetryCount)
        {
            try
            {
                await ProcessBatchAsync(records, cancellationToken);
                return;
            }
            catch (Exception ex) when (IsRetriableException(ex) && retryCount < _maxRetryCount)
            {
                retryCount++;
                var delay = _retryDelayMs * Math.Pow(2, retryCount - 1);
                await Task.Delay(TimeSpan.FromMilliseconds(delay), cancellationToken);
            }
        }
    }

    private static bool IsRetriableException(Exception ex)
    {
        // Retry on rate limit, transient network errors
        var message = ex.Message.ToLowerInvariant();
        return message.Contains("rate") ||
               message.Contains("timeout") ||
               message.Contains("unavailable") ||
               message.Contains("backendError") ||
               ex is TaskCanceledException;
    }

    private async Task ProcessBatchAsync(SinkRecord[] records, CancellationToken cancellationToken)
    {
        var rows = new List<BigQueryInsertRow>();

        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0)
                continue;

            try
            {
                var data = JsonSerializer.Deserialize<Dictionary<string, object?>>(record.Value);
                if (data != null)
                {
                    var row = CreateInsertRow(data, record);
                    rows.Add(row);
                }
            }
            catch (JsonException)
            {
                // Invalid JSON, skip this record
            }
        }

        if (rows.Count == 0)
            return;

        if (_useStreaming)
        {
            await InsertRowsStreamingAsync(rows, cancellationToken);
        }
        else
        {
            await InsertRowsLoadJobAsync(rows, cancellationToken);
        }
    }

    private static BigQueryInsertRow CreateInsertRow(Dictionary<string, object?> data, SinkRecord record)
    {
        // Generate insert ID for deduplication
        var insertId = $"{record.Topic}-{record.Partition}-{record.Offset}";
        var row = new BigQueryInsertRow(insertId);

        foreach (var kvp in data)
        {
            var value = ConvertValue(kvp.Value);
            if (value != null)
            {
                row[kvp.Key] = value;
            }
        }

        return row;
    }

    private static object? ConvertValue(object? value)
    {
        if (value == null)
            return null;

        if (value is JsonElement element)
        {
            return element.ValueKind switch
            {
                JsonValueKind.Null => null,
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Number when element.TryGetInt64(out var l) => l,
                JsonValueKind.Number when element.TryGetDouble(out var d) => d,
                JsonValueKind.String => element.GetString(),
                JsonValueKind.Array => element.GetRawText(),
                JsonValueKind.Object => element.GetRawText(),
                _ => element.GetRawText()
            };
        }

        return value;
    }

    private async Task InsertRowsStreamingAsync(List<BigQueryInsertRow> rows, CancellationToken cancellationToken)
    {
        var tableRef = _client!.GetTableReference(_dataset, _table);
        var options = new InsertOptions
        {
            AllowUnknownFields = true,
            SkipInvalidRows = true
        };

        await _client.InsertRowsAsync(tableRef, rows, options, cancellationToken);
    }

    private async Task InsertRowsLoadJobAsync(List<BigQueryInsertRow> rows, CancellationToken cancellationToken)
    {
        // For load jobs, we need to write to temp file and upload
        var tableRef = _client!.GetTableReference(_dataset, _table);

        // Convert rows to newline-delimited JSON
        var jsonLines = rows.Select(row =>
        {
            var dict = new Dictionary<string, object?>();
            foreach (KeyValuePair<string, object> field in row)
            {
                dict[field.Key] = field.Value;
            }
            return JsonSerializer.Serialize(dict);
        });

        var json = string.Join("\n", jsonLines);
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

        var writeDisposition = _writeMode.ToLowerInvariant() switch
        {
            "truncate" => WriteDisposition.WriteTruncate,
            "append" => WriteDisposition.WriteAppend,
            _ => WriteDisposition.WriteAppend
        };

        var options = new UploadJsonOptions
        {
            WriteDisposition = writeDisposition
        };

        var job = await _client.UploadJsonAsync(_dataset, _table, _tableSchema, stream, options, cancellationToken);
        job = await job.PollUntilCompletedAsync(cancellationToken: cancellationToken);

        if (job.Status.ErrorResult != null)
        {
            throw new InvalidOperationException($"BigQuery load job failed: {job.Status.ErrorResult.Message}");
        }
    }

    private async Task EnsureTableExistsAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        // Try to get existing table
        try
        {
            var table = await _client!.GetTableAsync(_dataset, _table, cancellationToken: cancellationToken);
            _tableSchema = table.Schema;
            return;
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // Table doesn't exist
        }

        if (!_autoCreateTable)
            return;

        // Ensure dataset exists
        if (_autoCreateDataset)
        {
            try
            {
                await _client!.GetDatasetAsync(_dataset, cancellationToken: cancellationToken);
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                var datasetResource = new Dataset
                {
                    Location = _location
                };
                await _client!.CreateDatasetAsync(_dataset, datasetResource, cancellationToken: cancellationToken);
            }
        }

        // Create table from first record
        var firstRecord = records.FirstOrDefault(r => r.Value != null && r.Value.Length > 0);
        if (firstRecord?.Value == null)
            return;

        try
        {
            var data = JsonSerializer.Deserialize<Dictionary<string, object?>>(firstRecord.Value);
            if (data == null)
                return;

            var schema = InferSchema(data);
            _tableSchema = schema;

            // Create table resource with partitioning and clustering
            var tableResource = new Table
            {
                Schema = schema
            };

            // Add time partitioning if configured
            if (!string.IsNullOrEmpty(_timePartitioning))
            {
                tableResource.TimePartitioning = new TimePartitioning
                {
                    Type = _timePartitioning.ToUpperInvariant()
                };
            }

            // Add clustering if configured
            if (_clusteringFields.Length > 0)
            {
                tableResource.Clustering = new Clustering
                {
                    Fields = _clusteringFields.ToList()
                };
            }

            await _client!.CreateTableAsync(_dataset, _table, tableResource, cancellationToken: cancellationToken);
        }
        catch (JsonException)
        {
            // Invalid JSON, cannot create table
        }
    }

    private static TableSchema InferSchema(Dictionary<string, object?> data)
    {
        var fields = new List<TableFieldSchema>();

        foreach (var kvp in data)
        {
            var fieldType = InferFieldType(kvp.Value);
            fields.Add(new TableFieldSchema
            {
                Name = kvp.Key,
                Type = fieldType,
                Mode = "NULLABLE"
            });
        }

        return new TableSchema { Fields = fields };
    }

    private static string InferFieldType(object? value)
    {
        if (value == null)
            return "STRING";

        if (value is JsonElement element)
        {
            return element.ValueKind switch
            {
                JsonValueKind.True or JsonValueKind.False => "BOOLEAN",
                JsonValueKind.Number when element.TryGetInt64(out _) => "INTEGER",
                JsonValueKind.Number => "FLOAT",
                JsonValueKind.String => "STRING",
                JsonValueKind.Array or JsonValueKind.Object => "JSON",
                _ => "STRING"
            };
        }

        return value switch
        {
            bool => "BOOLEAN",
            int or long => "INTEGER",
            float or double or decimal => "FLOAT",
            DateTime or DateTimeOffset => "TIMESTAMP",
            byte[] => "BYTES",
            _ => "STRING"
        };
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // BigQuery writes are committed in PutAsync
        return Task.CompletedTask;
    }
}
