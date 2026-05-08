using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Table;

/// <summary>
/// Task that writes records to Azure Table Storage.
/// Supports batch operations for high throughput.
/// </summary>
public sealed class TableStorageSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private TableClient? _tableClient;
    private string _tableName = "";
    private string _writeMode = TableStorageConnectorConfig.WriteModeUpsert;
    private string _partitionKeyField = "partitionKey";
    private string _rowKeyField = "rowKey";
    private int _batchSize = TableStorageConnectorConfig.DefaultBatchSize;
    private bool _autoCreateTable;
    private int _maxRetryCount = TableStorageConnectorConfig.DefaultMaxRetryCount;
    private long _retryDelayMs = TableStorageConnectorConfig.DefaultRetryDelayMs;
    private bool _tableVerified;

    public override void Start(IDictionary<string, string> config)
    {
        _tableName = config[TableStorageConnectorConfig.TableNameConfig];
        _writeMode = GetConfigValue(config, TableStorageConnectorConfig.WriteModeConfig, TableStorageConnectorConfig.WriteModeUpsert);
        _partitionKeyField = GetConfigValue(config, TableStorageConnectorConfig.PartitionKeyFieldConfig, "partitionKey");
        _rowKeyField = GetConfigValue(config, TableStorageConnectorConfig.RowKeyFieldConfig, "rowKey");
        _batchSize = Math.Min(100, int.Parse(GetConfigValue(config, TableStorageConnectorConfig.BatchSizeConfig, TableStorageConnectorConfig.DefaultBatchSize.ToString())));
        _autoCreateTable = bool.Parse(GetConfigValue(config, TableStorageConnectorConfig.AutoCreateTableConfig, "false"));
        _maxRetryCount = int.Parse(GetConfigValue(config, TableStorageConnectorConfig.MaxRetryCountConfig, TableStorageConnectorConfig.DefaultMaxRetryCount.ToString()));
        _retryDelayMs = long.Parse(GetConfigValue(config, TableStorageConnectorConfig.RetryDelayMsConfig, TableStorageConnectorConfig.DefaultRetryDelayMs.ToString()));

        // Create Table client
        var connectionString = GetConfigValue(config, TableStorageConnectorConfig.ConnectionStringConfig, "");
        var accountName = GetConfigValue(config, TableStorageConnectorConfig.AccountNameConfig, "");
        var accountKey = GetConfigValue(config, TableStorageConnectorConfig.AccountKeyConfig, "");
        var endpoint = GetConfigValue(config, TableStorageConnectorConfig.EndpointConfig, "");

        if (!string.IsNullOrEmpty(connectionString))
        {
            _tableClient = new TableClient(connectionString, _tableName);
        }
        else if (!string.IsNullOrEmpty(accountName) && !string.IsNullOrEmpty(accountKey))
        {
            var serviceUri = !string.IsNullOrEmpty(endpoint)
                ? new Uri(endpoint)
                : new Uri($"https://{accountName}.table.core.windows.net");

            var credential = new TableSharedKeyCredential(accountName, accountKey);
            _tableClient = new TableClient(serviceUri, _tableName, credential);
        }
        else if (!string.IsNullOrEmpty(endpoint))
        {
            // For Azurite with default credentials
            _tableClient = new TableClient(new Uri($"{endpoint.TrimEnd('/')}/{_tableName}"));
        }
        else
        {
            throw new ArgumentException("Connection string or account name/key must be provided");
        }
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        _tableClient = null;
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
        if (_tableClient == null || records.Count == 0)
            return;

        // Ensure table exists
        if (!_tableVerified)
        {
            await EnsureTableExistsAsync(cancellationToken);
            _tableVerified = true;
        }

        // Group by partition key for efficient batching
        var byPartition = new Dictionary<string, List<SinkRecord>>();
        foreach (var record in records)
        {
            var entity = ParseEntity(record);
            if (entity == null)
                continue;

            var pk = entity.PartitionKey;
            if (!byPartition.TryGetValue(pk, out var list))
            {
                list = new List<SinkRecord>();
                byPartition[pk] = list;
            }
            list.Add(record);
        }

        // Process each partition's records
        foreach (var partition in byPartition)
        {
            var partitionRecords = partition.Value;

            // Process in batches (max 100 per batch, same partition key)
            foreach (var batch in partitionRecords.Chunk(_batchSize))
            {
                await ProcessBatchAsync(batch, cancellationToken);
            }
        }
    }

    private async Task ProcessBatchAsync(SinkRecord[] batch, CancellationToken cancellationToken)
    {
        var actions = new List<TableTransactionAction>();

        foreach (var record in batch)
        {
            var entity = ParseEntity(record);
            if (entity == null)
                continue;

            // Handle tombstones (null/empty value = delete)
            if (record.Value == null || record.Value.Length == 0)
            {
                if (_writeMode != TableStorageConnectorConfig.WriteModeDelete)
                {
                    var deleteEntity = ParseKeyFromRecord(record);
                    if (deleteEntity != null)
                    {
                        actions.Add(new TableTransactionAction(TableTransactionActionType.Delete, deleteEntity, ETag.All));
                    }
                }
                continue;
            }

            var actionType = _writeMode switch
            {
                TableStorageConnectorConfig.WriteModeUpsert => TableTransactionActionType.UpsertReplace,
                TableStorageConnectorConfig.WriteModeInsert => TableTransactionActionType.Add,
                TableStorageConnectorConfig.WriteModeUpdate => TableTransactionActionType.UpdateReplace,
                TableStorageConnectorConfig.WriteModeDelete => TableTransactionActionType.Delete,
                _ => TableTransactionActionType.UpsertReplace
            };

            var etag = _writeMode == TableStorageConnectorConfig.WriteModeDelete ? ETag.All : default;
            actions.Add(new TableTransactionAction(actionType, entity, etag));
        }

        if (actions.Count == 0)
            return;

        // Execute batch with retry
        var retries = 0;
        while (retries <= _maxRetryCount)
        {
            try
            {
                await _tableClient!.SubmitTransactionAsync(actions, cancellationToken);
                return;
            }
            catch (RequestFailedException ex) when (ex.Status == 409 && _writeMode == TableStorageConnectorConfig.WriteModeInsert)
            {
                // Conflict on insert - entity already exists, skip
                return;
            }
            catch (RequestFailedException ex) when (ex.Status == 404 && _writeMode == TableStorageConnectorConfig.WriteModeUpdate)
            {
                // Not found on update - entity doesn't exist, skip
                return;
            }
            catch (RequestFailedException ex) when (IsTransient(ex.Status) && retries < _maxRetryCount)
            {
                retries++;
                await Task.Delay(TimeSpan.FromMilliseconds(_retryDelayMs * retries), cancellationToken);
            }
            catch (TableTransactionFailedException)
            {
                // Batch failed - fall back to individual operations
                await ProcessIndividuallyAsync(actions, cancellationToken);
                return;
            }
        }
    }

    private async Task ProcessIndividuallyAsync(List<TableTransactionAction> actions, CancellationToken cancellationToken)
    {
        foreach (var action in actions)
        {
            try
            {
                switch (action.ActionType)
                {
                    case TableTransactionActionType.Add:
                        await _tableClient!.AddEntityAsync(action.Entity, cancellationToken);
                        break;
                    case TableTransactionActionType.UpsertReplace:
                    case TableTransactionActionType.UpsertMerge:
                        await _tableClient!.UpsertEntityAsync(action.Entity, TableUpdateMode.Replace, cancellationToken);
                        break;
                    case TableTransactionActionType.UpdateReplace:
                    case TableTransactionActionType.UpdateMerge:
                        await _tableClient!.UpdateEntityAsync(action.Entity, ETag.All, TableUpdateMode.Replace, cancellationToken);
                        break;
                    case TableTransactionActionType.Delete:
                        await _tableClient!.DeleteEntityAsync(action.Entity.PartitionKey, action.Entity.RowKey, ETag.All, cancellationToken);
                        break;
                }
            }
            catch (RequestFailedException ex) when (ex.Status == 409 || ex.Status == 404)
            {
                // Conflict or not found - skip
            }
        }
    }

    private static bool IsTransient(int statusCode)
    {
        return statusCode == 429 || statusCode == 500 || statusCode == 503;
    }

    private TableEntity? ParseEntity(SinkRecord record)
    {
        if (record.Value == null || record.Value.Length == 0)
            return null;

        try
        {
            using var doc = JsonDocument.Parse(record.Value);
            var root = doc.RootElement;

            // Extract partition key
            string? partitionKey = null;
            if (root.TryGetProperty(_partitionKeyField, out var pkProp))
            {
                partitionKey = pkProp.ToString();
            }
            else if (root.TryGetProperty("PartitionKey", out var pk2))
            {
                partitionKey = pk2.ToString();
            }

            // Extract row key
            string? rowKey = null;
            if (root.TryGetProperty(_rowKeyField, out var rkProp))
            {
                rowKey = rkProp.ToString();
            }
            else if (root.TryGetProperty("RowKey", out var rk2))
            {
                rowKey = rk2.ToString();
            }

            // Generate keys if not found
            if (string.IsNullOrEmpty(partitionKey))
            {
                partitionKey = record.Key != null && record.Key.Length > 0
                    ? Convert.ToBase64String(record.Key)
                    : "default";
            }

            if (string.IsNullOrEmpty(rowKey))
            {
                rowKey = record.Key != null && record.Key.Length > 0
                    ? Convert.ToBase64String(record.Key)
                    : Guid.NewGuid().ToString();
            }

            var entity = new TableEntity(partitionKey, rowKey);

            // Add all properties
            foreach (var prop in root.EnumerateObject())
            {
                // Skip partition and row key fields
                if (prop.Name == _partitionKeyField || prop.Name == _rowKeyField ||
                    prop.Name == "PartitionKey" || prop.Name == "RowKey")
                    continue;

                var value = ConvertJsonElement(prop.Value);
                if (value != null)
                {
                    entity[prop.Name] = value;
                }
            }

            return entity;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private TableEntity? ParseKeyFromRecord(SinkRecord record)
    {
        if (record.Key == null || record.Key.Length == 0)
            return null;

        try
        {
            using var doc = JsonDocument.Parse(record.Key);
            var root = doc.RootElement;

            string? partitionKey = null;
            string? rowKey = null;

            if (root.TryGetProperty("partition_key", out var pk))
                partitionKey = pk.ToString();
            else if (root.TryGetProperty(_partitionKeyField, out var pk2))
                partitionKey = pk2.ToString();

            if (root.TryGetProperty("row_key", out var rk))
                rowKey = rk.ToString();
            else if (root.TryGetProperty(_rowKeyField, out var rk2))
                rowKey = rk2.ToString();

            if (string.IsNullOrEmpty(partitionKey) || string.IsNullOrEmpty(rowKey))
                return null;

            return new TableEntity(partitionKey, rowKey);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static object? ConvertJsonElement(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number when element.TryGetInt32(out var i) => i,
            JsonValueKind.Number when element.TryGetInt64(out var l) => l,
            JsonValueKind.Number when element.TryGetDouble(out var d) => d,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => element.GetRawText()
        };
    }

    private async Task EnsureTableExistsAsync(CancellationToken cancellationToken)
    {
        if (!_autoCreateTable)
            return;

        try
        {
            await _tableClient!.CreateIfNotExistsAsync(cancellationToken);
        }
        catch (RequestFailedException)
        {
            // Table might already exist or creation failed - continue anyway
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // Table operations are executed in PutAsync
        return Task.CompletedTask;
    }
}
