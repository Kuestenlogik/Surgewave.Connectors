using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.CosmosDb;

/// <summary>
/// Task that writes records to Azure Cosmos DB containers.
/// Uses bulk execution for high throughput operations.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class CosmosDbSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private CosmosClient? _cosmosClient;
    private Container? _container;
    private Database? _database;
    private string _databaseName = "";
    private string _containerName = "";
    private string _partitionKeyPath = "/id";
    private string _idField = "id";
    private string _writeMode = CosmosDbConnectorConfig.WriteModeUpsert;
    private int _batchSize = CosmosDbConnectorConfig.DefaultBatchSize;
    private bool _autoCreateContainer;
    private int _throughput = CosmosDbConnectorConfig.DefaultThroughput;
    private bool _containerVerified;

    public override void Start(IDictionary<string, string> config)
    {
        _databaseName = config[CosmosDbConnectorConfig.DatabaseConfig];
        _containerName = config[CosmosDbConnectorConfig.ContainerConfig];
        _partitionKeyPath = GetConfigValue(config, CosmosDbConnectorConfig.PartitionKeyPathConfig, "/id");
        _idField = GetConfigValue(config, CosmosDbConnectorConfig.IdFieldConfig, "id");
        _writeMode = GetConfigValue(config, CosmosDbConnectorConfig.WriteModeConfig, CosmosDbConnectorConfig.WriteModeUpsert);
        _batchSize = int.Parse(GetConfigValue(config, CosmosDbConnectorConfig.BatchSizeConfig, CosmosDbConnectorConfig.DefaultBatchSize.ToString()));
        _autoCreateContainer = bool.Parse(GetConfigValue(config, CosmosDbConnectorConfig.AutoCreateContainerConfig, "false"));
        _throughput = int.Parse(GetConfigValue(config, CosmosDbConnectorConfig.ThroughputConfig, CosmosDbConnectorConfig.DefaultThroughput.ToString()));

        var maxRetryCount = int.Parse(GetConfigValue(config, CosmosDbConnectorConfig.MaxRetryCountConfig, CosmosDbConnectorConfig.DefaultMaxRetryCount.ToString()));
        var maxRetryWaitTime = long.Parse(GetConfigValue(config, CosmosDbConnectorConfig.MaxRetryWaitTimeMsConfig, CosmosDbConnectorConfig.DefaultMaxRetryWaitTimeMs.ToString()));

        var connectionString = GetConfigValue(config, CosmosDbConnectorConfig.ConnectionStringConfig, "");
        var endpoint = GetConfigValue(config, CosmosDbConnectorConfig.EndpointConfig, "");
        var accountKey = GetConfigValue(config, CosmosDbConnectorConfig.AccountKeyConfig, "");

        var clientOptions = new CosmosClientOptions
        {
            AllowBulkExecution = true,
            MaxRetryAttemptsOnRateLimitedRequests = maxRetryCount,
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromMilliseconds(maxRetryWaitTime),
            SerializerOptions = new CosmosSerializationOptions
            {
                PropertyNamingPolicy = CosmosPropertyNamingPolicy.Default
            }
        };

        if (!string.IsNullOrEmpty(connectionString))
        {
            _cosmosClient = new CosmosClient(connectionString, clientOptions);
        }
        else if (!string.IsNullOrEmpty(endpoint) && !string.IsNullOrEmpty(accountKey))
        {
            _cosmosClient = new CosmosClient(endpoint, accountKey, clientOptions);
        }
        else
        {
            throw new ArgumentException("Connection string or endpoint/key must be provided");
        }

        _database = _cosmosClient.GetDatabase(_databaseName);
        _container = _database.GetContainer(_containerName);
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        _container = null;
        _database = null;
        _cosmosClient?.Dispose();
        _cosmosClient = null;
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
        if (_container == null || _cosmosClient == null || records.Count == 0)
            return;

        // Ensure container exists
        if (!_containerVerified)
        {
            await EnsureContainerExistsAsync(cancellationToken);
            _containerVerified = true;
        }

        // Process in batches with bulk execution
        var batches = records.Chunk(_batchSize);

        foreach (var batch in batches)
        {
            var tasks = new List<Task>();

            foreach (var record in batch)
            {
                var task = ProcessRecordAsync(record, cancellationToken);
                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
        }
    }

    private async Task ProcessRecordAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        if (record.Value == null || record.Value.Length == 0)
        {
            // Tombstone - delete if we can extract the key
            if (_writeMode != CosmosDbConnectorConfig.WriteModeDelete)
            {
                await DeleteFromKeyAsync(record, cancellationToken);
            }
            return;
        }

        try
        {
            var document = JsonDocument.Parse(record.Value);
            var root = document.RootElement;

            // Ensure document has id
            string? id = null;
            if (root.TryGetProperty(_idField, out var idProp))
            {
                id = idProp.ToString();
            }
            else if (root.TryGetProperty("id", out var idProp2))
            {
                id = idProp2.ToString();
            }

            if (string.IsNullOrEmpty(id))
            {
                // Generate id from key if available
                if (record.Key != null && record.Key.Length > 0)
                {
                    id = Convert.ToBase64String(record.Key);
                }
                else
                {
                    id = Guid.NewGuid().ToString();
                }
            }

            // Extract partition key value
            var partitionKeyValue = ExtractPartitionKeyValue(root);

            // Create document with id
            var docDict = JsonSerializer.Deserialize<Dictionary<string, object>>(record.Value)
                ?? new Dictionary<string, object>();
            docDict["id"] = id;

            var partitionKey = new PartitionKey(partitionKeyValue ?? id);

            switch (_writeMode)
            {
                case CosmosDbConnectorConfig.WriteModeUpsert:
                    await _container!.UpsertItemAsync(docDict, partitionKey, cancellationToken: cancellationToken);
                    break;

                case CosmosDbConnectorConfig.WriteModeCreate:
                    try
                    {
                        await _container!.CreateItemAsync(docDict, partitionKey, cancellationToken: cancellationToken);
                    }
                    catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
                    {
                        // Item already exists, ignore in create mode
                    }
                    break;

                case CosmosDbConnectorConfig.WriteModeReplace:
                    try
                    {
                        await _container!.ReplaceItemAsync(docDict, id, partitionKey, cancellationToken: cancellationToken);
                    }
                    catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                    {
                        // Item doesn't exist, create it
                        await _container!.CreateItemAsync(docDict, partitionKey, cancellationToken: cancellationToken);
                    }
                    break;

                case CosmosDbConnectorConfig.WriteModeDelete:
                    try
                    {
                        await _container!.DeleteItemAsync<object>(id, partitionKey, cancellationToken: cancellationToken);
                    }
                    catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                    {
                        // Item doesn't exist, ignore
                    }
                    break;
            }
        }
        catch (JsonException)
        {
            // Invalid JSON, skip this record
        }
    }

    private async Task DeleteFromKeyAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        if (record.Key == null || record.Key.Length == 0)
            return;

        try
        {
            var keyDoc = JsonDocument.Parse(record.Key);
            var root = keyDoc.RootElement;

            string? id = null;
            if (root.TryGetProperty("id", out var idProp))
            {
                id = idProp.ToString();
            }
            else if (root.TryGetProperty(_idField, out var idProp2))
            {
                id = idProp2.ToString();
            }

            if (string.IsNullOrEmpty(id))
                return;

            var partitionKeyValue = ExtractPartitionKeyValue(root) ?? id;

            try
            {
                await _container!.DeleteItemAsync<object>(id, new PartitionKey(partitionKeyValue), cancellationToken: cancellationToken);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // Item doesn't exist, ignore
            }
        }
        catch (JsonException)
        {
            // Invalid key JSON, skip
        }
    }

    private string? ExtractPartitionKeyValue(JsonElement root)
    {
        // Extract partition key based on path
        var pkPropertyName = _partitionKeyPath.TrimStart('/').Split('/')[0];

        if (root.TryGetProperty(pkPropertyName, out var pkProp))
        {
            return pkProp.ValueKind switch
            {
                JsonValueKind.String => pkProp.GetString(),
                JsonValueKind.Number => pkProp.GetRawText(),
                JsonValueKind.True => "true",
                JsonValueKind.False => "false",
                _ => pkProp.ToString()
            };
        }

        // Fallback to id
        if (root.TryGetProperty("id", out var idProp))
        {
            return idProp.ToString();
        }

        return null;
    }

    private async Task EnsureContainerExistsAsync(CancellationToken cancellationToken)
    {
        if (!_autoCreateContainer)
            return;

        try
        {
            await _container!.ReadContainerAsync(cancellationToken: cancellationToken);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            // Container doesn't exist, create it
            var containerProperties = new ContainerProperties(_containerName, _partitionKeyPath);
            await _database!.CreateContainerAsync(containerProperties, _throughput, cancellationToken: cancellationToken);

            // Re-get container reference
            _container = _database.GetContainer(_containerName);
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // Cosmos DB writes are executed in PutAsync with bulk execution
        return Task.CompletedTask;
    }
}
