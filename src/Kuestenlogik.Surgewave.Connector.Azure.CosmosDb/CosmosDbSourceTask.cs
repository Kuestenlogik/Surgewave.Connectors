using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.CosmosDb;

/// <summary>
/// Task that captures changes from Azure Cosmos DB using Change Feed.
/// Uses the Change Feed Pull Model for polling-based consumption.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class CosmosDbSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private CosmosClient? _cosmosClient;
    private Container? _container;
    private FeedIterator<JsonElement>? _changeFeedIterator;
    private string _database = "";
    private string _containerName = "";
    private string _topicPattern = CosmosDbConnectorConfig.DefaultTopicPattern;
    private int _maxItemCount = CosmosDbConnectorConfig.DefaultChangeFeedMaxItems;
    private long _pollIntervalMs = CosmosDbConnectorConfig.DefaultPollIntervalMs;
    private string _startFrom = CosmosDbConnectorConfig.StartFromNow;
    private bool _includeMetadata = true;
    private string? _continuationToken;

    private readonly Dictionary<string, object> _sourcePartition = new();

    public override void Start(IDictionary<string, string> config)
    {
        _database = config[CosmosDbConnectorConfig.DatabaseConfig];
        _containerName = config[CosmosDbConnectorConfig.ContainerConfig];
        _topicPattern = GetConfigValue(config, CosmosDbConnectorConfig.TopicPatternConfig, CosmosDbConnectorConfig.DefaultTopicPattern);
        _maxItemCount = int.Parse(GetConfigValue(config, CosmosDbConnectorConfig.ChangeFeedMaxItemsConfig, CosmosDbConnectorConfig.DefaultChangeFeedMaxItems.ToString()));
        _pollIntervalMs = long.Parse(GetConfigValue(config, CosmosDbConnectorConfig.ChangeFeedPollIntervalMsConfig, CosmosDbConnectorConfig.DefaultPollIntervalMs.ToString()));
        _startFrom = GetConfigValue(config, CosmosDbConnectorConfig.ChangeFeedStartFromConfig, CosmosDbConnectorConfig.StartFromNow);
        _includeMetadata = bool.Parse(GetConfigValue(config, CosmosDbConnectorConfig.IncludeMetadataConfig, "true"));

        _sourcePartition["database"] = _database;
        _sourcePartition["container"] = _containerName;

        // Create Cosmos client
        var connectionString = GetConfigValue(config, CosmosDbConnectorConfig.ConnectionStringConfig, "");
        var endpoint = GetConfigValue(config, CosmosDbConnectorConfig.EndpointConfig, "");
        var accountKey = GetConfigValue(config, CosmosDbConnectorConfig.AccountKeyConfig, "");

        var clientOptions = new CosmosClientOptions
        {
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

        _container = _cosmosClient.GetContainer(_database, _containerName);

        RestoreOffset();
        InitializeChangeFeedIterator();
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    private void RestoreOffset()
    {
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return;

        if (storedOffset.TryGetValue(CosmosDbConnectorConfig.OffsetContinuationToken, out var token) && token != null)
        {
            _continuationToken = token.ToString();
            _startFrom = CosmosDbConnectorConfig.StartFromContinuation;
        }
    }

    private void InitializeChangeFeedIterator()
    {
        ChangeFeedStartFrom startFrom = _startFrom switch
        {
            CosmosDbConnectorConfig.StartFromBeginning => ChangeFeedStartFrom.Beginning(),
            CosmosDbConnectorConfig.StartFromContinuation when !string.IsNullOrEmpty(_continuationToken)
                => ChangeFeedStartFrom.ContinuationToken(_continuationToken),
            _ => ChangeFeedStartFrom.Now()
        };

        var options = new ChangeFeedRequestOptions
        {
            PageSizeHint = _maxItemCount
        };

        _changeFeedIterator = _container!.GetChangeFeedIterator<JsonElement>(
            startFrom,
            ChangeFeedMode.LatestVersion,
            options);
    }

    public override void Stop()
    {
        _changeFeedIterator = null;
        _container = null;
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

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_changeFeedIterator == null || _container == null)
            return [];

        var records = new List<SourceRecord>();

        try
        {
            if (_changeFeedIterator.HasMoreResults)
            {
                var response = await _changeFeedIterator.ReadNextAsync(cancellationToken);

                // Update continuation token
                _continuationToken = response.ContinuationToken;

                foreach (var item in response)
                {
                    var sourceRecord = ConvertToSourceRecord(item, response);
                    records.Add(sourceRecord);
                }

                // If no changes, wait before next poll
                if (response.StatusCode == System.Net.HttpStatusCode.NotModified || records.Count == 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs), cancellationToken);
                }
            }
            else
            {
                // Re-initialize iterator to continue polling
                InitializeChangeFeedIterator();
                await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs), cancellationToken);
            }
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotModified)
        {
            // No changes available, wait and retry
            await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs), cancellationToken);
        }

        return records;
    }

    private SourceRecord ConvertToSourceRecord(JsonElement item, FeedResponse<JsonElement> response)
    {
        // Extract standard Cosmos DB properties
        var id = item.TryGetProperty("id", out var idProp) ? idProp.GetString() ?? "" : "";
        var partitionKey = ExtractPartitionKey(item);
        var etag = item.TryGetProperty("_etag", out var etagProp) ? etagProp.GetString() ?? "" : "";
        var ts = item.TryGetProperty("_ts", out var tsProp) ? tsProp.GetInt64() : DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        // Build key
        var key = new Dictionary<string, object>
        {
            ["id"] = id
        };

        if (!string.IsNullOrEmpty(partitionKey))
        {
            key["partition_key"] = partitionKey;
        }

        // Build payload
        Dictionary<string, object?> payload;
        if (_includeMetadata)
        {
            payload = new Dictionary<string, object?>
            {
                ["op"] = "c", // Change feed only returns current state (create/replace)
                ["source"] = new Dictionary<string, object>
                {
                    ["database"] = _database,
                    ["container"] = _containerName,
                    ["partition_key"] = partitionKey ?? "",
                    ["etag"] = etag,
                    ["timestamp"] = ts,
                    ["activity_id"] = response.ActivityId
                },
                ["after"] = JsonSerializer.Deserialize<object>(item.GetRawText()),
                ["ts_ms"] = ts * 1000
            };
        }
        else
        {
            payload = new Dictionary<string, object?>
            {
                ["data"] = JsonSerializer.Deserialize<object>(item.GetRawText())
            };
        }

        // Build offset with continuation token
        var offset = new Dictionary<string, object>
        {
            [CosmosDbConnectorConfig.OffsetContinuationToken] = _continuationToken ?? ""
        };

        var headers = new Dictionary<string, byte[]>
        {
            [CosmosDbConnectorConfig.HeaderDatabase] = Encoding.UTF8.GetBytes(_database),
            [CosmosDbConnectorConfig.HeaderContainer] = Encoding.UTF8.GetBytes(_containerName),
            [CosmosDbConnectorConfig.HeaderEtag] = Encoding.UTF8.GetBytes(etag),
            [CosmosDbConnectorConfig.HeaderTimestamp] = Encoding.UTF8.GetBytes(ts.ToString())
        };

        if (!string.IsNullOrEmpty(partitionKey))
        {
            headers[CosmosDbConnectorConfig.HeaderPartitionKey] = Encoding.UTF8.GetBytes(partitionKey);
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = offset,
            Topic = GetTopicName(),
            Key = JsonSerializer.SerializeToUtf8Bytes(key),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = DateTimeOffset.FromUnixTimeSeconds(ts),
            Headers = headers
        };
    }

    private static string? ExtractPartitionKey(JsonElement item)
    {
        // Try common partition key property names
        string[] commonPartitionKeys = ["partitionKey", "pk", "tenantId", "userId", "id"];

        foreach (var pkName in commonPartitionKeys)
        {
            if (item.TryGetProperty(pkName, out var pkProp) && pkProp.ValueKind == JsonValueKind.String)
            {
                return pkProp.GetString();
            }
        }

        // Return id as fallback
        if (item.TryGetProperty("id", out var idProp))
        {
            return idProp.GetString();
        }

        return null;
    }

    private string GetTopicName()
    {
        return _topicPattern
            .Replace("${database}", _database)
            .Replace("${container}", _containerName);
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Cosmos DB change feed position is tracked via continuation token
        // Position is stored in offset storage automatically
        return Task.CompletedTask;
    }
}
