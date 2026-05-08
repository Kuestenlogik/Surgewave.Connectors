using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.DynamoDB;

/// <summary>
/// Task that writes records to DynamoDB tables.
/// Supports batch writes for high throughput.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class DynamoDbSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private AmazonDynamoDBClient? _dynamoClient;
    private string _tableName = "";
    private string _writeMode = DynamoDbConnectorConfig.WriteModePut;
    private string _partitionKeyField = "";
    private string _sortKeyField = "";
    private int _batchSize = DynamoDbConnectorConfig.DefaultBatchSize;
    private bool _autoCreateTable;
    private string _billingMode = DynamoDbConnectorConfig.BillingModePayPerRequest;
    private long _readCapacity = DynamoDbConnectorConfig.DefaultReadCapacity;
    private long _writeCapacity = DynamoDbConnectorConfig.DefaultWriteCapacity;
    private bool _tableVerified;

    public override void Start(IDictionary<string, string> config)
    {
        _tableName = config[DynamoDbConnectorConfig.TableNameConfig];
        _writeMode = GetConfigValue(config, DynamoDbConnectorConfig.WriteModeConfig, DynamoDbConnectorConfig.WriteModePut);
        _partitionKeyField = config[DynamoDbConnectorConfig.PartitionKeyFieldConfig];
        _sortKeyField = GetConfigValue(config, DynamoDbConnectorConfig.SortKeyFieldConfig, "");
        _batchSize = Math.Min(25, int.Parse(GetConfigValue(config, DynamoDbConnectorConfig.BatchSizeConfig, DynamoDbConnectorConfig.DefaultBatchSize.ToString())));
        _autoCreateTable = bool.Parse(GetConfigValue(config, DynamoDbConnectorConfig.AutoCreateTableConfig, "false"));
        _billingMode = GetConfigValue(config, DynamoDbConnectorConfig.BillingModeConfig, DynamoDbConnectorConfig.BillingModePayPerRequest);
        _readCapacity = long.Parse(GetConfigValue(config, DynamoDbConnectorConfig.ReadCapacityConfig, DynamoDbConnectorConfig.DefaultReadCapacity.ToString()));
        _writeCapacity = long.Parse(GetConfigValue(config, DynamoDbConnectorConfig.WriteCapacityConfig, DynamoDbConnectorConfig.DefaultWriteCapacity.ToString()));

        var region = GetConfigValue(config, DynamoDbConnectorConfig.RegionConfig, DynamoDbConnectorConfig.DefaultRegion);
        var accessKey = GetConfigValue(config, DynamoDbConnectorConfig.AccessKeyConfig, "");
        var secretKey = GetConfigValue(config, DynamoDbConnectorConfig.SecretKeyConfig, "");
        var endpoint = GetConfigValue(config, DynamoDbConnectorConfig.EndpointConfig, "");

        var clientConfig = new AmazonDynamoDBConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(region)
        };

        if (!string.IsNullOrEmpty(endpoint))
        {
            clientConfig.ServiceURL = endpoint;
        }

        if (!string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey))
        {
            var credentials = new BasicAWSCredentials(accessKey, secretKey);
            _dynamoClient = new AmazonDynamoDBClient(credentials, clientConfig);
        }
        else
        {
            _dynamoClient = new AmazonDynamoDBClient(clientConfig);
        }
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        _dynamoClient?.Dispose();
        _dynamoClient = null;
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
        if (_dynamoClient == null)
            return;

        // Ensure table exists
        if (!_tableVerified)
        {
            await EnsureTableExistsAsync(cancellationToken);
            _tableVerified = true;
        }

        if (records.Count == 0)
            return;

        // Batch writes for put mode
        if (_writeMode == DynamoDbConnectorConfig.WriteModePut || _writeMode == DynamoDbConnectorConfig.WriteModeInsert)
        {
            await BatchWriteAsync(records, cancellationToken);
        }
        else
        {
            // Individual writes for update/delete
            foreach (var record in records)
            {
                await WriteRecordAsync(record, cancellationToken);
            }
        }
    }

    private async Task BatchWriteAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        var batches = records.Chunk(_batchSize);

        foreach (var batch in batches)
        {
            var writeRequests = new List<WriteRequest>();

            foreach (var record in batch)
            {
                var item = ParseRecordValue(record);
                if (item == null)
                    continue;

                // Handle tombstones (null values) as deletes
                if (record.Value == null || record.Value.Length == 0)
                {
                    var key = GetKeyFromRecord(record, item);
                    if (key.Count > 0)
                    {
                        writeRequests.Add(new WriteRequest
                        {
                            DeleteRequest = new DeleteRequest { Key = key }
                        });
                    }
                }
                else
                {
                    writeRequests.Add(new WriteRequest
                    {
                        PutRequest = new PutRequest { Item = item }
                    });
                }
            }

            if (writeRequests.Count > 0)
            {
                var request = new BatchWriteItemRequest
                {
                    RequestItems = new Dictionary<string, List<WriteRequest>>
                    {
                        [_tableName] = writeRequests
                    }
                };

                var response = await _dynamoClient!.BatchWriteItemAsync(request, cancellationToken);

                // Retry unprocessed items
                while (response.UnprocessedItems.Count > 0)
                {
                    await Task.Delay(100, cancellationToken);
                    request.RequestItems = response.UnprocessedItems;
                    response = await _dynamoClient.BatchWriteItemAsync(request, cancellationToken);
                }
            }
        }
    }

    private async Task WriteRecordAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var item = ParseRecordValue(record);
        if (item == null)
            return;

        switch (_writeMode)
        {
            case DynamoDbConnectorConfig.WriteModePut:
            case DynamoDbConnectorConfig.WriteModeInsert:
                await _dynamoClient!.PutItemAsync(new PutItemRequest
                {
                    TableName = _tableName,
                    Item = item,
                    ConditionExpression = _writeMode == DynamoDbConnectorConfig.WriteModeInsert
                        ? $"attribute_not_exists({_partitionKeyField})"
                        : null
                }, cancellationToken);
                break;

            case DynamoDbConnectorConfig.WriteModeUpdate:
                var key = GetKeyFromRecord(record, item);
                var updateExpressions = new List<string>();
                var expressionValues = new Dictionary<string, AttributeValue>();
                var expressionNames = new Dictionary<string, string>();

                var i = 0;
                foreach (var kvp in item)
                {
                    if (kvp.Key == _partitionKeyField || kvp.Key == _sortKeyField)
                        continue;

                    var attrName = $"#attr{i}";
                    var attrValue = $":val{i}";
                    expressionNames[attrName] = kvp.Key;
                    expressionValues[attrValue] = kvp.Value;
                    updateExpressions.Add($"{attrName} = {attrValue}");
                    i++;
                }

                if (updateExpressions.Count > 0)
                {
                    await _dynamoClient!.UpdateItemAsync(new UpdateItemRequest
                    {
                        TableName = _tableName,
                        Key = key,
                        UpdateExpression = "SET " + string.Join(", ", updateExpressions),
                        ExpressionAttributeNames = expressionNames,
                        ExpressionAttributeValues = expressionValues
                    }, cancellationToken);
                }
                break;

            case DynamoDbConnectorConfig.WriteModeDelete:
                var deleteKey = GetKeyFromRecord(record, item);
                await _dynamoClient!.DeleteItemAsync(new DeleteItemRequest
                {
                    TableName = _tableName,
                    Key = deleteKey
                }, cancellationToken);
                break;
        }
    }

    private Dictionary<string, AttributeValue>? ParseRecordValue(SinkRecord record)
    {
        if (record.Value == null || record.Value.Length == 0)
            return null;

        try
        {
            var json = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(record.Value);
            if (json == null)
                return null;

            var item = new Dictionary<string, AttributeValue>();

            foreach (var kvp in json)
            {
                var attrValue = ConvertJsonToAttributeValue(kvp.Value);
                if (attrValue != null)
                {
                    item[kvp.Key] = attrValue;
                }
            }

            return item;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static AttributeValue? ConvertJsonToAttributeValue(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.String:
                var str = element.GetString();
                return string.IsNullOrEmpty(str) ? null : new AttributeValue { S = str };

            case JsonValueKind.Number:
                return new AttributeValue { N = element.GetRawText() };

            case JsonValueKind.True:
                return new AttributeValue { BOOL = true };

            case JsonValueKind.False:
                return new AttributeValue { BOOL = false };

            case JsonValueKind.Null:
                return new AttributeValue { NULL = true };

            case JsonValueKind.Array:
                var list = new List<AttributeValue>();
                foreach (var item in element.EnumerateArray())
                {
                    var av = ConvertJsonToAttributeValue(item);
                    if (av != null)
                        list.Add(av);
                }
                return list.Count > 0 ? new AttributeValue { L = list } : null;

            case JsonValueKind.Object:
                var map = new Dictionary<string, AttributeValue>();
                foreach (var prop in element.EnumerateObject())
                {
                    var av = ConvertJsonToAttributeValue(prop.Value);
                    if (av != null)
                        map[prop.Name] = av;
                }
                return map.Count > 0 ? new AttributeValue { M = map } : null;

            default:
                return null;
        }
    }

    private Dictionary<string, AttributeValue> GetKeyFromRecord(SinkRecord record, Dictionary<string, AttributeValue> item)
    {
        var key = new Dictionary<string, AttributeValue>();

        // Try to get partition key from item
        if (item.TryGetValue(_partitionKeyField, out var pk))
        {
            key[_partitionKeyField] = pk;
        }
        else if (record.Key != null && record.Key.Length > 0)
        {
            // Try to parse key from record key
            try
            {
                var keyJson = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(record.Key);
                if (keyJson != null && keyJson.TryGetValue(_partitionKeyField, out var pkElement))
                {
                    var pkAttr = ConvertJsonToAttributeValue(pkElement);
                    if (pkAttr != null)
                        key[_partitionKeyField] = pkAttr;
                }
            }
            catch (JsonException)
            {
                // Use raw key as partition key string
                key[_partitionKeyField] = new AttributeValue { S = System.Text.Encoding.UTF8.GetString(record.Key) };
            }
        }

        // Add sort key if specified
        if (!string.IsNullOrEmpty(_sortKeyField) && item.TryGetValue(_sortKeyField, out var sk))
        {
            key[_sortKeyField] = sk;
        }

        return key;
    }

    private async Task EnsureTableExistsAsync(CancellationToken cancellationToken)
    {
        if (!_autoCreateTable)
            return;

        try
        {
            await _dynamoClient!.DescribeTableAsync(_tableName, cancellationToken);
            return; // Table exists
        }
        catch (ResourceNotFoundException)
        {
            // Table doesn't exist, create it
        }

        var keySchema = new List<KeySchemaElement>
        {
            new() { AttributeName = _partitionKeyField, KeyType = KeyType.HASH }
        };

        var attributeDefinitions = new List<AttributeDefinition>
        {
            new() { AttributeName = _partitionKeyField, AttributeType = ScalarAttributeType.S }
        };

        if (!string.IsNullOrEmpty(_sortKeyField))
        {
            keySchema.Add(new KeySchemaElement { AttributeName = _sortKeyField, KeyType = KeyType.RANGE });
            attributeDefinitions.Add(new AttributeDefinition { AttributeName = _sortKeyField, AttributeType = ScalarAttributeType.S });
        }

        var request = new CreateTableRequest
        {
            TableName = _tableName,
            KeySchema = keySchema,
            AttributeDefinitions = attributeDefinitions,
            BillingMode = _billingMode == DynamoDbConnectorConfig.BillingModePayPerRequest
                ? BillingMode.PAY_PER_REQUEST
                : BillingMode.PROVISIONED
        };

        if (_billingMode == DynamoDbConnectorConfig.BillingModeProvisioned)
        {
            request.ProvisionedThroughput = new ProvisionedThroughput
            {
                ReadCapacityUnits = _readCapacity,
                WriteCapacityUnits = _writeCapacity
            };
        }

        await _dynamoClient!.CreateTableAsync(request, cancellationToken);

        // Wait for table to become active
        var describeRequest = new DescribeTableRequest { TableName = _tableName };
        TableStatus status;
        do
        {
            await Task.Delay(1000, cancellationToken);
            var response = await _dynamoClient.DescribeTableAsync(describeRequest, cancellationToken);
            status = response.Table.TableStatus;
        } while (status != TableStatus.ACTIVE);
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // DynamoDB writes are synchronous in PutAsync
        return Task.CompletedTask;
    }
}
