namespace Kuestenlogik.Surgewave.Connector.Elasticsearch;

using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Bulk;
using Elastic.Transport;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that writes records to Elasticsearch using bulk operations for performance.
/// </summary>
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "ElasticsearchClient is managed by task lifecycle")]
public sealed class ElasticsearchSinkTask : SinkTask
{
    private ElasticsearchClient? _client;
    private string _indexPattern = ElasticsearchConnectorConfig.DefaultIndexPattern;
    private string _indexStrategy = ElasticsearchConnectorConfig.IndexStrategyTopic;
    private string _timeFormat = ElasticsearchConnectorConfig.DefaultTimeFormat;
    private string _indexField = "";
    private string _docIdStrategy = ElasticsearchConnectorConfig.DocIdStrategyAuto;
    private string _docIdField = "";
    private string[] _compositeFields = [];
    private string _compositeDelimiter = ElasticsearchConnectorConfig.DefaultCompositeDelimiter;
    private string _writeMethod = ElasticsearchConnectorConfig.WriteMethodIndex;
    private string _behaviorOnMalformed = ElasticsearchConnectorConfig.BehaviorWarn;
    private int _batchSize = ElasticsearchConnectorConfig.DefaultBatchSize;
    private int _retryMax = ElasticsearchConnectorConfig.DefaultRetryMax;
    private long _retryBackoffMs = ElasticsearchConnectorConfig.DefaultRetryBackoffMs;
    private readonly List<SinkRecord> _buffer = [];

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _client = CreateClient(config);

        _indexPattern = GetConfigValue(config, ElasticsearchConnectorConfig.IndexConfig, ElasticsearchConnectorConfig.DefaultIndexPattern);
        _indexStrategy = GetConfigValue(config, ElasticsearchConnectorConfig.IndexStrategyConfig, ElasticsearchConnectorConfig.IndexStrategyTopic);
        _timeFormat = GetConfigValue(config, ElasticsearchConnectorConfig.IndexTimeFormatConfig, ElasticsearchConnectorConfig.DefaultTimeFormat);
        _indexField = GetConfigValue(config, ElasticsearchConnectorConfig.IndexFieldConfig, "");
        _docIdStrategy = GetConfigValue(config, ElasticsearchConnectorConfig.DocumentIdStrategyConfig, ElasticsearchConnectorConfig.DocIdStrategyAuto);
        _docIdField = GetConfigValue(config, ElasticsearchConnectorConfig.DocumentIdFieldConfig, "");
        _compositeFields = GetConfigValue(config, ElasticsearchConnectorConfig.DocumentIdCompositeFieldsConfig, "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(f => f.Trim())
            .ToArray();
        _compositeDelimiter = GetConfigValue(config, ElasticsearchConnectorConfig.DocumentIdCompositeDelimiterConfig, ElasticsearchConnectorConfig.DefaultCompositeDelimiter);
        _writeMethod = GetConfigValue(config, ElasticsearchConnectorConfig.WriteMethodConfig, ElasticsearchConnectorConfig.WriteMethodIndex);
        _behaviorOnMalformed = GetConfigValue(config, ElasticsearchConnectorConfig.BehaviorOnMalformedConfig, ElasticsearchConnectorConfig.BehaviorWarn);
        _batchSize = int.Parse(GetConfigValue(config, ElasticsearchConnectorConfig.BatchSizeConfig, ElasticsearchConnectorConfig.DefaultBatchSize.ToString()));
        _retryMax = int.Parse(GetConfigValue(config, ElasticsearchConnectorConfig.RetryMaxConfig, ElasticsearchConnectorConfig.DefaultRetryMax.ToString()));
        _retryBackoffMs = long.Parse(GetConfigValue(config, ElasticsearchConnectorConfig.RetryBackoffMsConfig, ElasticsearchConnectorConfig.DefaultRetryBackoffMs.ToString()));
    }

    private static ElasticsearchClient CreateClient(IDictionary<string, string> config)
    {
        var hasCloudId = config.TryGetValue(ElasticsearchConnectorConfig.CloudIdConfig, out var cloudId) && !string.IsNullOrEmpty(cloudId);
        var hasApiKey = config.TryGetValue(ElasticsearchConnectorConfig.ApiKeyConfig, out var apiKey) && !string.IsNullOrEmpty(apiKey);
        var hasUsername = config.TryGetValue(ElasticsearchConnectorConfig.UsernameConfig, out var username) && !string.IsNullOrEmpty(username);
        var password = GetConfigValue(config, ElasticsearchConnectorConfig.PasswordConfig, "");

        ElasticsearchClientSettings settings;

        if (hasCloudId)
        {
            // CloudNodePool requires credentials
            if (hasApiKey)
            {
                settings = new ElasticsearchClientSettings(new CloudNodePool(cloudId!, new ApiKey(apiKey!)));
            }
            else if (hasUsername)
            {
                settings = new ElasticsearchClientSettings(new CloudNodePool(cloudId!, new BasicAuthentication(username!, password)));
            }
            else
            {
                throw new ArgumentException("Cloud ID requires API key or username/password authentication");
            }
        }
        else
        {
            var urls = config[ElasticsearchConnectorConfig.UrlConfig]
                .Split(',', StringSplitOptions.RemoveEmptyEntries)
                .Select(u => new Uri(u.Trim()))
                .ToArray();

            settings = urls.Length == 1
                ? new ElasticsearchClientSettings(urls[0])
                : new ElasticsearchClientSettings(new StaticNodePool(urls));

            // Configure authentication for non-cloud
            if (hasApiKey)
            {
                settings = settings.Authentication(new ApiKey(apiKey!));
            }
            else if (hasUsername)
            {
                settings = settings.Authentication(new BasicAuthentication(username!, password));
            }
        }

        return new ElasticsearchClient(settings);
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        FlushBuffer();
        _client = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _client = null;
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
        if (_buffer.Count == 0 || _client == null)
            return;

        var attempt = 0;
        while (true)
        {
            try
            {
                var bulkRequest = new BulkRequest
                {
                    Operations = CreateBulkOperations(_buffer)
                };

                var response = await _client.BulkAsync(bulkRequest, cancellationToken);
                HandleBulkResponse(response);
                _buffer.Clear();
                return;
            }
            catch (Exception ex) when (IsRetryable(ex) && attempt < _retryMax)
            {
                attempt++;
                await Task.Delay((int)(_retryBackoffMs * Math.Pow(2, attempt - 1)), cancellationToken);
            }
        }
    }

    private List<IBulkOperation> CreateBulkOperations(IReadOnlyList<SinkRecord> records)
    {
        var operations = new List<IBulkOperation>();

        foreach (var record in records)
        {
            try
            {
                var indexName = ResolveIndexName(record);
                var docId = GenerateDocumentId(record);

                // Parse the record value as JSON
                using var jsonDoc = JsonDocument.Parse(record.Value);
                var source = jsonDoc.RootElement.Clone();

                switch (_writeMethod)
                {
                    case ElasticsearchConnectorConfig.WriteMethodIndex:
                        var indexOp = new BulkIndexOperation<JsonElement>(source) { Index = indexName };
                        if (docId != null)
                            indexOp.Id = docId;
                        operations.Add(indexOp);
                        break;

                    case ElasticsearchConnectorConfig.WriteMethodCreate:
                        var createOp = new BulkCreateOperation<JsonElement>(source) { Index = indexName };
                        if (docId != null)
                            createOp.Id = docId;
                        operations.Add(createOp);
                        break;

                    case ElasticsearchConnectorConfig.WriteMethodUpsert:
                        if (docId != null)
                        {
                            var updateOp = new BulkUpdateOperation<JsonElement, JsonElement>(docId)
                            {
                                Index = indexName,
                                Doc = source,
                                DocAsUpsert = true
                            };
                            operations.Add(updateOp);
                        }
                        else
                        {
                            // Upsert requires an ID, fallback to index
                            operations.Add(new BulkIndexOperation<JsonElement>(source) { Index = indexName });
                        }
                        break;
                }
            }
            catch (JsonException ex)
            {
                HandleMalformedDocument(record, ex);
            }
        }

        return operations;
    }

    private string ResolveIndexName(SinkRecord record)
    {
        return _indexStrategy switch
        {
            ElasticsearchConnectorConfig.IndexStrategyStatic => _indexPattern,
            ElasticsearchConnectorConfig.IndexStrategyTopic => _indexPattern.Replace("${topic}", record.Topic),
            ElasticsearchConnectorConfig.IndexStrategyTime => $"{record.Topic}-{record.Timestamp.ToString(_timeFormat)}",
            ElasticsearchConnectorConfig.IndexStrategyField => ExtractFieldValue(record, _indexField) ?? record.Topic,
            _ => record.Topic
        };
    }

    private string? GenerateDocumentId(SinkRecord record)
    {
        return _docIdStrategy switch
        {
            ElasticsearchConnectorConfig.DocIdStrategyAuto => null,
            ElasticsearchConnectorConfig.DocIdStrategyKey => record.Key != null ? Encoding.UTF8.GetString(record.Key) : null,
            ElasticsearchConnectorConfig.DocIdStrategyField => ExtractFieldValue(record, _docIdField),
            ElasticsearchConnectorConfig.DocIdStrategyComposite => GenerateCompositeId(record),
            _ => null
        };
    }

    private string? GenerateCompositeId(SinkRecord record)
    {
        var values = _compositeFields
            .Select(f => ExtractFieldValue(record, f))
            .Where(v => v != null)
            .ToList();

        return values.Count > 0 ? string.Join(_compositeDelimiter, values) : null;
    }

    private static string? ExtractFieldValue(SinkRecord record, string fieldName)
    {
        try
        {
            using var doc = JsonDocument.Parse(record.Value);
            if (doc.RootElement.TryGetProperty(fieldName, out var prop))
            {
                return prop.ValueKind switch
                {
                    JsonValueKind.String => prop.GetString(),
                    JsonValueKind.Number => prop.GetRawText(),
                    JsonValueKind.True => "true",
                    JsonValueKind.False => "false",
                    _ => prop.GetRawText()
                };
            }
        }
        catch (JsonException)
        {
            // Invalid JSON, return null
        }

        return null;
    }

    private void HandleBulkResponse(BulkResponse response)
    {
        if (!response.Errors)
            return;

        foreach (var item in response.ItemsWithErrors)
        {
            var error = item.Error;

            switch (_behaviorOnMalformed)
            {
                case ElasticsearchConnectorConfig.BehaviorIgnore:
                    // Silently ignore
                    break;

                case ElasticsearchConnectorConfig.BehaviorWarn:
                    // Log to console since TaskContext doesn't have Logger
                    Console.Error.WriteLine($"Elasticsearch indexing failed: {error?.Reason}");
                    break;

                case ElasticsearchConnectorConfig.BehaviorFail:
                    throw new InvalidOperationException($"Elasticsearch indexing failed: {error?.Reason}");
            }
        }
    }

    private void HandleMalformedDocument(SinkRecord record, Exception ex)
    {
        switch (_behaviorOnMalformed)
        {
            case ElasticsearchConnectorConfig.BehaviorIgnore:
                break;

            case ElasticsearchConnectorConfig.BehaviorWarn:
                Console.Error.WriteLine($"Malformed document in topic {record.Topic} partition {record.Partition} offset {record.Offset}: {ex.Message}");
                break;

            case ElasticsearchConnectorConfig.BehaviorFail:
                throw new InvalidOperationException($"Malformed document: {ex.Message}", ex);
        }
    }

    private static bool IsRetryable(Exception ex)
    {
        // Retry on transient failures
        return ex is HttpRequestException or TaskCanceledException { InnerException: TimeoutException };
    }
}
