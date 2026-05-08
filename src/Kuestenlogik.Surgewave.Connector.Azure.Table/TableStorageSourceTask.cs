using System.Text;
using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Table;

/// <summary>
/// Task that reads entities from Azure Table Storage using query polling.
/// Supports incremental modes based on Timestamp or RowKey.
/// </summary>
public sealed class TableStorageSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private TableClient? _tableClient;
    private string _tableName = "";
    private string _topicPattern = TableStorageConnectorConfig.DefaultTopicPattern;
    private string _queryFilter = "";
    private string[]? _selectColumns;
    private long _pollIntervalMs = TableStorageConnectorConfig.DefaultPollIntervalMs;
    private int _maxEntitiesPerPoll = TableStorageConnectorConfig.DefaultMaxEntitiesPerPoll;
    private string _incrementalMode = TableStorageConnectorConfig.IncrementalModeNone;
    private string _incrementalColumn = TableStorageConnectorConfig.DefaultIncrementalColumn;
    private bool _includeMetadata = true;

    private DateTimeOffset? _lastTimestamp;
    private string? _lastRowKey;
    private string? _lastPartitionKey;

    private readonly Dictionary<string, object> _sourcePartition = new();

    public override void Start(IDictionary<string, string> config)
    {
        _tableName = config[TableStorageConnectorConfig.TableNameConfig];
        _topicPattern = GetConfigValue(config, TableStorageConnectorConfig.TopicPatternConfig, TableStorageConnectorConfig.DefaultTopicPattern);
        _queryFilter = GetConfigValue(config, TableStorageConnectorConfig.QueryFilterConfig, "");
        _pollIntervalMs = long.Parse(GetConfigValue(config, TableStorageConnectorConfig.PollIntervalMsConfig, TableStorageConnectorConfig.DefaultPollIntervalMs.ToString()));
        _maxEntitiesPerPoll = int.Parse(GetConfigValue(config, TableStorageConnectorConfig.MaxEntitiesPerPollConfig, TableStorageConnectorConfig.DefaultMaxEntitiesPerPoll.ToString()));
        _incrementalMode = GetConfigValue(config, TableStorageConnectorConfig.IncrementalModeConfig, TableStorageConnectorConfig.IncrementalModeNone);
        _incrementalColumn = GetConfigValue(config, TableStorageConnectorConfig.IncrementalColumnConfig, TableStorageConnectorConfig.DefaultIncrementalColumn);
        _includeMetadata = bool.Parse(GetConfigValue(config, TableStorageConnectorConfig.IncludeMetadataConfig, "true"));

        var selectColumnsStr = GetConfigValue(config, TableStorageConnectorConfig.SelectColumnsConfig, "");
        if (!string.IsNullOrEmpty(selectColumnsStr))
        {
            _selectColumns = selectColumnsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        _sourcePartition["table"] = _tableName;

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

        RestoreOffset();
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    private void RestoreOffset()
    {
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return;

        if (storedOffset.TryGetValue(TableStorageConnectorConfig.OffsetTimestamp, out var ts) && ts != null)
        {
            if (DateTimeOffset.TryParse(ts.ToString(), out var timestamp))
            {
                _lastTimestamp = timestamp;
            }
        }

        if (storedOffset.TryGetValue(TableStorageConnectorConfig.OffsetPartitionKey, out var pk) && pk != null)
        {
            _lastPartitionKey = pk.ToString();
        }

        if (storedOffset.TryGetValue(TableStorageConnectorConfig.OffsetRowKey, out var rk) && rk != null)
        {
            _lastRowKey = rk.ToString();
        }
    }

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

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_tableClient == null)
            return [];

        var records = new List<SourceRecord>();

        try
        {
            var filter = BuildQueryFilter();
            var query = _tableClient.QueryAsync<TableEntity>(
                filter: string.IsNullOrEmpty(filter) ? null : filter,
                maxPerPage: _maxEntitiesPerPoll,
                select: _selectColumns,
                cancellationToken: cancellationToken);

            var count = 0;
            await foreach (var entity in query.WithCancellation(cancellationToken))
            {
                if (count >= _maxEntitiesPerPoll)
                    break;

                var record = ConvertToSourceRecord(entity);
                records.Add(record);

                // Update tracking state
                UpdateTrackingState(entity);
                count++;
            }

            // If no records, wait before next poll
            if (records.Count == 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs), cancellationToken);
            }
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // Table doesn't exist, wait and retry
            await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMs), cancellationToken);
        }

        return records;
    }

    private string BuildQueryFilter()
    {
        var filters = new List<string>();

        // Add user-defined filter
        if (!string.IsNullOrEmpty(_queryFilter))
        {
            filters.Add($"({_queryFilter})");
        }

        // Add incremental filter
        switch (_incrementalMode)
        {
            case TableStorageConnectorConfig.IncrementalModeTimestamp when _lastTimestamp.HasValue:
                filters.Add($"Timestamp gt datetime'{_lastTimestamp.Value:O}'");
                break;

            case TableStorageConnectorConfig.IncrementalModeRowKey when !string.IsNullOrEmpty(_lastRowKey):
                if (!string.IsNullOrEmpty(_lastPartitionKey))
                {
                    // For same partition, get rows after last row key
                    // For different partitions, get all rows
                    filters.Add($"(PartitionKey gt '{_lastPartitionKey}') or (PartitionKey eq '{_lastPartitionKey}' and RowKey gt '{_lastRowKey}')");
                }
                else
                {
                    filters.Add($"RowKey gt '{_lastRowKey}'");
                }
                break;
        }

        return filters.Count > 0 ? string.Join(" and ", filters) : "";
    }

    private void UpdateTrackingState(TableEntity entity)
    {
        switch (_incrementalMode)
        {
            case TableStorageConnectorConfig.IncrementalModeTimestamp:
                if (entity.Timestamp.HasValue && (!_lastTimestamp.HasValue || entity.Timestamp.Value > _lastTimestamp.Value))
                {
                    _lastTimestamp = entity.Timestamp.Value;
                }
                break;

            case TableStorageConnectorConfig.IncrementalModeRowKey:
                _lastPartitionKey = entity.PartitionKey;
                _lastRowKey = entity.RowKey;
                break;
        }
    }

    private SourceRecord ConvertToSourceRecord(TableEntity entity)
    {
        // Build key
        var key = new Dictionary<string, object>
        {
            ["partition_key"] = entity.PartitionKey,
            ["row_key"] = entity.RowKey
        };

        // Build payload
        Dictionary<string, object?> payload;
        if (_includeMetadata)
        {
            var entityData = new Dictionary<string, object?>();
            foreach (var prop in entity)
            {
                if (prop.Key != "odata.etag")
                {
                    entityData[prop.Key] = ConvertValue(prop.Value);
                }
            }

            payload = new Dictionary<string, object?>
            {
                ["source"] = new Dictionary<string, object>
                {
                    ["table"] = _tableName,
                    ["partition_key"] = entity.PartitionKey,
                    ["row_key"] = entity.RowKey,
                    ["timestamp"] = entity.Timestamp?.ToString("O") ?? "",
                    ["etag"] = entity.ETag.ToString()
                },
                ["data"] = entityData,
                ["ts_ms"] = entity.Timestamp?.ToUnixTimeMilliseconds() ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };
        }
        else
        {
            payload = new Dictionary<string, object?>();
            foreach (var prop in entity)
            {
                if (prop.Key != "odata.etag")
                {
                    payload[prop.Key] = ConvertValue(prop.Value);
                }
            }
        }

        // Build offset
        var offset = new Dictionary<string, object>();
        if (_lastTimestamp.HasValue)
        {
            offset[TableStorageConnectorConfig.OffsetTimestamp] = _lastTimestamp.Value.ToString("O");
        }
        if (!string.IsNullOrEmpty(_lastPartitionKey))
        {
            offset[TableStorageConnectorConfig.OffsetPartitionKey] = _lastPartitionKey;
        }
        if (!string.IsNullOrEmpty(_lastRowKey))
        {
            offset[TableStorageConnectorConfig.OffsetRowKey] = _lastRowKey;
        }

        // Build headers
        var headers = new Dictionary<string, byte[]>
        {
            [TableStorageConnectorConfig.HeaderTableName] = Encoding.UTF8.GetBytes(_tableName),
            [TableStorageConnectorConfig.HeaderPartitionKey] = Encoding.UTF8.GetBytes(entity.PartitionKey),
            [TableStorageConnectorConfig.HeaderRowKey] = Encoding.UTF8.GetBytes(entity.RowKey)
        };

        if (entity.Timestamp.HasValue)
        {
            headers[TableStorageConnectorConfig.HeaderTimestamp] = Encoding.UTF8.GetBytes(entity.Timestamp.Value.ToString("O"));
        }
        headers[TableStorageConnectorConfig.HeaderEtag] = Encoding.UTF8.GetBytes(entity.ETag.ToString());

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = offset,
            Topic = GetTopicName(),
            Key = JsonSerializer.SerializeToUtf8Bytes(key),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = entity.Timestamp ?? DateTimeOffset.UtcNow,
            Headers = headers
        };
    }

    private static object? ConvertValue(object? value)
    {
        return value switch
        {
            DateTimeOffset dto => dto.ToString("O"),
            DateTime dt => dt.ToString("O"),
            BinaryData bd => Convert.ToBase64String(bd.ToArray()),
            byte[] bytes => Convert.ToBase64String(bytes),
            _ => value
        };
    }

    private string GetTopicName()
    {
        return _topicPattern.Replace("${table}", _tableName);
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Table Storage position is tracked via offset storage automatically
        return Task.CompletedTask;
    }
}
