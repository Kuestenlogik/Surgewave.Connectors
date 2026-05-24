using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Neo4j.Driver;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Neo4j;

/// <summary>
/// Task that reads graph data from Neo4j using Cypher queries.
/// Supports incremental polling with timestamp-based tracking.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Driver disposed in Stop()")]
public sealed class Neo4jSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private IDriver? _driver;
    private string _database = Neo4jConnectorConfig.DefaultDatabase;
    private string _label = "";
    private string _customQuery = "";
    private string _topic = "";
    private string _topicPattern = Neo4jConnectorConfig.DefaultTopicPattern;
    private long _pollIntervalMs = Neo4jConnectorConfig.DefaultPollIntervalMs;
    private int _maxRowsPerPoll = Neo4jConnectorConfig.DefaultMaxRowsPerPoll;
    private bool _includeMetadata = true;
    private string _timestampProperty = "";
    private string _idProperty = "";

    private object? _lastTimestamp;
    private DateTime _lastPollTime = DateTime.MinValue;
    private IDictionary<string, object> _sourcePartition = new Dictionary<string, object>();

    public override void Start(IDictionary<string, string> config)
    {
        var uri = config[Neo4jConnectorConfig.UriConfig];
        var username = GetConfigValue(config, Neo4jConnectorConfig.UsernameConfig, "");
        var password = GetConfigValue(config, Neo4jConnectorConfig.PasswordConfig, "");
        _database = GetConfigValue(config, Neo4jConnectorConfig.DatabaseConfig, Neo4jConnectorConfig.DefaultDatabase);
        _label = GetConfigValue(config, Neo4jConnectorConfig.LabelConfig, "");
        _customQuery = GetConfigValue(config, Neo4jConnectorConfig.QueryConfig, "");
        _topic = GetConfigValue(config, Neo4jConnectorConfig.TopicConfig, "");
        _topicPattern = GetConfigValue(config, Neo4jConnectorConfig.TopicPatternConfig, Neo4jConnectorConfig.DefaultTopicPattern);
        _pollIntervalMs = long.Parse(GetConfigValue(config, Neo4jConnectorConfig.PollIntervalMsConfig, Neo4jConnectorConfig.DefaultPollIntervalMs.ToString()));
        _maxRowsPerPoll = int.Parse(GetConfigValue(config, Neo4jConnectorConfig.MaxRowsPerPollConfig, Neo4jConnectorConfig.DefaultMaxRowsPerPoll.ToString()));
        _includeMetadata = bool.Parse(GetConfigValue(config, Neo4jConnectorConfig.IncludeMetadataConfig, "true"));
        _timestampProperty = GetConfigValue(config, Neo4jConnectorConfig.TimestampPropertyConfig, "");
        _idProperty = GetConfigValue(config, Neo4jConnectorConfig.IdPropertyConfig, "");
        var encrypted = bool.Parse(GetConfigValue(config, Neo4jConnectorConfig.EncryptedConfig, "false"));

        _sourcePartition = new Dictionary<string, object>
        {
            [Neo4jConnectorConfig.HeaderDatabase] = _database,
            [Neo4jConnectorConfig.HeaderLabel] = _label
        };

        var authToken = string.IsNullOrEmpty(username) ? AuthTokens.None : AuthTokens.Basic(username, password);

        _driver = GraphDatabase.Driver(new Uri(uri), authToken, builder =>
        {
            if (encrypted)
                builder.WithEncryptionLevel(EncryptionLevel.Encrypted);
            else
                builder.WithEncryptionLevel(EncryptionLevel.None);
        });
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    public override void Stop()
    {
        _driver?.Dispose();
        _driver = null;
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
        if (_driver == null)
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
            await using var session = _driver.AsyncSession(o => o.WithDatabase(_database));

            var query = BuildQuery();
            var parameters = BuildParameters();

            var result = await session.RunAsync(query, parameters);
            var resultRecords = await result.ToListAsync(cancellationToken);

            foreach (var record in resultRecords)
            {
                var sourceRecord = CreateSourceRecord(record);
                records.Add(sourceRecord);

                // Track timestamp for incremental polling
                if (!string.IsNullOrEmpty(_timestampProperty))
                {
                    var timestamp = GetPropertyValue(record, _timestampProperty);
                    if (timestamp != null)
                    {
                        _lastTimestamp = timestamp;
                    }
                }

                if (records.Count >= _maxRowsPerPoll)
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Neo4j poll error: {ex.Message}");
        }

        return records;
    }

    private string BuildQuery()
    {
        if (!string.IsNullOrEmpty(_customQuery))
        {
            return _customQuery;
        }

        var sb = new StringBuilder();
        sb.Append($"MATCH (n:{_label})");

        // Add incremental filter if timestamp property is set
        if (!string.IsNullOrEmpty(_timestampProperty) && _lastTimestamp != null)
        {
            sb.Append($" WHERE n.{_timestampProperty} > $lastTimestamp");
        }

        sb.Append(" RETURN n");

        // Order by timestamp if available
        if (!string.IsNullOrEmpty(_timestampProperty))
        {
            sb.Append($" ORDER BY n.{_timestampProperty} ASC");
        }

        sb.Append($" LIMIT {_maxRowsPerPoll}");

        return sb.ToString();
    }

    private Dictionary<string, object?> BuildParameters()
    {
        var parameters = new Dictionary<string, object?>();

        if (!string.IsNullOrEmpty(_timestampProperty) && _lastTimestamp != null)
        {
            parameters["lastTimestamp"] = _lastTimestamp;
        }

        return parameters;
    }

    private SourceRecord CreateSourceRecord(IRecord record)
    {
        var rowDict = new Dictionary<string, object?>();
        string? label = _label;
        string? elementId = null;
        long? nodeId = null;

        // Handle node result
        if (record.Values.TryGetValue("n", out var nodeValue) && nodeValue is INode node)
        {
            foreach (var prop in node.Properties)
            {
                rowDict[prop.Key] = ConvertNeo4jValue(prop.Value);
            }

            label = node.Labels.Count > 0 ? node.Labels[0] : _label;
            elementId = node.ElementId;
        }
        else
        {
            // Handle generic result (custom query)
            foreach (var recordKey in record.Keys)
            {
                rowDict[recordKey] = ConvertNeo4jValue(record[recordKey]);
            }
        }

        var topic = BuildTopic(label);
        var recordKeyBytes = BuildKey(record, elementId);
        var recordValue = JsonSerializer.SerializeToUtf8Bytes(rowDict, JsonSerializerOptions);
        var headers = BuildHeaders(label, elementId, nodeId);

        var sourceOffset = new Dictionary<string, object>();
        if (elementId != null)
        {
            sourceOffset[Neo4jConnectorConfig.OffsetElementId] = elementId;
        }
        if (!string.IsNullOrEmpty(_timestampProperty))
        {
            var timestamp = GetPropertyValue(record, _timestampProperty);
            if (timestamp != null)
            {
                sourceOffset[Neo4jConnectorConfig.OffsetTimestamp] = timestamp.ToString()!;
            }
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = sourceOffset,
            Topic = topic,
            Key = recordKeyBytes,
            Value = recordValue,
            Timestamp = DateTimeOffset.UtcNow,
            Headers = headers
        };
    }

    private static object? ConvertNeo4jValue(object? value)
    {
        return value switch
        {
            null => null,
            INode node => node.Properties.ToDictionary(p => p.Key, p => ConvertNeo4jValue(p.Value)),
            IRelationship rel => rel.Properties.ToDictionary(p => p.Key, p => ConvertNeo4jValue(p.Value)),
            IPath path => new { nodes = path.Nodes.Select(n => ConvertNeo4jValue(n)), relationships = path.Relationships.Select(r => ConvertNeo4jValue(r)) },
            ZonedDateTime zdt => zdt.ToDateTimeOffset(),
            LocalDateTime ldt => ldt.ToDateTime(),
            LocalDate ld => ld.ToDateTime(),
            LocalTime lt => lt.ToTimeSpan(),
            Duration d => d.ToString(),
            Point p => new { x = p.X, y = p.Y, z = p.Z, srid = p.SrId },
            IReadOnlyList<object> list => list.Select(ConvertNeo4jValue).ToList(),
            IReadOnlyDictionary<string, object> dict => dict.ToDictionary(kv => kv.Key, kv => ConvertNeo4jValue(kv.Value)),
            _ => value
        };
    }

    private static object? GetPropertyValue(IRecord record, string propertyName)
    {
        if (record.Values.TryGetValue("n", out var nodeValue) && nodeValue is INode node)
        {
            if (node.Properties.TryGetValue(propertyName, out var propValue))
            {
                return propValue;
            }
        }
        else if (record.Values.TryGetValue(propertyName, out var value))
        {
            return value;
        }

        return null;
    }

    private string BuildTopic(string? label)
    {
        if (!string.IsNullOrEmpty(_topic))
            return _topic;

        return _topicPattern
            .Replace("${database}", _database)
            .Replace("${label}", label ?? "node");
    }

    private byte[]? BuildKey(IRecord record, string? elementId)
    {
        // Use configured ID property
        if (!string.IsNullOrEmpty(_idProperty))
        {
            var idValue = GetPropertyValue(record, _idProperty);
            if (idValue != null)
            {
                return Encoding.UTF8.GetBytes(idValue.ToString()!);
            }
        }

        // Fall back to element ID
        if (!string.IsNullOrEmpty(elementId))
        {
            return Encoding.UTF8.GetBytes(elementId);
        }

        return null;
    }

    private Dictionary<string, byte[]> BuildHeaders(string? label, string? elementId, long? nodeId)
    {
        var headers = new Dictionary<string, byte[]>();

        if (_includeMetadata)
        {
            headers[Neo4jConnectorConfig.HeaderDatabase] = Encoding.UTF8.GetBytes(_database);

            if (!string.IsNullOrEmpty(label))
            {
                headers[Neo4jConnectorConfig.HeaderLabel] = Encoding.UTF8.GetBytes(label);
            }

            if (!string.IsNullOrEmpty(elementId))
            {
                headers[Neo4jConnectorConfig.HeaderElementId] = Encoding.UTF8.GetBytes(elementId);
            }

            if (nodeId.HasValue)
            {
                headers[Neo4jConnectorConfig.HeaderNodeId] = Encoding.UTF8.GetBytes(nodeId.Value.ToString());
            }
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
