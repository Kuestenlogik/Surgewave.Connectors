namespace Kuestenlogik.Surgewave.Connector.Elasticsearch;

using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Elastic.Transport;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that reads documents from Elasticsearch using scroll or search_after pagination.
/// </summary>
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "ElasticsearchClient is managed by task lifecycle")]
public sealed class ElasticsearchSourceTask : SourceTask
{
    private ElasticsearchClient? _client;
    private string _topic = "";
    private string _index = "";
    private string _query = "*";
    private string _scrollMode = ElasticsearchConnectorConfig.ScrollModeSearchAfter;
    private int _scrollSize = ElasticsearchConnectorConfig.DefaultScrollSize;
    private string _scrollKeepAlive = ElasticsearchConnectorConfig.DefaultScrollKeepAlive;
    private string _sortField = ElasticsearchConnectorConfig.DefaultSortField;
    private long _pollIntervalMs = ElasticsearchConnectorConfig.DefaultPollIntervalMs;
    private string _incrementalMode = ElasticsearchConnectorConfig.IncrementalModeNone;
    private string _incrementalField = ElasticsearchConnectorConfig.DefaultIncrementalField;
    private DateTimeOffset _lastPollTime = DateTimeOffset.MinValue;

    // Parsed 'query' when it was supplied as DSL JSON instead of a query string
    private Query? _dslQuery;

    // Scroll state
    private string? _scrollId;
    private List<FieldValue>? _lastSortValues;
    private string? _lastTimestampValue;

    private readonly Dictionary<string, object> _sourcePartition = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _client = CreateClient(config);

        _topic = config[ElasticsearchConnectorConfig.TopicConfig];
        _index = config[ElasticsearchConnectorConfig.IndexConfig];
        _query = GetConfigValue(config, ElasticsearchConnectorConfig.QueryConfig, "*");
        _scrollMode = GetConfigValue(config, ElasticsearchConnectorConfig.ScrollModeConfig, ElasticsearchConnectorConfig.ScrollModeSearchAfter);
        _scrollSize = int.Parse(GetConfigValue(config, ElasticsearchConnectorConfig.ScrollSizeConfig, ElasticsearchConnectorConfig.DefaultScrollSize.ToString()));
        _scrollKeepAlive = GetConfigValue(config, ElasticsearchConnectorConfig.ScrollKeepAliveConfig, ElasticsearchConnectorConfig.DefaultScrollKeepAlive);
        _sortField = GetConfigValue(config, ElasticsearchConnectorConfig.SortFieldConfig, ElasticsearchConnectorConfig.DefaultSortField);
        _pollIntervalMs = long.Parse(GetConfigValue(config, ElasticsearchConnectorConfig.PollIntervalMsConfig, ElasticsearchConnectorConfig.DefaultPollIntervalMs.ToString()));
        _incrementalMode = GetConfigValue(config, ElasticsearchConnectorConfig.IncrementalModeConfig, ElasticsearchConnectorConfig.IncrementalModeNone);
        _incrementalField = GetConfigValue(config, ElasticsearchConnectorConfig.IncrementalFieldConfig, ElasticsearchConnectorConfig.DefaultIncrementalField);

        _sourcePartition["index"] = _index;
        _sourcePartition["query"] = _query;

        // A DSL body is parsed once here so a malformed query fails at startup instead of
        // silently degrading to match-all at poll time.
        _dslQuery = ParseDslQuery(_query);

        // Restore offsets if available
        RestoreOffsets();
    }

    /// <summary>
    /// Parses a query supplied as DSL JSON. Accepts both a bare query clause
    /// ('{"term":{...}}') and a full search body ('{"query":{...}}').
    /// Returns null when the configured query is a plain query string.
    /// </summary>
    private Query? ParseDslQuery(string query)
    {
        if (!query.TrimStart().StartsWith('{'))
            return null;

        try
        {
            var clause = query;

            using (var document = JsonDocument.Parse(query))
            {
                if (document.RootElement.ValueKind == JsonValueKind.Object &&
                    document.RootElement.TryGetProperty("query", out var inner))
                {
                    clause = inner.GetRawText();
                }
            }

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(clause));
            return _client!.RequestResponseSerializer.Deserialize<Query>(stream);
        }
        catch (Exception ex)
        {
            throw new ArgumentException(
                $"{ElasticsearchConnectorConfig.QueryConfig} is not a valid Elasticsearch query DSL document", ex);
        }
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

    private void RestoreOffsets()
    {
        var storedOffset = Context.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return;

        if (storedOffset.TryGetValue("scroll_id", out var scrollId))
        {
            _scrollId = scrollId?.ToString();
        }

        if (storedOffset.TryGetValue("sort_values", out var sortValuesJson))
        {
            try
            {
                var json = sortValuesJson?.ToString();
                if (!string.IsNullOrEmpty(json))
                {
                    // Parse stored sort values
                    using var doc = JsonDocument.Parse(json);
                    _lastSortValues = doc.RootElement.EnumerateArray()
                        .Select(ParseFieldValue)
                        .ToList();
                }
            }
            catch (JsonException)
            {
                // Invalid JSON, ignore
            }
        }

        if (storedOffset.TryGetValue("last_timestamp", out var timestamp))
        {
            _lastTimestampValue = timestamp?.ToString();
        }
    }

    private static FieldValue ParseFieldValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => FieldValue.String(element.GetString()!),
            JsonValueKind.Number when element.TryGetInt64(out var l) => FieldValue.Long(l),
            JsonValueKind.Number => FieldValue.Double(element.GetDouble()),
            JsonValueKind.True => FieldValue.True,
            JsonValueKind.False => FieldValue.False,
            JsonValueKind.Null => FieldValue.Null,
            _ => FieldValue.String(element.GetRawText())
        };
    }

    public override void Stop()
    {
        // Clear scroll context if using scroll mode
        if (_scrollMode == ElasticsearchConnectorConfig.ScrollModeScroll && _scrollId != null && _client != null)
        {
            try
            {
                _client.ClearScrollAsync(new ClearScrollRequest { ScrollId = _scrollId })
                    .GetAwaiter().GetResult();
            }
            catch
            {
                // Ignore errors during cleanup
            }
        }

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
        // Handle poll interval
        var now = DateTimeOffset.UtcNow;
        var elapsed = (now - _lastPollTime).TotalMilliseconds;
        if (elapsed < _pollIntervalMs)
        {
            var waitTime = (int)(_pollIntervalMs - elapsed);
            await Task.Delay(waitTime, cancellationToken);
        }
        _lastPollTime = DateTimeOffset.UtcNow;

        return _scrollMode == ElasticsearchConnectorConfig.ScrollModeScroll
            ? await PollWithScrollAsync(cancellationToken)
            : await PollWithSearchAfterAsync(cancellationToken);
    }

    private async Task<List<SourceRecord>> PollWithScrollAsync(CancellationToken cancellationToken)
    {
        if (_client == null)
            return [];

        var records = new List<SourceRecord>();

        if (_scrollId == null)
        {
            // Initial search request
            var searchResponse = await _client.SearchAsync<JsonDocument>(s => s
                .Indices(_index)
                .Size(_scrollSize)
                .Scroll(_scrollKeepAlive)
                .Query(BuildQuery())
                .Sort(sort => sort.Field(new Field(_sortField))),
                cancellationToken);

            if (!searchResponse.IsValidResponse)
            {
                RaiseSearchError("scroll search", searchResponse.DebugInformation);
                return records;
            }

            _scrollId = searchResponse.ScrollId?.ToString();
            records.AddRange(ConvertHits(searchResponse.Hits));
        }
        else
        {
            // Continue scrolling
            var scrollResponse = await _client.ScrollAsync<JsonDocument>(
                new ScrollRequest { ScrollId = _scrollId, Scroll = _scrollKeepAlive },
                cancellationToken);

            if (!scrollResponse.IsValidResponse)
            {
                RaiseSearchError("scroll", scrollResponse.DebugInformation);
                _scrollId = null; // Reset on error
                return records;
            }

            _scrollId = scrollResponse.ScrollId?.ToString();
            records.AddRange(ConvertHits(scrollResponse.Hits));

            // If no more results, clear scroll
            if (records.Count == 0 && _scrollId != null)
            {
                await _client.ClearScrollAsync(new ClearScrollRequest { ScrollId = _scrollId }, cancellationToken);
                _scrollId = null;
            }
        }

        return records;
    }

    private async Task<List<SourceRecord>> PollWithSearchAfterAsync(CancellationToken cancellationToken)
    {
        if (_client == null)
            return [];

        var searchRequest = new SearchRequest(_index)
        {
            Size = _scrollSize,
            Query = BuildQuery(),
            Sort = [new SortOptions { Field = new FieldSort { Field = new Field(_sortField) } }]
        };

        if (_lastSortValues != null && _lastSortValues.Count > 0)
        {
            searchRequest.SearchAfter = _lastSortValues;
        }

        var response = await _client.SearchAsync<JsonDocument>(searchRequest, cancellationToken);

        if (!response.IsValidResponse)
        {
            RaiseSearchError("search", response.DebugInformation);
            return [];
        }

        var records = ConvertHits(response.Hits);

        // Update last sort values for next request
        if (response.Hits.Count > 0)
        {
            var lastHit = response.Hits.Last();
            _lastSortValues = lastHit.Sort?.ToList();

            // Track timestamp for incremental mode
            if (_incrementalMode == ElasticsearchConnectorConfig.IncrementalModeTimestamp && lastHit.Source != null)
            {
                _lastTimestampValue = ExtractTimestampValue(lastHit.Source);
            }
        }

        return records;
    }

    /// <summary>
    /// Makes a rejected search visible instead of returning an empty stream forever.
    /// </summary>
    private void RaiseSearchError(string operation, string debugInformation)
        => Context?.RaiseError?.Invoke(new InvalidOperationException(
            $"Elasticsearch {operation} on index '{_index}' failed: {debugInformation}"));

    private Query BuildQuery()
    {
        Query baseQuery;

        // Parse query - either simple query string or DSL JSON
        if (_dslQuery != null)
        {
            // Use the DSL document the user configured
            baseQuery = _dslQuery;
        }
        else if (_query == "*")
        {
            baseQuery = new MatchAllQuery();
        }
        else
        {
            // Simple query string
            baseQuery = new QueryStringQuery { Query = _query };
        }

        // Add timestamp filter for incremental mode
        if (_incrementalMode == ElasticsearchConnectorConfig.IncrementalModeTimestamp && _lastTimestampValue != null)
        {
            return new BoolQuery
            {
                Must = [baseQuery],
                // Strictly greater than the cursor - an equality filter would never return
                // anything newer than the last document already exported.
                Filter = [new UntypedRangeQuery { Field = new Field(_incrementalField), Gt = _lastTimestampValue }]
            };
        }

        return baseQuery;
    }

    private List<SourceRecord> ConvertHits(IReadOnlyCollection<Elastic.Clients.Elasticsearch.Core.Search.Hit<JsonDocument>> hits)
    {
        var records = new List<SourceRecord>();

        foreach (var hit in hits)
        {
            if (hit.Source == null)
                continue;

            var sourceOffset = new Dictionary<string, object>();

            if (_scrollId != null)
            {
                sourceOffset["scroll_id"] = _scrollId;
            }

            if (hit.Sort != null && hit.Sort.Count > 0)
            {
                sourceOffset["sort_values"] = JsonSerializer.Serialize(hit.Sort.Select(SerializeFieldValue));
            }

            if (_incrementalMode == ElasticsearchConnectorConfig.IncrementalModeTimestamp)
            {
                var ts = ExtractTimestampValue(hit.Source);
                if (ts != null)
                {
                    sourceOffset["last_timestamp"] = ts;
                }
            }

            records.Add(new SourceRecord
            {
                SourcePartition = _sourcePartition,
                SourceOffset = sourceOffset,
                Topic = _topic,
                Key = Encoding.UTF8.GetBytes(hit.Id ?? ""),
                Value = JsonSerializer.SerializeToUtf8Bytes(hit.Source),
                Timestamp = DateTimeOffset.UtcNow,
                Headers = new Dictionary<string, byte[]>
                {
                    ["elasticsearch.index"] = Encoding.UTF8.GetBytes(hit.Index ?? _index),
                    ["elasticsearch.id"] = Encoding.UTF8.GetBytes(hit.Id ?? "")
                }
            });
        }

        return records;
    }

    private static object SerializeFieldValue(FieldValue fv)
    {
        if (fv.TryGetLong(out var l)) return l;
        if (fv.TryGetDouble(out var d)) return d;
        if (fv.TryGetBool(out var b)) return b;
        if (fv.TryGetString(out var s)) return s ?? "";
        return fv.ToString() ?? "";
    }

    private string? ExtractTimestampValue(JsonDocument? doc)
    {
        if (doc == null)
            return null;

        try
        {
            if (doc.RootElement.TryGetProperty(_incrementalField, out var prop))
            {
                return prop.ValueKind switch
                {
                    JsonValueKind.String => prop.GetString(),
                    JsonValueKind.Number => prop.GetRawText(),
                    _ => null
                };
            }
        }
        catch
        {
            // Ignore extraction errors
        }

        return null;
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Nothing to do - offsets are tracked in SourceOffset
        return Task.CompletedTask;
    }
}
