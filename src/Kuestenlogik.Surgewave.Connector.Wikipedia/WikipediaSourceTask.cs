using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Wikipedia;

/// <summary>
/// Task that fetches content from Wikipedia via MediaWiki API.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
[SuppressMessage("Usage", "CA2234:Pass System.Uri objects instead of strings", Justification = "String URLs are more practical for API calls")]
public sealed class WikipediaSourceTask : SourceTask
{
    private HttpClient? _httpClient;
    private string _topic = null!;
    private string _language = "en";
    private string _mode = "search";
    private string? _searchQuery;
    private List<string> _pageTitles = [];
    private int _pollIntervalMs;
    private bool _includeContent;
    private bool _includeExtract;
    private int _extractLength;
    private bool _includeCategories;
    private int _changesLimit;
    private DateTime _lastPoll = DateTime.MinValue;
    private string? _lastChangeTimestamp;
    private readonly HashSet<int> _processedRevisions = [];
    private long _messageId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[WikipediaConnectorConfig.Topic];
        _language = config.TryGetValue(WikipediaConnectorConfig.Language, out var lang) ? lang : "en";
        _mode = config.TryGetValue(WikipediaConnectorConfig.Mode, out var mode) ? mode : "search";
        _searchQuery = config.TryGetValue(WikipediaConnectorConfig.SearchQuery, out var searchQuery) ? searchQuery : null;
        _pollIntervalMs = int.Parse(config.TryGetValue(WikipediaConnectorConfig.PollIntervalMs, out var pollInterval)
            ? pollInterval : WikipediaConnectorConfig.DefaultPollIntervalMs.ToString());
        _includeContent = (config.TryGetValue(WikipediaConnectorConfig.IncludeContent, out var includeContent) ? includeContent : "false") == "true";
        _includeExtract = (config.TryGetValue(WikipediaConnectorConfig.IncludeExtract, out var includeExtract) ? includeExtract : "true") == "true";
        _extractLength = int.Parse(config.TryGetValue(WikipediaConnectorConfig.ExtractLength, out var extractLength)
            ? extractLength : WikipediaConnectorConfig.DefaultExtractLength.ToString());
        _includeCategories = (config.TryGetValue(WikipediaConnectorConfig.IncludeCategories, out var includeCategories) ? includeCategories : "true") == "true";
        _changesLimit = int.Parse(config.TryGetValue(WikipediaConnectorConfig.ChangesLimit, out var changesLimit)
            ? changesLimit : WikipediaConnectorConfig.DefaultChangesLimit.ToString());

        if (config.TryGetValue(WikipediaConnectorConfig.PageTitles, out var titles) && !string.IsNullOrWhiteSpace(titles))
        {
            _pageTitles = titles.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
        }

        _httpClient = new HttpClient();
        _httpClient.DefaultRequestHeaders.Add("User-Agent", "SurgewaveWikipediaConnector/1.0");
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if ((DateTime.UtcNow - _lastPoll).TotalMilliseconds < _pollIntervalMs)
        {
            return [];
        }

        _lastPoll = DateTime.UtcNow;
        var records = new List<SourceRecord>();

        try
        {
            switch (_mode)
            {
                case "search":
                    records.AddRange(await FetchSearchResultsAsync(cancellationToken));
                    break;
                case "page":
                    records.AddRange(await FetchPagesAsync(cancellationToken));
                    break;
                case "changes":
                    records.AddRange(await FetchRecentChangesAsync(cancellationToken));
                    break;
                case "random":
                    records.AddRange(await FetchRandomPagesAsync(cancellationToken));
                    break;
            }
        }
        catch (Exception)
        {
            // Log and continue
        }

        return records;
    }

    private string GetApiUrl() => $"https://{_language}.wikipedia.org/w/api.php";

    private async Task<List<SourceRecord>> FetchSearchResultsAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();
        var url = $"{GetApiUrl()}?action=query&list=search&srsearch={Uri.EscapeDataString(_searchQuery!)}&format=json&srlimit=10";

        var response = await _httpClient!.GetStringAsync(url, cancellationToken);
        using var doc = JsonDocument.Parse(response);

        if (doc.RootElement.TryGetProperty("query", out var query) &&
            query.TryGetProperty("search", out var results))
        {
            foreach (var result in results.EnumerateArray())
            {
                var pageId = result.GetProperty("pageid").GetInt32();
                var title = result.GetProperty("title").GetString()!;
                var snippet = result.TryGetProperty("snippet", out var s) ? s.GetString() : null;

                // Fetch full page info if needed
                var pageInfo = await FetchPageInfoAsync(title, cancellationToken);
                if (pageInfo != null)
                {
                    records.Add(CreateRecord(title, "search", pageInfo));
                }
            }
        }

        return records;
    }

    private async Task<List<SourceRecord>> FetchPagesAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        foreach (var title in _pageTitles)
        {
            var pageInfo = await FetchPageInfoAsync(title, cancellationToken);
            if (pageInfo != null)
            {
                records.Add(CreateRecord(title, "page", pageInfo));
            }
        }

        return records;
    }

    private async Task<List<SourceRecord>> FetchRecentChangesAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();
        var url = $"{GetApiUrl()}?action=query&list=recentchanges&rcprop=title|ids|user|timestamp|comment|sizes&format=json&rclimit={_changesLimit}";

        if (_lastChangeTimestamp != null)
        {
            url += $"&rcstart={_lastChangeTimestamp}";
        }

        var response = await _httpClient!.GetStringAsync(url, cancellationToken);
        using var doc = JsonDocument.Parse(response);

        if (doc.RootElement.TryGetProperty("query", out var query) &&
            query.TryGetProperty("recentchanges", out var changes))
        {
            foreach (var change in changes.EnumerateArray())
            {
                var revId = change.GetProperty("revid").GetInt32();
                if (_processedRevisions.Contains(revId)) continue;

                var payload = new
                {
                    type = "recent_change",
                    revid = revId,
                    title = change.GetProperty("title").GetString(),
                    user = change.TryGetProperty("user", out var u) ? u.GetString() : null,
                    timestamp = change.GetProperty("timestamp").GetString(),
                    comment = change.TryGetProperty("comment", out var c) ? c.GetString() : null,
                    oldlen = change.TryGetProperty("oldlen", out var ol) ? ol.GetInt32() : 0,
                    newlen = change.TryGetProperty("newlen", out var nl) ? nl.GetInt32() : 0
                };

                records.Add(new SourceRecord
                {
                    SourcePartition = new Dictionary<string, object> { ["source"] = "wikipedia" },
                    SourceOffset = new Dictionary<string, object>
                    {
                        ["message_id"] = Interlocked.Increment(ref _messageId),
                        ["revid"] = revId
                    },
                    Topic = _topic,
                    Key = Encoding.UTF8.GetBytes(revId.ToString()),
                    Value = JsonSerializer.SerializeToUtf8Bytes(payload),
                    Headers = new Dictionary<string, byte[]>
                    {
                        ["wikipedia.type"] = Encoding.UTF8.GetBytes("recent_change"),
                        ["wikipedia.language"] = Encoding.UTF8.GetBytes(_language)
                    }
                });

                _processedRevisions.Add(revId);
                _lastChangeTimestamp = change.GetProperty("timestamp").GetString();
            }
        }

        return records;
    }

    private async Task<List<SourceRecord>> FetchRandomPagesAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();
        var url = $"{GetApiUrl()}?action=query&list=random&rnlimit=5&rnnamespace=0&format=json";

        var response = await _httpClient!.GetStringAsync(url, cancellationToken);
        using var doc = JsonDocument.Parse(response);

        if (doc.RootElement.TryGetProperty("query", out var query) &&
            query.TryGetProperty("random", out var random))
        {
            foreach (var page in random.EnumerateArray())
            {
                var title = page.GetProperty("title").GetString()!;
                var pageInfo = await FetchPageInfoAsync(title, cancellationToken);
                if (pageInfo != null)
                {
                    records.Add(CreateRecord(title, "random", pageInfo));
                }
            }
        }

        return records;
    }

    private async Task<JsonDocument?> FetchPageInfoAsync(string title, CancellationToken cancellationToken)
    {
        var props = new List<string> { "info" };
        if (_includeExtract) props.Add("extracts");
        if (_includeCategories) props.Add("categories");

        var url = $"{GetApiUrl()}?action=query&titles={Uri.EscapeDataString(title)}&prop={string.Join("|", props)}&format=json";

        if (_includeExtract)
        {
            url += $"&exintro=1&explaintext=1&exchars={_extractLength}";
        }

        var response = await _httpClient!.GetStringAsync(url, cancellationToken);
        return JsonDocument.Parse(response);
    }

    private SourceRecord CreateRecord(string title, string mode, JsonDocument data)
    {
        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["source"] = "wikipedia",
                ["language"] = _language
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = Interlocked.Increment(ref _messageId),
                ["title"] = title
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(title),
            Value = JsonSerializer.SerializeToUtf8Bytes(data.RootElement),
            Headers = new Dictionary<string, byte[]>
            {
                ["wikipedia.title"] = Encoding.UTF8.GetBytes(title),
                ["wikipedia.language"] = Encoding.UTF8.GetBytes(_language),
                ["wikipedia.mode"] = Encoding.UTF8.GetBytes(mode)
            }
        };
    }

    public override void Stop()
    {
        _httpClient?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }
}
