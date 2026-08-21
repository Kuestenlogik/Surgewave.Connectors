using System.Diagnostics.CodeAnalysis;
using System.Globalization;
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
    /// <summary>Upper bound for the recent-changes de-duplication set.</summary>
    private const int MaxProcessedRevisions = 10000;

    /// <summary>Maximum number of member pages fetched per configured category.</summary>
    private const int CategoryMemberLimit = 50;

    private readonly HashSet<int> _processedRevisions = [];
    private readonly HttpMessageHandler? _messageHandler;
    private HttpClient? _httpClient;
    private string _topic = null!;
    private string _language = WikipediaConnectorConfig.DefaultLanguage;
    private string _mode = WikipediaConnectorConfig.DefaultMode;
    private string? _searchQuery;
    private List<string> _pageTitles = [];
    private List<string> _categories = [];
    private int _pollIntervalMs;
    private bool _includeContent;
    private bool _includeExtract;
    private int _extractLength;
    private bool _includeLinks;
    private bool _includeImages;
    private bool _includeCategories;
    private int _changesLimit;
    private DateTime _lastPoll = DateTime.MinValue;
    private string? _lastChangeTimestamp;
    private long _messageId;

    public override string Version => "1.0.0";

    /// <summary>
    /// Creates a task that talks to the MediaWiki API over a default <see cref="HttpClient"/>.
    /// </summary>
    public WikipediaSourceTask()
    {
    }

    /// <summary>
    /// Creates a task whose HTTP traffic runs through <paramref name="messageHandler"/>. The handler
    /// stays owned by the caller.
    /// </summary>
    internal WikipediaSourceTask(HttpMessageHandler messageHandler)
    {
        _messageHandler = messageHandler;
    }

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[WikipediaConnectorConfig.Topic];
        _language = config.TryGetValue(WikipediaConnectorConfig.Language, out var lang) && !string.IsNullOrWhiteSpace(lang)
            ? lang : WikipediaConnectorConfig.DefaultLanguage;
        _mode = config.TryGetValue(WikipediaConnectorConfig.Mode, out var mode) && !string.IsNullOrWhiteSpace(mode)
            ? mode : WikipediaConnectorConfig.DefaultMode;
        WikipediaConnectorConfig.ValidateMode(_mode);
        _searchQuery = config.TryGetValue(WikipediaConnectorConfig.SearchQuery, out var searchQuery) ? searchQuery : null;
        _pollIntervalMs = config.TryGetValue(WikipediaConnectorConfig.PollIntervalMs, out var pollInterval) && !string.IsNullOrWhiteSpace(pollInterval)
            ? int.Parse(pollInterval, CultureInfo.InvariantCulture)
            : WikipediaConnectorConfig.DefaultPollIntervalMs;
        _includeContent = (config.TryGetValue(WikipediaConnectorConfig.IncludeContent, out var includeContent) ? includeContent : "false") == "true";
        _includeExtract = (config.TryGetValue(WikipediaConnectorConfig.IncludeExtract, out var includeExtract) ? includeExtract : "true") == "true";
        _extractLength = config.TryGetValue(WikipediaConnectorConfig.ExtractLength, out var extractLength) && !string.IsNullOrWhiteSpace(extractLength)
            ? int.Parse(extractLength, CultureInfo.InvariantCulture)
            : WikipediaConnectorConfig.DefaultExtractLength;
        _includeLinks = (config.TryGetValue(WikipediaConnectorConfig.IncludeLinks, out var includeLinks) ? includeLinks : "false") == "true";
        _includeImages = (config.TryGetValue(WikipediaConnectorConfig.IncludeImages, out var includeImages) ? includeImages : "false") == "true";
        _includeCategories = (config.TryGetValue(WikipediaConnectorConfig.IncludeCategories, out var includeCategories) ? includeCategories : "true") == "true";
        _changesLimit = config.TryGetValue(WikipediaConnectorConfig.ChangesLimit, out var changesLimit) && !string.IsNullOrWhiteSpace(changesLimit)
            ? int.Parse(changesLimit, CultureInfo.InvariantCulture)
            : WikipediaConnectorConfig.DefaultChangesLimit;

        if (config.TryGetValue(WikipediaConnectorConfig.PageTitles, out var titles) && !string.IsNullOrWhiteSpace(titles))
        {
            _pageTitles = titles.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
        }

        if (config.TryGetValue(WikipediaConnectorConfig.Categories, out var categories) && !string.IsNullOrWhiteSpace(categories))
        {
            _categories = categories.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
        }

        if (_mode == "search" && string.IsNullOrWhiteSpace(_searchQuery))
        {
            throw new ArgumentException(
                $"'{WikipediaConnectorConfig.SearchQuery}' is required for search mode", nameof(config));
        }

        if (_mode == "page" && _pageTitles.Count == 0 && _categories.Count == 0)
        {
            throw new ArgumentException(
                $"'{WikipediaConnectorConfig.PageTitles}' or '{WikipediaConnectorConfig.Categories}' is required for page mode",
                nameof(config));
        }

        // Restore the recent-changes cursor so a restart resumes instead of replaying history
        var storedOffset = Context?.OffsetStorageReader?.Offset(CreateChangesPartition());
        if (storedOffset != null &&
            storedOffset.TryGetValue(WikipediaConnectorConfig.OffsetTimestamp, out var storedTimestamp))
        {
            var timestamp = storedTimestamp?.ToString();
            if (!string.IsNullOrWhiteSpace(timestamp))
            {
                _lastChangeTimestamp = timestamp;
            }
        }

        _httpClient = _messageHandler is null
            ? new HttpClient()
            : new HttpClient(_messageHandler, disposeHandler: false);
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
                default:
                    throw new InvalidOperationException($"Unsupported mode: {_mode}");
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Surface the failure to the framework instead of producing nothing forever
            Context?.RaiseError?.Invoke(ex);
        }

        return records;
    }

    private string GetApiUrl() => $"https://{_language}.wikipedia.org/w/api.php";

    private Dictionary<string, object> CreateChangesPartition() => new()
    {
        ["source"] = "wikipedia",
        ["language"] = _language,
        ["mode"] = "changes"
    };

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
                var title = result.GetProperty("title").GetString()!;

                // Fetch full page info if needed
                using var pageInfo = await FetchPageInfoAsync(title, cancellationToken);
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
        var titles = new List<string>(_pageTitles);

        foreach (var category in _categories)
        {
            titles.AddRange(await FetchCategoryMemberTitlesAsync(category, cancellationToken));
        }

        foreach (var title in titles.Distinct(StringComparer.Ordinal))
        {
            using var pageInfo = await FetchPageInfoAsync(title, cancellationToken);
            if (pageInfo != null)
            {
                records.Add(CreateRecord(title, "page", pageInfo));
            }
        }

        return records;
    }

    private async Task<List<string>> FetchCategoryMemberTitlesAsync(string category, CancellationToken cancellationToken)
    {
        var titles = new List<string>();
        var categoryTitle = category.StartsWith("Category:", StringComparison.OrdinalIgnoreCase) ? category : "Category:" + category;
        var url = FormattableString.Invariant(
            $"{GetApiUrl()}?action=query&list=categorymembers&cmtitle={Uri.EscapeDataString(categoryTitle)}&cmnamespace=0&cmlimit={CategoryMemberLimit}&format=json");

        var response = await _httpClient!.GetStringAsync(url, cancellationToken);
        using var doc = JsonDocument.Parse(response);

        if (doc.RootElement.TryGetProperty("query", out var query) &&
            query.TryGetProperty("categorymembers", out var members))
        {
            foreach (var member in members.EnumerateArray())
            {
                var title = member.TryGetProperty("title", out var t) ? t.GetString() : null;
                if (!string.IsNullOrEmpty(title))
                {
                    titles.Add(title);
                }
            }
        }

        return titles;
    }

    private async Task<List<SourceRecord>> FetchRecentChangesAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();
        var url = FormattableString.Invariant(
            $"{GetApiUrl()}?action=query&list=recentchanges&rcprop=title|ids|user|timestamp|comment|sizes&format=json&rclimit={_changesLimit}");

        if (_lastChangeTimestamp != null)
        {
            // Walk forward from the newest change already delivered - the API default (rcdir=older)
            // would enumerate progressively older history and never return a new edit.
            url += $"&rcdir=newer&rcstart={Uri.EscapeDataString(_lastChangeTimestamp)}";
        }

        var response = await _httpClient!.GetStringAsync(url, cancellationToken);
        using var doc = JsonDocument.Parse(response);

        if (doc.RootElement.TryGetProperty("query", out var query) &&
            query.TryGetProperty("recentchanges", out var changes))
        {
            foreach (var change in changes.EnumerateArray())
            {
                var revId = change.GetProperty("revid").GetInt32();
                if (!_processedRevisions.Add(revId)) continue;

                var timestamp = change.GetProperty("timestamp").GetString();

                var payload = new
                {
                    type = "recent_change",
                    revid = revId,
                    title = change.GetProperty("title").GetString(),
                    user = change.TryGetProperty("user", out var u) ? u.GetString() : null,
                    timestamp,
                    comment = change.TryGetProperty("comment", out var c) ? c.GetString() : null,
                    oldlen = change.TryGetProperty("oldlen", out var ol) ? ol.GetInt32() : 0,
                    newlen = change.TryGetProperty("newlen", out var nl) ? nl.GetInt32() : 0
                };

                records.Add(new SourceRecord
                {
                    SourcePartition = CreateChangesPartition(),
                    SourceOffset = new Dictionary<string, object>
                    {
                        ["message_id"] = Interlocked.Increment(ref _messageId),
                        [WikipediaConnectorConfig.OffsetRevisionId] = revId,
                        [WikipediaConnectorConfig.OffsetTimestamp] = timestamp ?? string.Empty
                    },
                    Topic = _topic,
                    Key = Encoding.UTF8.GetBytes(revId.ToString(CultureInfo.InvariantCulture)),
                    Value = JsonSerializer.SerializeToUtf8Bytes(payload),
                    Headers = new Dictionary<string, byte[]>
                    {
                        ["wikipedia.type"] = Encoding.UTF8.GetBytes("recent_change"),
                        ["wikipedia.language"] = Encoding.UTF8.GetBytes(_language)
                    }
                });

                // The cursor is the NEWEST timestamp seen, not the last one enumerated
                if (timestamp != null && string.CompareOrdinal(timestamp, _lastChangeTimestamp) > 0)
                {
                    _lastChangeTimestamp = timestamp;
                }
            }

            TrimProcessedRevisions();
        }

        return records;
    }

    /// <summary>
    /// Keeps the de-duplication set bounded so a long-running task cannot leak memory.
    /// </summary>
    private void TrimProcessedRevisions()
    {
        if (_processedRevisions.Count <= MaxProcessedRevisions)
        {
            return;
        }

        var retained = _processedRevisions.Skip(_processedRevisions.Count - (MaxProcessedRevisions / 2)).ToArray();
        _processedRevisions.Clear();
        foreach (var revId in retained)
        {
            _processedRevisions.Add(revId);
        }
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
                using var pageInfo = await FetchPageInfoAsync(title, cancellationToken);
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
        if (_includeContent) props.Add("revisions");
        if (_includeLinks) props.Add("links");
        if (_includeImages) props.Add("images");

        var url = $"{GetApiUrl()}?action=query&titles={Uri.EscapeDataString(title)}&prop={string.Join("|", props)}&format=json";

        if (_includeExtract)
        {
            url += FormattableString.Invariant($"&exintro=1&explaintext=1&exchars={_extractLength}");
        }

        if (_includeContent)
        {
            url += "&rvprop=content&rvslots=main";
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
