namespace Kuestenlogik.Surgewave.Connector.Wikipedia;

/// <summary>
/// Configuration constants for Wikipedia connector.
/// </summary>
public static class WikipediaConnectorConfig
{
    // API settings
    public const string Language = "wikipedia.language";
    public const string ApiUrl = "wikipedia.api.url";

    // Source settings
    public const string Topic = "topic";
    public const string Mode = "wikipedia.mode";  // search, page, changes, random
    public const string SearchQuery = "wikipedia.search.query";
    public const string PageTitles = "wikipedia.page.titles";
    public const string Categories = "wikipedia.categories";
    public const string PollIntervalMs = "poll.interval.ms";

    // Content settings
    public const string IncludeContent = "wikipedia.include.content";
    public const string IncludeExtract = "wikipedia.include.extract";
    public const string ExtractLength = "wikipedia.extract.length";
    public const string IncludeLinks = "wikipedia.include.links";
    public const string IncludeImages = "wikipedia.include.images";
    public const string IncludeCategories = "wikipedia.include.categories";

    // Recent changes settings
    public const string ChangesNamespace = "wikipedia.changes.namespace";
    public const string ChangesTypes = "wikipedia.changes.types";  // edit, new, log
    public const string ChangesLimit = "wikipedia.changes.limit";

    // Defaults
    public const string DefaultLanguage = "en";
    public const string DefaultMode = "search";
    public const int DefaultPollIntervalMs = 60000;
    public const int DefaultExtractLength = 500;
    public const int DefaultChangesLimit = 50;
}
