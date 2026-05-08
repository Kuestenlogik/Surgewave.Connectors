using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Wikipedia;

/// <summary>
/// Source connector that fetches content from Wikipedia via MediaWiki API.
/// </summary>
[ConnectorMetadata(
    Name = "wikipedia-source",
    Description = "Fetches articles, recent changes, and content from Wikipedia via MediaWiki API",
    Author = "Surgewave",
    Tags = "wikipedia, mediawiki, source, api, knowledge")]
public sealed class WikipediaSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(WikipediaConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce Wikipedia content to", EditorHint.Topic)
        .Define(WikipediaConnectorConfig.Language, ConfigType.String, WikipediaConnectorConfig.DefaultLanguage,
            Importance.High, "Wikipedia language code (en, de, fr, etc.)")
        .Define(WikipediaConnectorConfig.Mode, ConfigType.String, WikipediaConnectorConfig.DefaultMode,
            Importance.High, "Mode: search, page, changes, random")
        .Define(WikipediaConnectorConfig.SearchQuery, ConfigType.String, "", Importance.Medium,
            "Search query for search mode")
        .Define(WikipediaConnectorConfig.PageTitles, ConfigType.List, "", Importance.Medium,
            "Comma-separated list of page titles for page mode")
        .Define(WikipediaConnectorConfig.Categories, ConfigType.List, "", Importance.Medium,
            "Comma-separated list of categories to watch")
        .Define(WikipediaConnectorConfig.PollIntervalMs, ConfigType.Int,
            WikipediaConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(WikipediaConnectorConfig.IncludeContent, ConfigType.Boolean, "false", Importance.Medium,
            "Include full page content")
        .Define(WikipediaConnectorConfig.IncludeExtract, ConfigType.Boolean, "true", Importance.Medium,
            "Include page extract/summary")
        .Define(WikipediaConnectorConfig.ExtractLength, ConfigType.Int,
            WikipediaConnectorConfig.DefaultExtractLength.ToString(), Importance.Low,
            "Maximum extract length in characters")
        .Define(WikipediaConnectorConfig.IncludeLinks, ConfigType.Boolean, "false", Importance.Low,
            "Include page links")
        .Define(WikipediaConnectorConfig.IncludeImages, ConfigType.Boolean, "false", Importance.Low,
            "Include page images")
        .Define(WikipediaConnectorConfig.IncludeCategories, ConfigType.Boolean, "true", Importance.Low,
            "Include page categories")
        .Define(WikipediaConnectorConfig.ChangesLimit, ConfigType.Int,
            WikipediaConnectorConfig.DefaultChangesLimit.ToString(), Importance.Low,
            "Maximum recent changes to fetch per poll");

    public override Type TaskClass => typeof(WikipediaSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(WikipediaConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{WikipediaConnectorConfig.Topic}' is required");
        }

        var mode = config.TryGetValue(WikipediaConnectorConfig.Mode, out var m) ? m : "search";
        if (mode == "search")
        {
            if (!config.TryGetValue(WikipediaConnectorConfig.SearchQuery, out var query) ||
                string.IsNullOrWhiteSpace(query))
            {
                throw new ArgumentException($"'{WikipediaConnectorConfig.SearchQuery}' is required for search mode");
            }
        }
        else if (mode == "page")
        {
            if (!config.TryGetValue(WikipediaConnectorConfig.PageTitles, out var titles) ||
                string.IsNullOrWhiteSpace(titles))
            {
                throw new ArgumentException($"'{WikipediaConnectorConfig.PageTitles}' is required for page mode");
            }
        }
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }

    public override void Stop()
    {
    }
}
