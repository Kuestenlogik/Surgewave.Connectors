namespace Kuestenlogik.Surgewave.Connector.Wikipedia.Tests;

/// <summary>
/// A Wikipedia source that cannot produce anything must say so at startup instead of polling
/// silently forever.
/// </summary>
public class WikipediaSourceConnectorTests
{
    [Fact]
    public void Start_WithoutTopic_Throws()
    {
        using var connector = new WikipediaSourceConnector();

        var config = SearchConfig();
        config.Remove(WikipediaConnectorConfig.Topic);

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithModeThatIsOnlyAdvertised_ThrowsNamingTheImplementedModes()
    {
        using var connector = new WikipediaSourceConnector();

        var config = SearchConfig();
        config[WikipediaConnectorConfig.Mode] = "stream";

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(WikipediaConnectorConfig.SupportedModes, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_SearchModeWithoutQuery_Throws()
    {
        using var connector = new WikipediaSourceConnector();

        var config = SearchConfig();
        config.Remove(WikipediaConnectorConfig.SearchQuery);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(WikipediaConnectorConfig.SearchQuery, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_PageModeWithNeitherTitlesNorCategories_Throws()
    {
        using var connector = new WikipediaSourceConnector();

        var config = SearchConfig();
        config[WikipediaConnectorConfig.Mode] = "page";
        config.Remove(WikipediaConnectorConfig.SearchQuery);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(WikipediaConnectorConfig.Categories, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_PageModeWithOnlyCategories_IsAccepted()
    {
        using var connector = new WikipediaSourceConnector();

        var config = SearchConfig();
        config[WikipediaConnectorConfig.Mode] = "page";
        config.Remove(WikipediaConnectorConfig.SearchQuery);
        config[WikipediaConnectorConfig.Categories] = "Physics";

        connector.Start(config);

        var taskConfig = Assert.Single(connector.TaskConfigs(3));
        Assert.Equal("Physics", taskConfig[WikipediaConnectorConfig.Categories]);
        Assert.NotSame(config, taskConfig);
    }

    private static Dictionary<string, string> SearchConfig() => new()
    {
        [WikipediaConnectorConfig.Topic] = "wiki",
        [WikipediaConnectorConfig.Mode] = "search",
        [WikipediaConnectorConfig.SearchQuery] = "kafka"
    };
}
