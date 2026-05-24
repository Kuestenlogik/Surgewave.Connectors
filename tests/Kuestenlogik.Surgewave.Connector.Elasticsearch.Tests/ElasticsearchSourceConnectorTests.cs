namespace Kuestenlogik.Surgewave.Connector.Elasticsearch.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class ElasticsearchSourceConnectorTests
{
    [Fact]
    public void ElasticsearchSourceConnector_HasCorrectVersion()
    {
        using var connector = new ElasticsearchSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void ElasticsearchSourceConnector_HasCorrectTaskClass()
    {
        using var connector = new ElasticsearchSourceConnector();
        Assert.Equal(typeof(ElasticsearchSourceTask), connector.TaskClass);
    }

    [Fact]
    public void ElasticsearchSourceConnector_Config_HasRequiredKeys()
    {
        using var connector = new ElasticsearchSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.url" && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == "topic" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.index" && k.Type == ConfigType.String);
    }

    [Fact]
    public void ElasticsearchSourceConnector_Config_HasOptionalKeys()
    {
        using var connector = new ElasticsearchSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.api.key");
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.username");
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.password");
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.cloud.id");
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.query");
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.scroll.mode");
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.scroll.size");
        Assert.Contains(config.Keys, k => k.Name == "poll.interval.ms");
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.incremental.mode");
    }

    [Fact]
    public void ElasticsearchSourceConnector_Start_ThrowsOnMissingUrlAndCloudId()
    {
        using var connector = new ElasticsearchSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["topic"] = "test-topic",
            ["elasticsearch.index"] = "logs-*"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("elasticsearch.url", ex.Message);
    }

    [Fact]
    public void ElasticsearchSourceConnector_Start_ThrowsOnMissingTopic()
    {
        using var connector = new ElasticsearchSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["elasticsearch.index"] = "logs-*"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topic", ex.Message);
    }

    [Fact]
    public void ElasticsearchSourceConnector_Start_ThrowsOnMissingIndex()
    {
        using var connector = new ElasticsearchSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topic"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("elasticsearch.index", ex.Message);
    }

    [Fact]
    public void ElasticsearchSourceConnector_Start_AcceptsValidConfig()
    {
        using var connector = new ElasticsearchSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topic"] = "test-topic",
            ["elasticsearch.index"] = "logs-*"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ElasticsearchSourceConnector_Start_AcceptsCloudIdConfig()
    {
        using var connector = new ElasticsearchSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.cloud.id"] = "my-cloud:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDEyMzQ=",
            ["topic"] = "test-topic",
            ["elasticsearch.index"] = "logs-*"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData("scroll")]
    [InlineData("search_after")]
    public void ElasticsearchSourceConnector_Start_AcceptsValidScrollModes(string mode)
    {
        using var connector = new ElasticsearchSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topic"] = "test-topic",
            ["elasticsearch.index"] = "logs-*",
            ["elasticsearch.scroll.mode"] = mode
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ElasticsearchSourceConnector_Start_ThrowsOnInvalidScrollMode()
    {
        using var connector = new ElasticsearchSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topic"] = "test-topic",
            ["elasticsearch.index"] = "logs-*",
            ["elasticsearch.scroll.mode"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid scroll mode", ex.Message);
    }

    [Theory]
    [InlineData("none")]
    [InlineData("timestamp")]
    public void ElasticsearchSourceConnector_Start_AcceptsValidIncrementalModes(string mode)
    {
        using var connector = new ElasticsearchSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topic"] = "test-topic",
            ["elasticsearch.index"] = "logs-*",
            ["elasticsearch.incremental.mode"] = mode
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ElasticsearchSourceConnector_Start_ThrowsOnInvalidIncrementalMode()
    {
        using var connector = new ElasticsearchSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topic"] = "test-topic",
            ["elasticsearch.index"] = "logs-*",
            ["elasticsearch.incremental.mode"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid incremental mode", ex.Message);
    }

    [Fact]
    public void ElasticsearchSourceConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new ElasticsearchSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topic"] = "test-topic",
            ["elasticsearch.index"] = "logs-*"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("https://localhost:9200", taskConfigs[0]["elasticsearch.url"]);
        Assert.Equal("test-topic", taskConfigs[0]["topic"]);
        Assert.Equal("logs-*", taskConfigs[0]["elasticsearch.index"]);
    }

    [Fact]
    public void ElasticsearchSourceConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new ElasticsearchSourceConnector();
        var config = connector.Config;

        var queryKey = config.Keys.First(k => k.Name == "elasticsearch.query");
        Assert.Equal("*", queryKey.DefaultValue);

        var scrollModeKey = config.Keys.First(k => k.Name == "elasticsearch.scroll.mode");
        Assert.Equal("search_after", scrollModeKey.DefaultValue);

        var scrollSizeKey = config.Keys.First(k => k.Name == "elasticsearch.scroll.size");
        Assert.Equal(500L, scrollSizeKey.DefaultValue);

        var pollIntervalKey = config.Keys.First(k => k.Name == "poll.interval.ms");
        Assert.Equal(5000L, pollIntervalKey.DefaultValue);

        var incrementalModeKey = config.Keys.First(k => k.Name == "elasticsearch.incremental.mode");
        Assert.Equal("none", incrementalModeKey.DefaultValue);

        var incrementalFieldKey = config.Keys.First(k => k.Name == "elasticsearch.incremental.field");
        Assert.Equal("@timestamp", incrementalFieldKey.DefaultValue);
    }

    private static ConnectorContext CreateContext()
    {
        return new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { },
            Logger = null
        };
    }
}
