namespace Kuestenlogik.Surgewave.Connector.Elasticsearch.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class ElasticsearchSinkConnectorTests
{
    [Fact]
    public void ElasticsearchSinkConnector_HasCorrectVersion()
    {
        using var connector = new ElasticsearchSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void ElasticsearchSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new ElasticsearchSinkConnector();
        Assert.Equal(typeof(ElasticsearchSinkTask), connector.TaskClass);
    }

    [Fact]
    public void ElasticsearchSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new ElasticsearchSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.url" && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == "topics" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.index" && k.Type == ConfigType.String);
    }

    [Fact]
    public void ElasticsearchSinkConnector_Config_HasOptionalKeys()
    {
        using var connector = new ElasticsearchSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.api.key");
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.username");
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.password");
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.cloud.id");
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.index.strategy");
        Assert.Contains(config.Keys, k => k.Name == "elasticsearch.document.id.strategy");
        Assert.Contains(config.Keys, k => k.Name == "batch.size");
        Assert.Contains(config.Keys, k => k.Name == "retry.max");
    }

    [Fact]
    public void ElasticsearchSinkConnector_Start_ThrowsOnMissingUrlAndCloudId()
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["topics"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("elasticsearch.url", ex.Message);
    }

    [Fact]
    public void ElasticsearchSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message);
    }

    [Fact]
    public void ElasticsearchSinkConnector_Start_AcceptsUrlWithTopics()
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topics"] = "test-topic"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ElasticsearchSinkConnector_Start_AcceptsCloudIdWithTopics()
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.cloud.id"] = "my-cloud:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDEyMzQ=",
            ["topics"] = "test-topic"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData("static")]
    [InlineData("topic")]
    [InlineData("time")]
    public void ElasticsearchSinkConnector_Start_AcceptsValidIndexStrategies(string strategy)
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topics"] = "test-topic",
            ["elasticsearch.index.strategy"] = strategy
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ElasticsearchSinkConnector_Start_FieldStrategyRequiresFieldConfig()
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topics"] = "test-topic",
            ["elasticsearch.index.strategy"] = "field"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("elasticsearch.index.field", ex.Message);
    }

    [Fact]
    public void ElasticsearchSinkConnector_Start_FieldStrategyWithFieldConfigSucceeds()
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topics"] = "test-topic",
            ["elasticsearch.index.strategy"] = "field",
            ["elasticsearch.index.field"] = "index_name"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ElasticsearchSinkConnector_Start_ThrowsOnInvalidIndexStrategy()
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topics"] = "test-topic",
            ["elasticsearch.index.strategy"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid index strategy", ex.Message);
    }

    [Theory]
    [InlineData("auto")]
    [InlineData("key")]
    public void ElasticsearchSinkConnector_Start_AcceptsValidDocIdStrategies(string strategy)
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topics"] = "test-topic",
            ["elasticsearch.document.id.strategy"] = strategy
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ElasticsearchSinkConnector_Start_FieldDocIdStrategyRequiresField()
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topics"] = "test-topic",
            ["elasticsearch.document.id.strategy"] = "field"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("elasticsearch.document.id.field", ex.Message);
    }

    [Fact]
    public void ElasticsearchSinkConnector_Start_CompositeDocIdStrategyRequiresFields()
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topics"] = "test-topic",
            ["elasticsearch.document.id.strategy"] = "composite"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("elasticsearch.document.id.composite.fields", ex.Message);
    }

    [Fact]
    public void ElasticsearchSinkConnector_Start_ThrowsOnInvalidDocIdStrategy()
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topics"] = "test-topic",
            ["elasticsearch.document.id.strategy"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid document ID strategy", ex.Message);
    }

    [Theory]
    [InlineData("index")]
    [InlineData("create")]
    [InlineData("upsert")]
    public void ElasticsearchSinkConnector_Start_AcceptsValidWriteMethods(string method)
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topics"] = "test-topic",
            ["elasticsearch.write.method"] = method
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ElasticsearchSinkConnector_Start_ThrowsOnInvalidWriteMethod()
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topics"] = "test-topic",
            ["elasticsearch.write.method"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid write method", ex.Message);
    }

    [Fact]
    public void ElasticsearchSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new ElasticsearchSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["elasticsearch.url"] = "https://localhost:9200",
            ["topics"] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("https://localhost:9200", taskConfigs[0]["elasticsearch.url"]);
        Assert.Equal("test-topic", taskConfigs[0]["topics"]);
    }

    [Fact]
    public void ElasticsearchSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new ElasticsearchSinkConnector();
        var config = connector.Config;

        var indexKey = config.Keys.First(k => k.Name == "elasticsearch.index");
        Assert.Equal("${topic}", indexKey.DefaultValue);

        var strategyKey = config.Keys.First(k => k.Name == "elasticsearch.index.strategy");
        Assert.Equal("topic", strategyKey.DefaultValue);

        var docIdKey = config.Keys.First(k => k.Name == "elasticsearch.document.id.strategy");
        Assert.Equal("auto", docIdKey.DefaultValue);

        var writeMethodKey = config.Keys.First(k => k.Name == "elasticsearch.write.method");
        Assert.Equal("index", writeMethodKey.DefaultValue);

        var batchKey = config.Keys.First(k => k.Name == "batch.size");
        Assert.Equal(100L, batchKey.DefaultValue);

        var retryMaxKey = config.Keys.First(k => k.Name == "retry.max");
        Assert.Equal(3L, retryMaxKey.DefaultValue);
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
