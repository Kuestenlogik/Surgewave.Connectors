namespace Kuestenlogik.Surgewave.Connector.Azure.Blob.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class AzureBlobSourceConnectorTests
{
    [Fact]
    public void AzureBlobSourceConnector_HasCorrectVersion()
    {
        using var connector = new AzureBlobSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void AzureBlobSourceConnector_HasCorrectTaskClass()
    {
        using var connector = new AzureBlobSourceConnector();
        Assert.Equal(typeof(AzureBlobSourceTask), connector.TaskClass);
    }

    [Fact]
    public void AzureBlobSourceConnector_Config_HasConnectionKeys()
    {
        using var connector = new AzureBlobSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "azure.storage.connection.string");
        Assert.Contains(config.Keys, k => k.Name == "azure.storage.account.name");
        Assert.Contains(config.Keys, k => k.Name == "azure.storage.account.key");
        Assert.Contains(config.Keys, k => k.Name == "azure.storage.container.name");
        Assert.Contains(config.Keys, k => k.Name == "azure.storage.endpoint");
    }

    [Fact]
    public void AzureBlobSourceConnector_Config_HasSourceKeys()
    {
        using var connector = new AzureBlobSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "topic");
        Assert.Contains(config.Keys, k => k.Name == "azure.blob.prefix");
        Assert.Contains(config.Keys, k => k.Name == "format");
        Assert.Contains(config.Keys, k => k.Name == "poll.interval.ms");
        Assert.Contains(config.Keys, k => k.Name == "delete.after.read");
    }

    [Fact]
    public void AzureBlobSourceConnector_Start_ThrowsOnMissingConnectionInfo()
    {
        using var connector = new AzureBlobSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.container.name"] = "test-container",
            ["topic"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("azure.storage.connection.string", ex.Message);
    }

    [Fact]
    public void AzureBlobSourceConnector_Start_ThrowsOnMissingContainerName()
    {
        using var connector = new AzureBlobSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["topic"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("azure.storage.container.name", ex.Message);
    }

    [Fact]
    public void AzureBlobSourceConnector_Start_ThrowsOnMissingTopic()
    {
        using var connector = new AzureBlobSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topic", ex.Message);
    }

    [Fact]
    public void AzureBlobSourceConnector_Start_AcceptsConnectionString()
    {
        using var connector = new AzureBlobSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container",
            ["topic"] = "test-topic"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void AzureBlobSourceConnector_Start_AcceptsAccountNameAndKey()
    {
        using var connector = new AzureBlobSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.account.name"] = "testaccount",
            ["azure.storage.account.key"] = "dGVzdGtleQ==",
            ["azure.storage.container.name"] = "test-container",
            ["topic"] = "test-topic"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData("invalid")]
    [InlineData("xml")]
    [InlineData("parquet")]
    public void AzureBlobSourceConnector_Start_ThrowsOnInvalidFormat(string format)
    {
        using var connector = new AzureBlobSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container",
            ["topic"] = "test-topic",
            ["format"] = format
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("format", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("json")]
    [InlineData("jsonlines")]
    [InlineData("csv")]
    [InlineData("raw")]
    public void AzureBlobSourceConnector_Start_AcceptsValidFormat(string format)
    {
        using var connector = new AzureBlobSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container",
            ["topic"] = "test-topic",
            ["format"] = format
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void AzureBlobSourceConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new AzureBlobSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container",
            ["topic"] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        Assert.Single(taskConfigs);
    }

    [Fact]
    public void AzureBlobSourceConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new AzureBlobSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container",
            ["topic"] = "test-topic",
            ["azure.blob.prefix"] = "data/",
            ["format"] = "jsonlines",
            ["poll.interval.ms"] = "5000"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("test-container", taskConfigs[0]["azure.storage.container.name"]);
        Assert.Equal("test-topic", taskConfigs[0]["topic"]);
        Assert.Equal("data/", taskConfigs[0]["azure.blob.prefix"]);
        Assert.Equal("jsonlines", taskConfigs[0]["format"]);
        Assert.Equal("5000", taskConfigs[0]["poll.interval.ms"]);
    }

    [Fact]
    public void AzureBlobSourceConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new AzureBlobSourceConnector();
        var config = connector.Config;

        var formatKey = config.Keys.First(k => k.Name == "format");
        Assert.Equal("json", formatKey.DefaultValue);

        var pollIntervalKey = config.Keys.First(k => k.Name == "poll.interval.ms");
        Assert.Equal(10000L, pollIntervalKey.DefaultValue);

        var deleteAfterReadKey = config.Keys.First(k => k.Name == "delete.after.read");
        Assert.Equal(false, deleteAfterReadKey.DefaultValue);
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
