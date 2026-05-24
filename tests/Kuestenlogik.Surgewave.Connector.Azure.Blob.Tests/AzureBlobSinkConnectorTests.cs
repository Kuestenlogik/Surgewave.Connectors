namespace Kuestenlogik.Surgewave.Connector.Azure.Blob.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class AzureBlobSinkConnectorTests
{
    [Fact]
    public void AzureBlobSinkConnector_HasCorrectVersion()
    {
        using var connector = new AzureBlobSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void AzureBlobSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new AzureBlobSinkConnector();
        Assert.Equal(typeof(AzureBlobSinkTask), connector.TaskClass);
    }

    [Fact]
    public void AzureBlobSinkConnector_Config_HasConnectionKeys()
    {
        using var connector = new AzureBlobSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "azure.storage.connection.string");
        Assert.Contains(config.Keys, k => k.Name == "azure.storage.account.name");
        Assert.Contains(config.Keys, k => k.Name == "azure.storage.account.key");
        Assert.Contains(config.Keys, k => k.Name == "azure.storage.container.name");
        Assert.Contains(config.Keys, k => k.Name == "azure.storage.endpoint");
    }

    [Fact]
    public void AzureBlobSinkConnector_Config_HasSinkKeys()
    {
        using var connector = new AzureBlobSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "topics");
        Assert.Contains(config.Keys, k => k.Name == "azure.blob.prefix");
        Assert.Contains(config.Keys, k => k.Name == "format");
        Assert.Contains(config.Keys, k => k.Name == "partitioner");
        Assert.Contains(config.Keys, k => k.Name == "flush.size");
        Assert.Contains(config.Keys, k => k.Name == "rotate.interval.ms");
    }

    [Fact]
    public void AzureBlobSinkConnector_Start_ThrowsOnMissingConnectionInfo()
    {
        using var connector = new AzureBlobSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.container.name"] = "test-container",
            ["topics"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("azure.storage.connection.string", ex.Message);
    }

    [Fact]
    public void AzureBlobSinkConnector_Start_ThrowsOnMissingContainerName()
    {
        using var connector = new AzureBlobSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["topics"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("azure.storage.container.name", ex.Message);
    }

    [Fact]
    public void AzureBlobSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new AzureBlobSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message);
    }

    [Fact]
    public void AzureBlobSinkConnector_Start_AcceptsConnectionString()
    {
        using var connector = new AzureBlobSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container",
            ["topics"] = "test-topic"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void AzureBlobSinkConnector_Start_AcceptsAccountNameAndKey()
    {
        using var connector = new AzureBlobSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.account.name"] = "testaccount",
            ["azure.storage.account.key"] = "dGVzdGtleQ==",
            ["azure.storage.container.name"] = "test-container",
            ["topics"] = "test-topic"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData("invalid")]
    [InlineData("csv")]
    [InlineData("parquet")]
    public void AzureBlobSinkConnector_Start_ThrowsOnInvalidFormat(string format)
    {
        using var connector = new AzureBlobSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container",
            ["topics"] = "test-topic",
            ["format"] = format
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("format", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("json")]
    [InlineData("jsonlines")]
    public void AzureBlobSinkConnector_Start_AcceptsValidFormat(string format)
    {
        using var connector = new AzureBlobSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container",
            ["topics"] = "test-topic",
            ["format"] = format
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData("invalid")]
    [InlineData("random")]
    public void AzureBlobSinkConnector_Start_ThrowsOnInvalidPartitioner(string partitioner)
    {
        using var connector = new AzureBlobSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container",
            ["topics"] = "test-topic",
            ["partitioner"] = partitioner
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("partitioner", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("default")]
    [InlineData("time")]
    [InlineData("field")]
    public void AzureBlobSinkConnector_Start_AcceptsValidPartitioner(string partitioner)
    {
        using var connector = new AzureBlobSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container",
            ["topics"] = "test-topic",
            ["partitioner"] = partitioner
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void AzureBlobSinkConnector_TaskConfigs_ReturnsRequestedNumberOfTasks()
    {
        using var connector = new AzureBlobSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container",
            ["topics"] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Equal(3, taskConfigs.Count);
    }

    [Fact]
    public void AzureBlobSinkConnector_TaskConfigs_AssignsUniqueTaskIds()
    {
        using var connector = new AzureBlobSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container",
            ["topics"] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Equal("0", taskConfigs[0]["task.id"]);
        Assert.Equal("1", taskConfigs[1]["task.id"]);
        Assert.Equal("2", taskConfigs[2]["task.id"]);
    }

    [Fact]
    public void AzureBlobSinkConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new AzureBlobSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.storage.connection.string"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ["azure.storage.container.name"] = "test-container",
            ["topics"] = "test-topic",
            ["azure.blob.prefix"] = "output/",
            ["format"] = "jsonlines",
            ["partitioner"] = "time",
            ["flush.size"] = "500"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("test-container", taskConfigs[0]["azure.storage.container.name"]);
        Assert.Equal("test-topic", taskConfigs[0]["topics"]);
        Assert.Equal("output/", taskConfigs[0]["azure.blob.prefix"]);
        Assert.Equal("jsonlines", taskConfigs[0]["format"]);
        Assert.Equal("time", taskConfigs[0]["partitioner"]);
        Assert.Equal("500", taskConfigs[0]["flush.size"]);
    }

    [Fact]
    public void AzureBlobSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new AzureBlobSinkConnector();
        var config = connector.Config;

        var formatKey = config.Keys.First(k => k.Name == "format");
        Assert.Equal("json", formatKey.DefaultValue);

        var partitionerKey = config.Keys.First(k => k.Name == "partitioner");
        Assert.Equal("default", partitionerKey.DefaultValue);

        var flushSizeKey = config.Keys.First(k => k.Name == "flush.size");
        Assert.Equal(1000L, flushSizeKey.DefaultValue);

        var rotateIntervalKey = config.Keys.First(k => k.Name == "rotate.interval.ms");
        Assert.Equal(3600000L, rotateIntervalKey.DefaultValue);
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
