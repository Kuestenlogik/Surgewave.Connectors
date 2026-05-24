namespace Kuestenlogik.Surgewave.Connector.Gcp.Storage.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class GcsSourceConnectorTests
{
    [Fact]
    public void GcsSourceConnector_HasCorrectVersion()
    {
        using var connector = new GcsSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void GcsSourceConnector_HasCorrectTaskClass()
    {
        using var connector = new GcsSourceConnector();
        Assert.Equal(typeof(GcsSourceTask), connector.TaskClass);
    }

    [Fact]
    public void GcsSourceConnector_Config_HasConnectionKeys()
    {
        using var connector = new GcsSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "gcs.project.id");
        Assert.Contains(config.Keys, k => k.Name == "gcs.bucket.name");
        Assert.Contains(config.Keys, k => k.Name == "gcs.credentials.json");
        Assert.Contains(config.Keys, k => k.Name == "gcs.credentials.file");
    }

    [Fact]
    public void GcsSourceConnector_Config_HasSourceKeys()
    {
        using var connector = new GcsSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "topic");
        Assert.Contains(config.Keys, k => k.Name == "gcs.prefix");
        Assert.Contains(config.Keys, k => k.Name == "format");
        Assert.Contains(config.Keys, k => k.Name == "poll.interval.ms");
        Assert.Contains(config.Keys, k => k.Name == "delete.after.read");
    }

    [Fact]
    public void GcsSourceConnector_Start_ThrowsOnMissingBucketName()
    {
        using var connector = new GcsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["topic"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("gcs.bucket.name", ex.Message);
    }

    [Fact]
    public void GcsSourceConnector_Start_ThrowsOnMissingTopic()
    {
        using var connector = new GcsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topic", ex.Message);
    }

    [Fact]
    public void GcsSourceConnector_Start_AcceptsMinimalConfig()
    {
        using var connector = new GcsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
            ["topic"] = "test-topic"
        };

        // Should not throw - will use ADC
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData("invalid")]
    [InlineData("xml")]
    [InlineData("parquet")]
    public void GcsSourceConnector_Start_ThrowsOnInvalidFormat(string format)
    {
        using var connector = new GcsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
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
    public void GcsSourceConnector_Start_AcceptsValidFormat(string format)
    {
        using var connector = new GcsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
            ["topic"] = "test-topic",
            ["format"] = format
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void GcsSourceConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new GcsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
            ["topic"] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        Assert.Single(taskConfigs);
    }

    [Fact]
    public void GcsSourceConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new GcsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
            ["topic"] = "test-topic",
            ["gcs.prefix"] = "data/",
            ["format"] = "jsonlines",
            ["poll.interval.ms"] = "5000",
            ["gcs.project.id"] = "my-project"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("test-bucket", taskConfigs[0]["gcs.bucket.name"]);
        Assert.Equal("test-topic", taskConfigs[0]["topic"]);
        Assert.Equal("data/", taskConfigs[0]["gcs.prefix"]);
        Assert.Equal("jsonlines", taskConfigs[0]["format"]);
        Assert.Equal("5000", taskConfigs[0]["poll.interval.ms"]);
        Assert.Equal("my-project", taskConfigs[0]["gcs.project.id"]);
    }

    [Fact]
    public void GcsSourceConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new GcsSourceConnector();
        var config = connector.Config;

        var formatKey = config.Keys.First(k => k.Name == "format");
        Assert.Equal("json", formatKey.DefaultValue);

        var pollIntervalKey = config.Keys.First(k => k.Name == "poll.interval.ms");
        Assert.Equal(10000L, pollIntervalKey.DefaultValue);

        var deleteAfterReadKey = config.Keys.First(k => k.Name == "delete.after.read");
        Assert.Equal(false, deleteAfterReadKey.DefaultValue);
    }

    [Fact]
    public void GcsSourceConnector_Config_HasCorrectImportanceLevels()
    {
        using var connector = new GcsSourceConnector();
        var config = connector.Config;

        var bucketNameKey = config.Keys.First(k => k.Name == "gcs.bucket.name");
        Assert.Equal(Importance.High, bucketNameKey.Importance);

        var topicKey = config.Keys.First(k => k.Name == "topic");
        Assert.Equal(Importance.High, topicKey.Importance);

        var formatKey = config.Keys.First(k => k.Name == "format");
        Assert.Equal(Importance.Medium, formatKey.Importance);
    }

    [Fact]
    public void GcsSourceConnector_Stop_ClearsConfig()
    {
        using var connector = new GcsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
            ["topic"] = "test-topic"
        };

        connector.Start(config);
        connector.Stop();

        // TaskConfigs should return empty config after stop
        var taskConfigs = connector.TaskConfigs(1);
        Assert.Single(taskConfigs);
        Assert.Empty(taskConfigs[0]);
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
