namespace Kuestenlogik.Surgewave.Connector.Gcp.Storage.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class GcsSinkConnectorTests
{
    [Fact]
    public void GcsSinkConnector_HasCorrectVersion()
    {
        using var connector = new GcsSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void GcsSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new GcsSinkConnector();
        Assert.Equal(typeof(GcsSinkTask), connector.TaskClass);
    }

    [Fact]
    public void GcsSinkConnector_Config_HasConnectionKeys()
    {
        using var connector = new GcsSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "gcs.project.id");
        Assert.Contains(config.Keys, k => k.Name == "gcs.bucket.name");
        Assert.Contains(config.Keys, k => k.Name == "gcs.credentials.json");
        Assert.Contains(config.Keys, k => k.Name == "gcs.credentials.file");
    }

    [Fact]
    public void GcsSinkConnector_Config_HasSinkKeys()
    {
        using var connector = new GcsSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "topics");
        Assert.Contains(config.Keys, k => k.Name == "gcs.prefix");
        Assert.Contains(config.Keys, k => k.Name == "format");
        Assert.Contains(config.Keys, k => k.Name == "partitioner");
        Assert.Contains(config.Keys, k => k.Name == "flush.size");
        Assert.Contains(config.Keys, k => k.Name == "rotate.interval.ms");
    }

    [Fact]
    public void GcsSinkConnector_Start_ThrowsOnMissingBucketName()
    {
        using var connector = new GcsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["topics"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("gcs.bucket.name", ex.Message);
    }

    [Fact]
    public void GcsSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new GcsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message);
    }

    [Fact]
    public void GcsSinkConnector_Start_AcceptsMinimalConfig()
    {
        using var connector = new GcsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic"
        };

        // Should not throw - will use ADC
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData("invalid")]
    [InlineData("csv")]
    [InlineData("parquet")]
    public void GcsSinkConnector_Start_ThrowsOnInvalidFormat(string format)
    {
        using var connector = new GcsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic",
            ["format"] = format
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("format", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("json")]
    [InlineData("jsonlines")]
    public void GcsSinkConnector_Start_AcceptsValidFormat(string format)
    {
        using var connector = new GcsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
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
    public void GcsSinkConnector_Start_ThrowsOnInvalidPartitioner(string partitioner)
    {
        using var connector = new GcsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
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
    public void GcsSinkConnector_Start_AcceptsValidPartitioner(string partitioner)
    {
        using var connector = new GcsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic",
            ["partitioner"] = partitioner
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void GcsSinkConnector_TaskConfigs_ReturnsRequestedNumberOfTasks()
    {
        using var connector = new GcsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Equal(3, taskConfigs.Count);
    }

    [Fact]
    public void GcsSinkConnector_TaskConfigs_AssignsUniqueTaskIds()
    {
        using var connector = new GcsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Equal("0", taskConfigs[0]["task.id"]);
        Assert.Equal("1", taskConfigs[1]["task.id"]);
        Assert.Equal("2", taskConfigs[2]["task.id"]);
    }

    [Fact]
    public void GcsSinkConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new GcsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic",
            ["gcs.prefix"] = "output/",
            ["format"] = "jsonlines",
            ["partitioner"] = "time",
            ["flush.size"] = "500",
            ["gcs.project.id"] = "my-project"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("test-bucket", taskConfigs[0]["gcs.bucket.name"]);
        Assert.Equal("test-topic", taskConfigs[0]["topics"]);
        Assert.Equal("output/", taskConfigs[0]["gcs.prefix"]);
        Assert.Equal("jsonlines", taskConfigs[0]["format"]);
        Assert.Equal("time", taskConfigs[0]["partitioner"]);
        Assert.Equal("500", taskConfigs[0]["flush.size"]);
        Assert.Equal("my-project", taskConfigs[0]["gcs.project.id"]);
    }

    [Fact]
    public void GcsSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new GcsSinkConnector();
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

    [Fact]
    public void GcsSinkConnector_Config_HasCorrectImportanceLevels()
    {
        using var connector = new GcsSinkConnector();
        var config = connector.Config;

        var bucketNameKey = config.Keys.First(k => k.Name == "gcs.bucket.name");
        Assert.Equal(Importance.High, bucketNameKey.Importance);

        var topicsKey = config.Keys.First(k => k.Name == "topics");
        Assert.Equal(Importance.High, topicsKey.Importance);

        var formatKey = config.Keys.First(k => k.Name == "format");
        Assert.Equal(Importance.Medium, formatKey.Importance);
    }

    [Fact]
    public void GcsSinkConnector_Stop_ClearsConfig()
    {
        using var connector = new GcsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcs.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic"
        };

        connector.Start(config);
        connector.Stop();

        // TaskConfigs should return empty config after stop
        var taskConfigs = connector.TaskConfigs(1);
        Assert.Single(taskConfigs);
        Assert.Single(taskConfigs[0]); // Only task.id remains
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
