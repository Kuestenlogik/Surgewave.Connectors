namespace Kuestenlogik.Surgewave.Connector.S3.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class S3SinkConnectorTests
{
    [Fact]
    public void S3SinkConnector_HasCorrectVersion()
    {
        using var connector = new S3SinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void S3SinkConnector_HasCorrectTaskClass()
    {
        using var connector = new S3SinkConnector();
        Assert.Equal(typeof(S3SinkTask), connector.TaskClass);
    }

    [Fact]
    public void S3SinkConnector_Config_HasConnectionKeys()
    {
        using var connector = new S3SinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "s3.bucket.name");
        Assert.Contains(config.Keys, k => k.Name == "s3.region");
        Assert.Contains(config.Keys, k => k.Name == "s3.endpoint");
        Assert.Contains(config.Keys, k => k.Name == "s3.access.key");
        Assert.Contains(config.Keys, k => k.Name == "s3.secret.key");
    }

    [Fact]
    public void S3SinkConnector_Config_HasSinkKeys()
    {
        using var connector = new S3SinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "topics");
        Assert.Contains(config.Keys, k => k.Name == "s3.prefix");
        Assert.Contains(config.Keys, k => k.Name == "format");
        Assert.Contains(config.Keys, k => k.Name == "partitioner");
        Assert.Contains(config.Keys, k => k.Name == "flush.size");
        Assert.Contains(config.Keys, k => k.Name == "rotate.interval.ms");
        Assert.Contains(config.Keys, k => k.Name == "timezone");
    }

    [Fact]
    public void S3SinkConnector_Start_ThrowsOnMissingBucketName()
    {
        using var connector = new S3SinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["topics"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("s3.bucket.name", ex.Message);
    }

    [Fact]
    public void S3SinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new S3SinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message);
    }

    [Fact]
    public void S3SinkConnector_Start_AcceptsMinimalConfig()
    {
        using var connector = new S3SinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic"
        };

        // Should not throw - will use default credentials
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void S3SinkConnector_Start_AcceptsCredentials()
    {
        using var connector = new S3SinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic",
            ["s3.access.key"] = "AKIAIOSFODNN7EXAMPLE",
            ["s3.secret.key"] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            ["s3.region"] = "us-west-2"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void S3SinkConnector_Start_AcceptsCustomEndpoint()
    {
        using var connector = new S3SinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic",
            ["s3.endpoint"] = "http://localhost:9000",
            ["s3.access.key"] = "minioadmin",
            ["s3.secret.key"] = "minioadmin"
        };

        // Should not throw - for MinIO/LocalStack
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void S3SinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new S3SinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        // S3 sink returns single task
        Assert.Single(taskConfigs);
    }

    [Fact]
    public void S3SinkConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new S3SinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic",
            ["s3.prefix"] = "output/",
            ["format"] = "jsonlines",
            ["partitioner"] = "time",
            ["flush.size"] = "500",
            ["s3.region"] = "ap-southeast-1"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("test-bucket", taskConfigs[0]["s3.bucket.name"]);
        Assert.Equal("test-topic", taskConfigs[0]["topics"]);
        Assert.Equal("output/", taskConfigs[0]["s3.prefix"]);
        Assert.Equal("jsonlines", taskConfigs[0]["format"]);
        Assert.Equal("time", taskConfigs[0]["partitioner"]);
        Assert.Equal("500", taskConfigs[0]["flush.size"]);
        Assert.Equal("ap-southeast-1", taskConfigs[0]["s3.region"]);
    }

    [Fact]
    public void S3SinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new S3SinkConnector();
        var config = connector.Config;

        var formatKey = config.Keys.First(k => k.Name == "format");
        Assert.Equal("json", formatKey.DefaultValue);

        var partitionerKey = config.Keys.First(k => k.Name == "partitioner");
        Assert.Equal("default", partitionerKey.DefaultValue);

        var flushSizeKey = config.Keys.First(k => k.Name == "flush.size");
        Assert.Equal(1000, flushSizeKey.DefaultValue);

        var rotateIntervalKey = config.Keys.First(k => k.Name == "rotate.interval.ms");
        Assert.Equal(3600000L, rotateIntervalKey.DefaultValue);

        var regionKey = config.Keys.First(k => k.Name == "s3.region");
        Assert.Equal("us-east-1", regionKey.DefaultValue);

        var timezoneKey = config.Keys.First(k => k.Name == "timezone");
        Assert.Equal("UTC", timezoneKey.DefaultValue);
    }

    [Fact]
    public void S3SinkConnector_Config_HasCorrectImportanceLevels()
    {
        using var connector = new S3SinkConnector();
        var config = connector.Config;

        var bucketNameKey = config.Keys.First(k => k.Name == "s3.bucket.name");
        Assert.Equal(Importance.High, bucketNameKey.Importance);

        var topicsKey = config.Keys.First(k => k.Name == "topics");
        Assert.Equal(Importance.High, topicsKey.Importance);

        var accessKeyKey = config.Keys.First(k => k.Name == "s3.access.key");
        Assert.Equal(Importance.High, accessKeyKey.Importance);

        var endpointKey = config.Keys.First(k => k.Name == "s3.endpoint");
        Assert.Equal(Importance.Low, endpointKey.Importance);
    }

    [Theory]
    [InlineData("json")]
    [InlineData("jsonlines")]
    [InlineData("parquet")]
    [InlineData("avro")]
    public void S3SinkConnector_Start_AcceptsAllFormats(string format)
    {
        using var connector = new S3SinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic",
            ["format"] = format
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData("default")]
    [InlineData("field")]
    [InlineData("time")]
    [InlineData("custom")]
    public void S3SinkConnector_Start_AcceptsAllPartitioners(string partitioner)
    {
        using var connector = new S3SinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket",
            ["topics"] = "test-topic",
            ["partitioner"] = partitioner
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
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
