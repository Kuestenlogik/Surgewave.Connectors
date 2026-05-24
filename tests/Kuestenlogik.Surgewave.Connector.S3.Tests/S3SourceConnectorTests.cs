namespace Kuestenlogik.Surgewave.Connector.S3.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class S3SourceConnectorTests
{
    [Fact]
    public void S3SourceConnector_HasCorrectVersion()
    {
        using var connector = new S3SourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void S3SourceConnector_HasCorrectTaskClass()
    {
        using var connector = new S3SourceConnector();
        Assert.Equal(typeof(S3SourceTask), connector.TaskClass);
    }

    [Fact]
    public void S3SourceConnector_Config_HasConnectionKeys()
    {
        using var connector = new S3SourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "s3.bucket.name");
        Assert.Contains(config.Keys, k => k.Name == "s3.region");
        Assert.Contains(config.Keys, k => k.Name == "s3.endpoint");
        Assert.Contains(config.Keys, k => k.Name == "s3.access.key");
        Assert.Contains(config.Keys, k => k.Name == "s3.secret.key");
    }

    [Fact]
    public void S3SourceConnector_Config_HasSourceKeys()
    {
        using var connector = new S3SourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "topic");
        Assert.Contains(config.Keys, k => k.Name == "s3.prefix");
        Assert.Contains(config.Keys, k => k.Name == "format");
        Assert.Contains(config.Keys, k => k.Name == "poll.interval.ms");
        Assert.Contains(config.Keys, k => k.Name == "delete.after.read");
        Assert.Contains(config.Keys, k => k.Name == "mode");
    }

    [Fact]
    public void S3SourceConnector_Start_ThrowsOnMissingBucketName()
    {
        using var connector = new S3SourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["topic"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("s3.bucket.name", ex.Message);
    }

    [Fact]
    public void S3SourceConnector_Start_ThrowsOnMissingTopic()
    {
        using var connector = new S3SourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topic", ex.Message);
    }

    [Fact]
    public void S3SourceConnector_Start_AcceptsMinimalConfig()
    {
        using var connector = new S3SourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket",
            ["topic"] = "test-topic"
        };

        // Should not throw - will use default credentials
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void S3SourceConnector_Start_AcceptsCredentials()
    {
        using var connector = new S3SourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket",
            ["topic"] = "test-topic",
            ["s3.access.key"] = "AKIAIOSFODNN7EXAMPLE",
            ["s3.secret.key"] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            ["s3.region"] = "us-west-2"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void S3SourceConnector_Start_AcceptsCustomEndpoint()
    {
        using var connector = new S3SourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket",
            ["topic"] = "test-topic",
            ["s3.endpoint"] = "http://localhost:9000",
            ["s3.access.key"] = "minioadmin",
            ["s3.secret.key"] = "minioadmin"
        };

        // Should not throw - for MinIO/LocalStack
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void S3SourceConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new S3SourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket",
            ["topic"] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        Assert.Single(taskConfigs);
    }

    [Fact]
    public void S3SourceConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new S3SourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["s3.bucket.name"] = "test-bucket",
            ["topic"] = "test-topic",
            ["s3.prefix"] = "data/",
            ["format"] = "jsonlines",
            ["poll.interval.ms"] = "5000",
            ["s3.region"] = "eu-west-1"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("test-bucket", taskConfigs[0]["s3.bucket.name"]);
        Assert.Equal("test-topic", taskConfigs[0]["topic"]);
        Assert.Equal("data/", taskConfigs[0]["s3.prefix"]);
        Assert.Equal("jsonlines", taskConfigs[0]["format"]);
        Assert.Equal("5000", taskConfigs[0]["poll.interval.ms"]);
        Assert.Equal("eu-west-1", taskConfigs[0]["s3.region"]);
    }

    [Fact]
    public void S3SourceConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new S3SourceConnector();
        var config = connector.Config;

        var formatKey = config.Keys.First(k => k.Name == "format");
        Assert.Equal("json", formatKey.DefaultValue);

        var pollIntervalKey = config.Keys.First(k => k.Name == "poll.interval.ms");
        Assert.Equal(10000L, pollIntervalKey.DefaultValue);

        var deleteAfterReadKey = config.Keys.First(k => k.Name == "delete.after.read");
        Assert.Equal(false, deleteAfterReadKey.DefaultValue);

        var regionKey = config.Keys.First(k => k.Name == "s3.region");
        Assert.Equal("us-east-1", regionKey.DefaultValue);

        var modeKey = config.Keys.First(k => k.Name == "mode");
        Assert.Equal("list", modeKey.DefaultValue);
    }

    [Fact]
    public void S3SourceConnector_Config_HasCorrectImportanceLevels()
    {
        using var connector = new S3SourceConnector();
        var config = connector.Config;

        var bucketNameKey = config.Keys.First(k => k.Name == "s3.bucket.name");
        Assert.Equal(Importance.High, bucketNameKey.Importance);

        var topicKey = config.Keys.First(k => k.Name == "topic");
        Assert.Equal(Importance.High, topicKey.Importance);

        var accessKeyKey = config.Keys.First(k => k.Name == "s3.access.key");
        Assert.Equal(Importance.High, accessKeyKey.Importance);

        var endpointKey = config.Keys.First(k => k.Name == "s3.endpoint");
        Assert.Equal(Importance.Low, endpointKey.Importance);
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
