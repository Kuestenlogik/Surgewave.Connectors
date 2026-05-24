using Kuestenlogik.Surgewave.Connector.Gcp.Firestore;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Firestore.Tests;

public class FirestoreSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new FirestoreSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsFirestoreSinkTask()
    {
        var connector = new FirestoreSinkConnector();
        Assert.Equal(typeof(FirestoreSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesProjectIdConfig()
    {
        var connector = new FirestoreSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.ProjectIdConfig);
    }

    [Fact]
    public void Config_DefinesCredentialsJsonConfig()
    {
        var connector = new FirestoreSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.CredentialsJsonConfig);
    }

    [Fact]
    public void Config_DefinesCredentialsFileConfig()
    {
        var connector = new FirestoreSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.CredentialsFileConfig);
    }

    [Fact]
    public void Config_DefinesEmulatorHostConfig()
    {
        var connector = new FirestoreSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.EmulatorHostConfig);
    }

    [Fact]
    public void Config_DefinesCollectionPathConfig()
    {
        var connector = new FirestoreSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.CollectionPathConfig);
    }

    [Fact]
    public void Config_DefinesTopicsConfig()
    {
        var connector = new FirestoreSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_DefinesDocumentIdFieldConfig()
    {
        var connector = new FirestoreSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.DocumentIdFieldConfig);
    }

    [Fact]
    public void Config_DefinesWriteModeConfig()
    {
        var connector = new FirestoreSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.WriteModeConfig);
    }

    [Fact]
    public void Config_DefinesBatchSizeConfig()
    {
        var connector = new FirestoreSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Config_DefinesMaxRetryCountConfig()
    {
        var connector = new FirestoreSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.MaxRetryCountConfig);
    }

    [Fact]
    public void Config_DefinesRetryDelayMsConfig()
    {
        var connector = new FirestoreSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void Start_ThrowsWhenProjectIdMissing()
    {
        var connector = new FirestoreSinkConnector();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.TopicsConfig] = "orders"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(FirestoreConnectorConfig.ProjectIdConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenCollectionMissing()
    {
        var connector = new FirestoreSinkConnector();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.TopicsConfig] = "orders"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(FirestoreConnectorConfig.CollectionPathConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTopicsMissing()
    {
        var connector = new FirestoreSinkConnector();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(FirestoreConnectorConfig.TopicsConfig, exception.Message);
    }

    [Fact]
    public void Start_AcceptsValidConfig()
    {
        var connector = new FirestoreSinkConnector();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.TopicsConfig] = "orders,payments"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new FirestoreSinkConnector();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.TopicsConfig] = "orders"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        Assert.Single(taskConfigs);
        Assert.Equal("test-project", taskConfigs[0][FirestoreConnectorConfig.ProjectIdConfig]);
        Assert.Equal("testcollection", taskConfigs[0][FirestoreConnectorConfig.CollectionPathConfig]);
        Assert.Equal("orders", taskConfigs[0][FirestoreConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void Stop_CompletesWithoutError()
    {
        var connector = new FirestoreSinkConnector();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.TopicsConfig] = "orders"
        };

        connector.Start(config);
        connector.Stop();
        // Should complete without exception
    }

    [Fact]
    public void Config_HasExpectedWriteModeDefault()
    {
        var connector = new FirestoreSinkConnector();
        var writeModeKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.WriteModeConfig);
        Assert.Equal(FirestoreConnectorConfig.DefaultWriteMode, writeModeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedBatchSizeDefault()
    {
        var connector = new FirestoreSinkConnector();
        var batchSizeKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.BatchSizeConfig);
        Assert.Equal(FirestoreConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxRetryCountDefault()
    {
        var connector = new FirestoreSinkConnector();
        var maxRetryKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.MaxRetryCountConfig);
        Assert.Equal(FirestoreConnectorConfig.DefaultMaxRetryCount, maxRetryKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedRetryDelayDefault()
    {
        var connector = new FirestoreSinkConnector();
        var retryDelayKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.RetryDelayMsConfig);
        Assert.Equal((int)FirestoreConnectorConfig.DefaultRetryDelayMs, retryDelayKey.DefaultValue);
    }

    [Fact]
    public void Config_CredentialsJsonIsPasswordType()
    {
        var connector = new FirestoreSinkConnector();
        var credJsonKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.CredentialsJsonConfig);
        Assert.Equal(ConfigType.Password, credJsonKey.Type);
    }

    [Fact]
    public void Config_ProjectIdIsHighImportance()
    {
        var connector = new FirestoreSinkConnector();
        var projectIdKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.ProjectIdConfig);
        Assert.Equal(Importance.High, projectIdKey.Importance);
    }

    [Fact]
    public void Config_CollectionPathIsHighImportance()
    {
        var connector = new FirestoreSinkConnector();
        var collectionKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.CollectionPathConfig);
        Assert.Equal(Importance.High, collectionKey.Importance);
    }

    [Fact]
    public void Config_TopicsIsHighImportance()
    {
        var connector = new FirestoreSinkConnector();
        var topicsKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.TopicsConfig);
        Assert.Equal(Importance.High, topicsKey.Importance);
    }

    [Fact]
    public void Config_DocumentIdFieldDefaultsToId()
    {
        var connector = new FirestoreSinkConnector();
        var docIdKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.DocumentIdFieldConfig);
        Assert.Equal("id", docIdKey.DefaultValue);
    }
}
