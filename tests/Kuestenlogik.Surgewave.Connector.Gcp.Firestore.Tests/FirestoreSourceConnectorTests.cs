using Kuestenlogik.Surgewave.Connector.Gcp.Firestore;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Firestore.Tests;

public class FirestoreSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsFirestoreSourceTask()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Equal(typeof(FirestoreSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesProjectIdConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.ProjectIdConfig);
    }

    [Fact]
    public void Config_DefinesCredentialsJsonConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.CredentialsJsonConfig);
    }

    [Fact]
    public void Config_DefinesCredentialsFileConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.CredentialsFileConfig);
    }

    [Fact]
    public void Config_DefinesEmulatorHostConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.EmulatorHostConfig);
    }

    [Fact]
    public void Config_DefinesCollectionPathConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.CollectionPathConfig);
    }

    [Fact]
    public void Config_DefinesTopicPatternConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.TopicPatternConfig);
    }

    [Fact]
    public void Config_DefinesWatchModeConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.WatchModeConfig);
    }

    [Fact]
    public void Config_DefinesPollIntervalConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesMaxDocumentsPerPollConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.MaxDocumentsPerPollConfig);
    }

    [Fact]
    public void Config_DefinesIncludeMetadataConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Config_DefinesQueryFilterConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.QueryFilterConfig);
    }

    [Fact]
    public void Config_DefinesOrderByFieldConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.OrderByFieldConfig);
    }

    [Fact]
    public void Config_DefinesOrderDirectionConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.OrderDirectionConfig);
    }

    [Fact]
    public void Config_DefinesTimestampFieldConfig()
    {
        var connector = new FirestoreSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == FirestoreConnectorConfig.TimestampFieldConfig);
    }

    [Fact]
    public void Start_ThrowsWhenProjectIdMissing()
    {
        var connector = new FirestoreSourceConnector();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(FirestoreConnectorConfig.ProjectIdConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenCollectionMissing()
    {
        var connector = new FirestoreSourceConnector();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(FirestoreConnectorConfig.CollectionPathConfig, exception.Message);
    }

    [Fact]
    public void Start_AcceptsValidConfig()
    {
        var connector = new FirestoreSourceConnector();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new FirestoreSourceConnector();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        Assert.Single(taskConfigs);
        Assert.Equal("test-project", taskConfigs[0][FirestoreConnectorConfig.ProjectIdConfig]);
        Assert.Equal("testcollection", taskConfigs[0][FirestoreConnectorConfig.CollectionPathConfig]);
    }

    [Fact]
    public void Stop_CompletesWithoutError()
    {
        var connector = new FirestoreSourceConnector();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection"
        };

        connector.Start(config);
        connector.Stop();
        // Should complete without exception
    }

    [Fact]
    public void Config_HasExpectedTopicPatternDefault()
    {
        var connector = new FirestoreSourceConnector();
        var topicPatternKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.TopicPatternConfig);
        Assert.Equal(FirestoreConnectorConfig.DefaultTopicPattern, topicPatternKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedWatchModeDefault()
    {
        var connector = new FirestoreSourceConnector();
        var watchModeKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.WatchModeConfig);
        Assert.Equal(FirestoreConnectorConfig.DefaultWatchMode, watchModeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPollIntervalDefault()
    {
        var connector = new FirestoreSourceConnector();
        var pollIntervalKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.PollIntervalMsConfig);
        Assert.Equal((int)FirestoreConnectorConfig.DefaultPollIntervalMs, pollIntervalKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxDocumentsPerPollDefault()
    {
        var connector = new FirestoreSourceConnector();
        var maxDocsKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.MaxDocumentsPerPollConfig);
        Assert.Equal(FirestoreConnectorConfig.DefaultMaxDocumentsPerPoll, maxDocsKey.DefaultValue);
    }

    [Fact]
    public void Config_IncludeMetadataDefaultsToTrue()
    {
        var connector = new FirestoreSourceConnector();
        var includeMetadataKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.IncludeMetadataConfig);
        Assert.Equal(true, includeMetadataKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedOrderDirectionDefault()
    {
        var connector = new FirestoreSourceConnector();
        var orderDirKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.OrderDirectionConfig);
        Assert.Equal(FirestoreConnectorConfig.DefaultOrderDirection, orderDirKey.DefaultValue);
    }

    [Fact]
    public void Config_CredentialsJsonIsPasswordType()
    {
        var connector = new FirestoreSourceConnector();
        var credJsonKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.CredentialsJsonConfig);
        Assert.Equal(ConfigType.Password, credJsonKey.Type);
    }

    [Fact]
    public void Config_ProjectIdIsStringType()
    {
        var connector = new FirestoreSourceConnector();
        var projectIdKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.ProjectIdConfig);
        Assert.Equal(ConfigType.String, projectIdKey.Type);
    }

    [Fact]
    public void Config_ProjectIdIsHighImportance()
    {
        var connector = new FirestoreSourceConnector();
        var projectIdKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.ProjectIdConfig);
        Assert.Equal(Importance.High, projectIdKey.Importance);
    }

    [Fact]
    public void Config_CollectionPathIsHighImportance()
    {
        var connector = new FirestoreSourceConnector();
        var collectionKey = connector.Config.Keys.First(k => k.Name == FirestoreConnectorConfig.CollectionPathConfig);
        Assert.Equal(Importance.High, collectionKey.Importance);
    }
}
