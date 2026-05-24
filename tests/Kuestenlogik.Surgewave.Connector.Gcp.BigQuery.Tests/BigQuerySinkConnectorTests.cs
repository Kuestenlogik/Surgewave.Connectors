using Kuestenlogik.Surgewave.Connector.Gcp.BigQuery;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.BigQuery.Tests;

public class BigQuerySinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsBigQuerySinkTask()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Equal(typeof(BigQuerySinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesProjectIdConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.ProjectIdConfig);
    }

    [Fact]
    public void Config_DefinesCredentialsJsonConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.CredentialsJsonConfig);
    }

    [Fact]
    public void Config_DefinesCredentialsFileConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.CredentialsFileConfig);
    }

    [Fact]
    public void Config_DefinesDatasetConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.DatasetConfig);
    }

    [Fact]
    public void Config_DefinesTableConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.TableConfig);
    }

    [Fact]
    public void Config_DefinesTopicsConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_DefinesWriteModeConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.WriteModeConfig);
    }

    [Fact]
    public void Config_DefinesBatchSizeConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Config_DefinesMaxRetryCountConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.MaxRetryCountConfig);
    }

    [Fact]
    public void Config_DefinesRetryDelayMsConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void Config_DefinesAutoCreateTableConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.AutoCreateTableConfig);
    }

    [Fact]
    public void Config_DefinesAutoCreateDatasetConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.AutoCreateDatasetConfig);
    }

    [Fact]
    public void Config_DefinesUseStreamingConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.UseStreamingConfig);
    }

    [Fact]
    public void Config_DefinesTimePartitioningConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.TimePartitioningConfig);
    }

    [Fact]
    public void Config_DefinesClusteringFieldsConfig()
    {
        var connector = new BigQuerySinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.ClusteringFieldsConfig);
    }

    [Fact]
    public void Start_ThrowsWhenProjectIdMissing()
    {
        var connector = new BigQuerySinkConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.DatasetConfig] = "testdataset",
            [BigQueryConnectorConfig.TableConfig] = "testtable",
            [BigQueryConnectorConfig.TopicsConfig] = "testtopic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(BigQueryConnectorConfig.ProjectIdConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenDatasetMissing()
    {
        var connector = new BigQuerySinkConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.ProjectIdConfig] = "testproject",
            [BigQueryConnectorConfig.TableConfig] = "testtable",
            [BigQueryConnectorConfig.TopicsConfig] = "testtopic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(BigQueryConnectorConfig.DatasetConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTableMissing()
    {
        var connector = new BigQuerySinkConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.ProjectIdConfig] = "testproject",
            [BigQueryConnectorConfig.DatasetConfig] = "testdataset",
            [BigQueryConnectorConfig.TopicsConfig] = "testtopic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(BigQueryConnectorConfig.TableConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTopicsMissing()
    {
        var connector = new BigQuerySinkConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.ProjectIdConfig] = "testproject",
            [BigQueryConnectorConfig.DatasetConfig] = "testdataset",
            [BigQueryConnectorConfig.TableConfig] = "testtable"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(BigQueryConnectorConfig.TopicsConfig, exception.Message);
    }

    [Fact]
    public void Start_AcceptsValidConfig()
    {
        var connector = new BigQuerySinkConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.ProjectIdConfig] = "testproject",
            [BigQueryConnectorConfig.DatasetConfig] = "testdataset",
            [BigQueryConnectorConfig.TableConfig] = "testtable",
            [BigQueryConnectorConfig.TopicsConfig] = "testtopic"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new BigQuerySinkConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.ProjectIdConfig] = "testproject",
            [BigQueryConnectorConfig.DatasetConfig] = "testdataset",
            [BigQueryConnectorConfig.TableConfig] = "testtable",
            [BigQueryConnectorConfig.TopicsConfig] = "testtopic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        Assert.Single(taskConfigs);
        Assert.Equal("testproject", taskConfigs[0][BigQueryConnectorConfig.ProjectIdConfig]);
        Assert.Equal("testdataset", taskConfigs[0][BigQueryConnectorConfig.DatasetConfig]);
        Assert.Equal("testtable", taskConfigs[0][BigQueryConnectorConfig.TableConfig]);
    }

    [Fact]
    public void Stop_CompletesWithoutError()
    {
        var connector = new BigQuerySinkConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.ProjectIdConfig] = "testproject",
            [BigQueryConnectorConfig.DatasetConfig] = "testdataset",
            [BigQueryConnectorConfig.TableConfig] = "testtable",
            [BigQueryConnectorConfig.TopicsConfig] = "testtopic"
        };

        connector.Start(config);
        connector.Stop();
        // Should complete without exception
    }

    [Fact]
    public void Config_HasExpectedWriteModeDefault()
    {
        var connector = new BigQuerySinkConnector();
        var writeModeKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.WriteModeConfig);
        Assert.Equal(BigQueryConnectorConfig.DefaultWriteMode, writeModeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedBatchSizeDefault()
    {
        var connector = new BigQuerySinkConnector();
        var batchSizeKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.BatchSizeConfig);
        Assert.Equal(BigQueryConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxRetryCountDefault()
    {
        var connector = new BigQuerySinkConnector();
        var maxRetryKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.MaxRetryCountConfig);
        Assert.Equal(BigQueryConnectorConfig.DefaultMaxRetryCount, maxRetryKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedRetryDelayDefault()
    {
        var connector = new BigQuerySinkConnector();
        var retryDelayKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.RetryDelayMsConfig);
        Assert.Equal((int)BigQueryConnectorConfig.DefaultRetryDelayMs, retryDelayKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedLocationDefault()
    {
        var connector = new BigQuerySinkConnector();
        var locationKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.LocationConfig);
        Assert.Equal(BigQueryConnectorConfig.DefaultLocation, locationKey.DefaultValue);
    }

    [Fact]
    public void Config_CredentialsJsonIsPasswordType()
    {
        var connector = new BigQuerySinkConnector();
        var credJsonKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.CredentialsJsonConfig);
        Assert.Equal(ConfigType.Password, credJsonKey.Type);
    }

    [Fact]
    public void Config_ProjectIdIsHighImportance()
    {
        var connector = new BigQuerySinkConnector();
        var projectIdKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.ProjectIdConfig);
        Assert.Equal(Importance.High, projectIdKey.Importance);
    }

    [Fact]
    public void Config_TopicsIsHighImportance()
    {
        var connector = new BigQuerySinkConnector();
        var topicsKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.TopicsConfig);
        Assert.Equal(Importance.High, topicsKey.Importance);
    }

    [Fact]
    public void Config_AutoCreateTableDefaultsToFalse()
    {
        var connector = new BigQuerySinkConnector();
        var autoCreateKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.AutoCreateTableConfig);
        Assert.Equal(false, autoCreateKey.DefaultValue);
    }

    [Fact]
    public void Config_AutoCreateDatasetDefaultsToFalse()
    {
        var connector = new BigQuerySinkConnector();
        var autoCreateKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.AutoCreateDatasetConfig);
        Assert.Equal(false, autoCreateKey.DefaultValue);
    }

    [Fact]
    public void Config_UseStreamingDefaultsToTrue()
    {
        var connector = new BigQuerySinkConnector();
        var useStreamingKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.UseStreamingConfig);
        Assert.Equal(true, useStreamingKey.DefaultValue);
    }
}
