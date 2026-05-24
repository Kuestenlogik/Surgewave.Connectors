using Kuestenlogik.Surgewave.Connector.Gcp.BigQuery;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.BigQuery.Tests;

public class BigQuerySourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsBigQuerySourceTask()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Equal(typeof(BigQuerySourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesProjectIdConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.ProjectIdConfig);
    }

    [Fact]
    public void Config_DefinesCredentialsJsonConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.CredentialsJsonConfig);
    }

    [Fact]
    public void Config_DefinesCredentialsFileConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.CredentialsFileConfig);
    }

    [Fact]
    public void Config_DefinesDatasetConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.DatasetConfig);
    }

    [Fact]
    public void Config_DefinesLocationConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.LocationConfig);
    }

    [Fact]
    public void Config_DefinesModeConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.ModeConfig);
    }

    [Fact]
    public void Config_DefinesTableConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.TableConfig);
    }

    [Fact]
    public void Config_DefinesQueryConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.QueryConfig);
    }

    [Fact]
    public void Config_DefinesTopicPatternConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.TopicPatternConfig);
    }

    [Fact]
    public void Config_DefinesPollIntervalConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesMaxRowsPerPollConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.MaxRowsPerPollConfig);
    }

    [Fact]
    public void Config_DefinesIncludeMetadataConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Config_DefinesTimestampColumnConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.TimestampColumnConfig);
    }

    [Fact]
    public void Config_DefinesPartitionFieldConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.PartitionFieldConfig);
    }

    [Fact]
    public void Config_DefinesUseStandardSqlConfig()
    {
        var connector = new BigQuerySourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == BigQueryConnectorConfig.UseStandardSqlConfig);
    }

    [Fact]
    public void Start_ThrowsWhenProjectIdMissing()
    {
        var connector = new BigQuerySourceConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.DatasetConfig] = "testdataset",
            [BigQueryConnectorConfig.TableConfig] = "testtable"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(BigQueryConnectorConfig.ProjectIdConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenDatasetMissing()
    {
        var connector = new BigQuerySourceConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.ProjectIdConfig] = "testproject",
            [BigQueryConnectorConfig.TableConfig] = "testtable"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(BigQueryConnectorConfig.DatasetConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTableMissingInTableMode()
    {
        var connector = new BigQuerySourceConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.ProjectIdConfig] = "testproject",
            [BigQueryConnectorConfig.DatasetConfig] = "testdataset",
            [BigQueryConnectorConfig.ModeConfig] = "table"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(BigQueryConnectorConfig.TableConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenQueryMissingInQueryMode()
    {
        var connector = new BigQuerySourceConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.ProjectIdConfig] = "testproject",
            [BigQueryConnectorConfig.DatasetConfig] = "testdataset",
            [BigQueryConnectorConfig.ModeConfig] = "query"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(BigQueryConnectorConfig.QueryConfig, exception.Message);
    }

    [Fact]
    public void Start_AcceptsValidTableModeConfig()
    {
        var connector = new BigQuerySourceConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.ProjectIdConfig] = "testproject",
            [BigQueryConnectorConfig.DatasetConfig] = "testdataset",
            [BigQueryConnectorConfig.TableConfig] = "testtable"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void Start_AcceptsValidQueryModeConfig()
    {
        var connector = new BigQuerySourceConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.ProjectIdConfig] = "testproject",
            [BigQueryConnectorConfig.DatasetConfig] = "testdataset",
            [BigQueryConnectorConfig.ModeConfig] = "query",
            [BigQueryConnectorConfig.QueryConfig] = "SELECT * FROM `testproject.testdataset.testtable`"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new BigQuerySourceConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.ProjectIdConfig] = "testproject",
            [BigQueryConnectorConfig.DatasetConfig] = "testdataset",
            [BigQueryConnectorConfig.TableConfig] = "testtable"
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
        var connector = new BigQuerySourceConnector();

        var config = new Dictionary<string, string>
        {
            [BigQueryConnectorConfig.ProjectIdConfig] = "testproject",
            [BigQueryConnectorConfig.DatasetConfig] = "testdataset",
            [BigQueryConnectorConfig.TableConfig] = "testtable"
        };

        connector.Start(config);
        connector.Stop();
        // Should complete without exception
    }

    [Fact]
    public void Config_HasExpectedTopicPatternDefault()
    {
        var connector = new BigQuerySourceConnector();
        var topicPatternKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.TopicPatternConfig);
        Assert.Equal(BigQueryConnectorConfig.DefaultTopicPattern, topicPatternKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedModeDefault()
    {
        var connector = new BigQuerySourceConnector();
        var modeKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.ModeConfig);
        Assert.Equal(BigQueryConnectorConfig.DefaultMode, modeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedLocationDefault()
    {
        var connector = new BigQuerySourceConnector();
        var locationKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.LocationConfig);
        Assert.Equal(BigQueryConnectorConfig.DefaultLocation, locationKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPollIntervalDefault()
    {
        var connector = new BigQuerySourceConnector();
        var pollIntervalKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.PollIntervalMsConfig);
        Assert.Equal((int)BigQueryConnectorConfig.DefaultPollIntervalMs, pollIntervalKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxRowsPerPollDefault()
    {
        var connector = new BigQuerySourceConnector();
        var maxRowsKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.MaxRowsPerPollConfig);
        Assert.Equal(BigQueryConnectorConfig.DefaultMaxRowsPerPoll, maxRowsKey.DefaultValue);
    }

    [Fact]
    public void Config_IncludeMetadataDefaultsToTrue()
    {
        var connector = new BigQuerySourceConnector();
        var includeMetadataKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.IncludeMetadataConfig);
        Assert.Equal(true, includeMetadataKey.DefaultValue);
    }

    [Fact]
    public void Config_UseStandardSqlDefaultsToTrue()
    {
        var connector = new BigQuerySourceConnector();
        var useStandardSqlKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.UseStandardSqlConfig);
        Assert.Equal(true, useStandardSqlKey.DefaultValue);
    }

    [Fact]
    public void Config_CredentialsJsonIsPasswordType()
    {
        var connector = new BigQuerySourceConnector();
        var credJsonKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.CredentialsJsonConfig);
        Assert.Equal(ConfigType.Password, credJsonKey.Type);
    }

    [Fact]
    public void Config_ProjectIdIsStringType()
    {
        var connector = new BigQuerySourceConnector();
        var projectIdKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.ProjectIdConfig);
        Assert.Equal(ConfigType.String, projectIdKey.Type);
    }

    [Fact]
    public void Config_ProjectIdIsHighImportance()
    {
        var connector = new BigQuerySourceConnector();
        var projectIdKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.ProjectIdConfig);
        Assert.Equal(Importance.High, projectIdKey.Importance);
    }

    [Fact]
    public void Config_DatasetIsHighImportance()
    {
        var connector = new BigQuerySourceConnector();
        var datasetKey = connector.Config.Keys.First(k => k.Name == BigQueryConnectorConfig.DatasetConfig);
        Assert.Equal(Importance.High, datasetKey.Importance);
    }
}
