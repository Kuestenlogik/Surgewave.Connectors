namespace Kuestenlogik.Surgewave.Connector.Qdrant.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class QdrantSinkConnectorTests
{
    [Fact]
    public void QdrantSinkConnector_HasCorrectVersion()
    {
        using var connector = new QdrantSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void QdrantSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new QdrantSinkConnector();
        Assert.Equal(typeof(QdrantSinkTask), connector.TaskClass);
    }

    [Fact]
    public void QdrantSinkConnector_Config_HasConnectionKeys()
    {
        using var connector = new QdrantSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.HostConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.PortConfig && k.Type == ConfigType.Int);
        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.HttpsConfig && k.Type == ConfigType.Boolean);
        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.ApiKeyConfig && k.Type == ConfigType.Password);
    }

    [Fact]
    public void QdrantSinkConnector_Config_HasCollectionKeys()
    {
        using var connector = new QdrantSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.CollectionConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.CreateCollectionConfig && k.Type == ConfigType.Boolean);
        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.VectorSizeConfig && k.Type == ConfigType.Int);
        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.DistanceMetricConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void QdrantSinkConnector_Config_HasFieldKeys()
    {
        using var connector = new QdrantSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.VectorFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.IdFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.IdStrategyConfig);
        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.PayloadFieldsConfig);
    }

    [Fact]
    public void QdrantSinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new QdrantSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == QdrantConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void QdrantSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new QdrantSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.CollectionConfig] = "test-collection"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(QdrantConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void QdrantSinkConnector_Start_ThrowsOnMissingCollection()
    {
        using var connector = new QdrantSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.TopicsConfig] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(QdrantConnectorConfig.CollectionConfig, ex.Message);
    }

    [Theory]
    [InlineData(QdrantConnectorConfig.DistanceCosine)]
    [InlineData(QdrantConnectorConfig.DistanceEuclid)]
    [InlineData(QdrantConnectorConfig.DistanceDot)]
    public void QdrantSinkConnector_Start_AcceptsValidDistanceMetrics(string metric)
    {
        using var connector = new QdrantSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.TopicsConfig] = "test-topic",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.DistanceMetricConfig] = metric
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void QdrantSinkConnector_Start_ThrowsOnInvalidDistanceMetric()
    {
        using var connector = new QdrantSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.TopicsConfig] = "test-topic",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.DistanceMetricConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid distance metric", ex.Message);
    }

    [Theory]
    [InlineData(QdrantConnectorConfig.IdStrategyAuto)]
    [InlineData(QdrantConnectorConfig.IdStrategyKey)]
    public void QdrantSinkConnector_Start_AcceptsValidIdStrategies(string strategy)
    {
        using var connector = new QdrantSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.TopicsConfig] = "test-topic",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.IdStrategyConfig] = strategy
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void QdrantSinkConnector_Start_AcceptsFieldIdStrategy()
    {
        using var connector = new QdrantSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.TopicsConfig] = "test-topic",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.IdStrategyConfig] = QdrantConnectorConfig.IdStrategyField,
            [QdrantConnectorConfig.IdFieldConfig] = "document_id"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void QdrantSinkConnector_Start_ThrowsOnFieldIdStrategyWithoutIdField()
    {
        using var connector = new QdrantSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.TopicsConfig] = "test-topic",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.IdStrategyConfig] = QdrantConnectorConfig.IdStrategyField
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(QdrantConnectorConfig.IdFieldConfig, ex.Message);
    }

    [Fact]
    public void QdrantSinkConnector_Start_ThrowsOnInvalidIdStrategy()
    {
        using var connector = new QdrantSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.TopicsConfig] = "test-topic",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.IdStrategyConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid ID strategy", ex.Message);
    }

    [Fact]
    public void QdrantSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new QdrantSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.TopicsConfig] = "test-topic",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-topic", taskConfigs[0][QdrantConnectorConfig.TopicsConfig]);
        Assert.Equal("test-collection", taskConfigs[0][QdrantConnectorConfig.CollectionConfig]);
    }

    [Fact]
    public void QdrantSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new QdrantSinkConnector();
        var config = connector.Config;

        var hostKey = config.Keys.First(k => k.Name == QdrantConnectorConfig.HostConfig);
        Assert.Equal(QdrantConnectorConfig.DefaultHost, hostKey.DefaultValue);

        var portKey = config.Keys.First(k => k.Name == QdrantConnectorConfig.PortConfig);
        Assert.Equal((long)QdrantConnectorConfig.DefaultPort, portKey.DefaultValue);

        var vectorSizeKey = config.Keys.First(k => k.Name == QdrantConnectorConfig.VectorSizeConfig);
        Assert.Equal((long)QdrantConnectorConfig.DefaultVectorSize, vectorSizeKey.DefaultValue);

        var distanceKey = config.Keys.First(k => k.Name == QdrantConnectorConfig.DistanceMetricConfig);
        Assert.Equal(QdrantConnectorConfig.DistanceCosine, distanceKey.DefaultValue);

        var vectorFieldKey = config.Keys.First(k => k.Name == QdrantConnectorConfig.VectorFieldConfig);
        Assert.Equal(QdrantConnectorConfig.DefaultVectorField, vectorFieldKey.DefaultValue);

        var idStrategyKey = config.Keys.First(k => k.Name == QdrantConnectorConfig.IdStrategyConfig);
        Assert.Equal(QdrantConnectorConfig.IdStrategyAuto, idStrategyKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == QdrantConnectorConfig.BatchSizeConfig);
        Assert.Equal((long)QdrantConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);
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
