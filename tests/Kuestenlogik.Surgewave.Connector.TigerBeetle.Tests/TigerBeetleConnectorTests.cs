namespace Kuestenlogik.Surgewave.Connector.TigerBeetle.Tests;

/// <summary>
/// Configuration validation for the TigerBeetle connectors: a ledger connector that starts
/// without a cluster address would come up "running" and never move a single entry.
/// </summary>
public class TigerBeetleConnectorTests
{
    [Fact]
    public void SourceConnector_Start_RequiresTheDestinationTopic()
    {
        using var connector = new TigerBeetleSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TigerBeetleConnectorConfig.ClusterAddresses] = "127.0.0.1:3000"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TigerBeetleConnectorConfig.Topic, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_RequiresTheClusterAddresses()
    {
        using var connector = new TigerBeetleSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TigerBeetleConnectorConfig.Topic] = "ledger-events"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TigerBeetleConnectorConfig.ClusterAddresses, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_HandsTheWholeConfigToItsSingleTask()
    {
        using var connector = new TigerBeetleSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TigerBeetleConnectorConfig.Topic] = "ledger-events",
            [TigerBeetleConnectorConfig.ClusterAddresses] = "127.0.0.1:3000,127.0.0.1:3001",
            [TigerBeetleConnectorConfig.WatchAccounts] = "1,2,3"
        };

        connector.Start(config);

        var taskConfig = Assert.Single(connector.TaskConfigs(8));
        Assert.Equal("1,2,3", taskConfig[TigerBeetleConnectorConfig.WatchAccounts]);
        Assert.Equal(typeof(TigerBeetleSourceTask), connector.TaskClass);
    }

    [Fact]
    public void SinkConnector_Start_RequiresTheTopicsToConsume()
    {
        using var connector = new TigerBeetleSinkConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TigerBeetleConnectorConfig.ClusterAddresses] = "127.0.0.1:3000"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TigerBeetleConnectorConfig.Topics, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_Start_RequiresTheClusterAddresses()
    {
        using var connector = new TigerBeetleSinkConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TigerBeetleConnectorConfig.Topics] = "ledger"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TigerBeetleConnectorConfig.ClusterAddresses, ex.Message, StringComparison.Ordinal);
    }
}
