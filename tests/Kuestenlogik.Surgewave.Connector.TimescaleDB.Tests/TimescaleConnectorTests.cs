namespace Kuestenlogik.Surgewave.Connector.TimescaleDB.Tests;

/// <summary>
/// Configuration validation for the TimescaleDB connectors. Connection details can be supplied
/// either as a full connection string or as individual settings, and the source needs to know
/// what to read - a table or a query, never neither.
/// </summary>
public class TimescaleConnectorTests
{
    private const string ConnectionString = "Host=127.0.0.1;Database=metrics";

    [Fact]
    public void SourceConnector_Start_RequiresTheDestinationTopic()
    {
        using var connector = new TimescaleSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TimescaleConnectorConfig.ConnectionString] = ConnectionString,
            [TimescaleConnectorConfig.Table] = "readings"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TimescaleConnectorConfig.Topic, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_RequiresADatabaseWhenThereIsNoConnectionString()
    {
        using var connector = new TimescaleSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TimescaleConnectorConfig.Topic] = "metrics",
            [TimescaleConnectorConfig.Host] = "db.internal",
            [TimescaleConnectorConfig.Table] = "readings"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TimescaleConnectorConfig.Database, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_AcceptsAConnectionStringInsteadOfIndividualSettings()
    {
        using var connector = new TimescaleSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TimescaleConnectorConfig.Topic] = "metrics",
            [TimescaleConnectorConfig.ConnectionString] = ConnectionString,
            [TimescaleConnectorConfig.Table] = "readings"
        };

        connector.Start(config);

        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("readings", taskConfig[TimescaleConnectorConfig.Table]);
        Assert.Equal(typeof(TimescaleSourceTask), connector.TaskClass);
    }

    [Fact]
    public void SourceConnector_Start_NeedsSomethingToRead()
    {
        using var connector = new TimescaleSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TimescaleConnectorConfig.Topic] = "metrics",
            [TimescaleConnectorConfig.ConnectionString] = ConnectionString
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TimescaleConnectorConfig.Query, ex.Message, StringComparison.Ordinal);
        Assert.Contains(TimescaleConnectorConfig.Table, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_Start_RequiresTheTargetHypertable()
    {
        using var connector = new TimescaleSinkConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TimescaleConnectorConfig.Topics] = "metrics",
            [TimescaleConnectorConfig.ConnectionString] = ConnectionString
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TimescaleConnectorConfig.TargetTable, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_Start_RequiresTheTopicsToConsume()
    {
        using var connector = new TimescaleSinkConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TimescaleConnectorConfig.ConnectionString] = ConnectionString,
            [TimescaleConnectorConfig.TargetTable] = "readings"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TimescaleConnectorConfig.Topics, ex.Message, StringComparison.Ordinal);
    }
}
