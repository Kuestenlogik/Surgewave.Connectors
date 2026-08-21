namespace Kuestenlogik.Surgewave.Connector.Sap.OData.Tests;

/// <summary>
/// Tests for the OData connectors: a connector that starts on an incomplete configuration
/// only fails once its tasks are already running against a live SAP system.
/// </summary>
public class ODataConnectorTests
{
    [Theory]
    [InlineData(ODataConnectorConfig.Topic)]
    [InlineData(ODataConnectorConfig.ServiceUrl)]
    [InlineData(ODataConnectorConfig.EntitySet)]
    public void SourceConnector_RefusesToStartWithoutARequiredKey(string missingKey)
    {
        using var connector = new ODataSourceConnector();

        var config = SourceConfig();
        config.Remove(missingKey);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(missingKey, ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(ODataConnectorConfig.Topics)]
    [InlineData(ODataConnectorConfig.ServiceUrl)]
    [InlineData(ODataConnectorConfig.TargetEntitySet)]
    public void SinkConnector_RefusesToStartWithoutARequiredKey(string missingKey)
    {
        using var connector = new ODataSinkConnector();

        var config = SinkConfig();
        config.Remove(missingKey);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(missingKey, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_HandsTheWholeConfigurationToItsSingleTask()
    {
        using var connector = new ODataSourceConnector();
        var config = SourceConfig();
        connector.Start(config);

        // The task reads keys the connector never validates (auth, $select, sap.client),
        // so anything dropped here is silently missing at runtime.
        var taskConfig = Assert.Single(connector.TaskConfigs(4));

        Assert.Equal(config.Count, taskConfig.Count);
        Assert.Equal("OrderId,Amount", taskConfig[ODataConnectorConfig.Select]);
        Assert.Equal("200", taskConfig["sap.client"]);
        Assert.Equal(typeof(ODataSourceTask), connector.TaskClass);
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [ODataConnectorConfig.Topic] = "sap-orders",
        [ODataConnectorConfig.ServiceUrl] = "http://sap.invalid/sap/opu/odata/sap/ZORDERS_SRV/",
        [ODataConnectorConfig.EntitySet] = "SalesOrderSet",
        [ODataConnectorConfig.Select] = "OrderId,Amount",
        ["sap.client"] = "200"
    };

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [ODataConnectorConfig.Topics] = "orders",
        [ODataConnectorConfig.ServiceUrl] = "http://sap.invalid/sap/opu/odata/sap/ZORDERS_SRV/",
        [ODataConnectorConfig.TargetEntitySet] = "SalesOrderSet"
    };
}
