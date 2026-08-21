namespace Kuestenlogik.Surgewave.Connector.Xmpp.Tests;

/// <summary>
/// Startup validation for the XMPP sink connector.
/// </summary>
public class XmppSinkConnectorTests
{
    [Theory]
    [InlineData(XmppConnectorConfig.Topics)]
    [InlineData(XmppConnectorConfig.Host)]
    [InlineData(XmppConnectorConfig.Domain)]
    [InlineData(XmppConnectorConfig.Username)]
    [InlineData(XmppConnectorConfig.Password)]
    public void Start_WithoutARequiredSetting_ThrowsNamingIt(string missingKey)
    {
        using var connector = new XmppSinkConnector();

        var config = ValidConfig();
        config.Remove(missingKey);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(missingKey, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_WithACompleteConfig_PassesTheWholeConfigOnToTheTask()
    {
        using var connector = new XmppSinkConnector();
        var config = ValidConfig();

        connector.Start(config);

        var taskConfig = Assert.Single(connector.TaskConfigs(2));
        Assert.Equal("xmpp.example.com", taskConfig[XmppConnectorConfig.Host]);
        Assert.Equal("example.com", taskConfig[XmppConnectorConfig.Domain]);
        Assert.NotSame(config, taskConfig);
    }

    private static Dictionary<string, string> ValidConfig() => new()
    {
        [XmppConnectorConfig.Topics] = "outbound",
        [XmppConnectorConfig.Host] = "xmpp.example.com",
        [XmppConnectorConfig.Domain] = "example.com",
        [XmppConnectorConfig.Username] = "bot",
        [XmppConnectorConfig.Password] = "secret"
    };
}
