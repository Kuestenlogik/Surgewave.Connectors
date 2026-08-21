namespace Kuestenlogik.Surgewave.Connector.Xmpp.Tests;

/// <summary>
/// Startup validation for the XMPP source connector.
/// </summary>
public class XmppSourceConnectorTests
{
    [Theory]
    [InlineData(XmppConnectorConfig.Topic)]
    [InlineData(XmppConnectorConfig.Host)]
    [InlineData(XmppConnectorConfig.Domain)]
    [InlineData(XmppConnectorConfig.Username)]
    [InlineData(XmppConnectorConfig.Password)]
    public void Start_WithoutARequiredSetting_ThrowsNamingIt(string missingKey)
    {
        using var connector = new XmppSourceConnector();

        var config = ValidConfig();
        config.Remove(missingKey);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(missingKey, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_WithACompleteConfig_PassesRoomsAndFiltersOnToTheTask()
    {
        using var connector = new XmppSourceConnector();
        var config = ValidConfig();
        config[XmppConnectorConfig.JoinRooms] = "room@conference.example.com";
        config[XmppConnectorConfig.FilterJids] = "friend@example.com";

        connector.Start(config);

        var taskConfig = Assert.Single(connector.TaskConfigs(2));
        Assert.Equal("room@conference.example.com", taskConfig[XmppConnectorConfig.JoinRooms]);
        Assert.Equal("friend@example.com", taskConfig[XmppConnectorConfig.FilterJids]);
        Assert.NotSame(config, taskConfig);
    }

    private static Dictionary<string, string> ValidConfig() => new()
    {
        [XmppConnectorConfig.Topic] = "inbound",
        [XmppConnectorConfig.Host] = "xmpp.example.com",
        [XmppConnectorConfig.Domain] = "example.com",
        [XmppConnectorConfig.Username] = "bot",
        [XmppConnectorConfig.Password] = "secret"
    };
}
