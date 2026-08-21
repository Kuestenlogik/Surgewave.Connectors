namespace Kuestenlogik.Surgewave.Connector.LinkedIn.Tests;

/// <summary>
/// Configuration contract of the LinkedIn sink connector.
/// </summary>
public class LinkedInSinkConnectorTests
{
    [Fact]
    public void TaskConfigs_HandsTheTaskAnIndependentCopy()
    {
        using var connector = new LinkedInSinkConnector();
        var config = new Dictionary<string, string>
        {
            [LinkedInConnectorConfig.AccessToken] = "token-abc",
            [LinkedInConnectorConfig.OrganizationId] = "12345"
        };

        connector.Start(config);
        var taskConfig = Assert.Single(connector.TaskConfigs(2));
        config[LinkedInConnectorConfig.OrganizationId] = "changed-after-start";

        Assert.Equal(typeof(LinkedInSinkTask), connector.TaskClass);
        Assert.Equal("12345", taskConfig[LinkedInConnectorConfig.OrganizationId]);
    }

    [Fact]
    public void Config_OffersTheVisibilityLevelsTheTaskCanSend()
    {
        using var connector = new LinkedInSinkConnector();

        var key = connector.Config.Keys.First(k => k.Name == LinkedInConnectorConfig.DefaultVisibility);
        var options = key.Options ?? [];

        Assert.Equal(new[] { "PUBLIC", "CONNECTIONS" }, options);
        Assert.Equal(LinkedInConnectorConfig.DefaultVisibilityValue, key.DefaultValue?.ToString());
    }
}
