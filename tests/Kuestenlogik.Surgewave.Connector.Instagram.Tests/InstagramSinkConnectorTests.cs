namespace Kuestenlogik.Surgewave.Connector.Instagram.Tests;

/// <summary>
/// Configuration contract of the Instagram sink connector. Only the image flow is implemented, so
/// the connector must reject the media types it cannot serve instead of accepting them silently.
/// </summary>
public class InstagramSinkConnectorTests
{
    [Fact]
    public void Start_RejectsUnsupportedMediaType()
    {
        using var connector = new InstagramSinkConnector();
        var config = SinkConfig();
        config[InstagramConnectorConfig.MediaType] = "carousel";

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains("carousel", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_AcceptsImage_AndHandsConfigToTheTask()
    {
        using var connector = new InstagramSinkConnector();
        var config = SinkConfig();
        config[InstagramConnectorConfig.MediaType] = "image";

        connector.Start(config);
        var taskConfig = Assert.Single(connector.TaskConfigs(4));

        Assert.Equal(typeof(InstagramSinkTask), connector.TaskClass);
        Assert.Equal("token", taskConfig[InstagramConnectorConfig.AccessToken]);
        Assert.Equal("17841400000000000", taskConfig[InstagramConnectorConfig.BusinessAccountId]);
    }

    [Fact]
    public void Config_OffersOnlyTheImplementedMediaType()
    {
        using var connector = new InstagramSinkConnector();

        var key = connector.Config.Keys.First(k => k.Name == InstagramConnectorConfig.MediaType);
        var options = key.Options ?? [];

        Assert.Equal("image", Assert.Single(options));
    }

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [InstagramConnectorConfig.AccessToken] = "token",
        [InstagramConnectorConfig.BusinessAccountId] = "17841400000000000"
    };
}
