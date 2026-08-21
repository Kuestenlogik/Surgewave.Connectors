using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Mattermost.Tests;

/// <summary>
/// Covers the configuration contract both Mattermost connectors publish and the task-configuration
/// handout.
/// </summary>
public class MattermostConnectorTests
{
    [Fact]
    public void SourceConnector_Config_DocumentsTheChannelFilterAsOptional()
    {
        using var connector = new MattermostSourceConnector();

        var channelIds = Assert.Single(connector.Config.Keys, k => k.Name == MattermostConnectorConfig.ChannelIds);

        // The task really does poll every visible channel when the filter is empty, so the
        // documentation must not promise something the task does not do.
        Assert.Equal("", channelIds.DefaultValue);
        Assert.Contains("empty", channelIds.Documentation, StringComparison.OrdinalIgnoreCase);

        var keys = connector.Config.Keys;
        Assert.Contains(keys, k => k.Name == MattermostConnectorConfig.Topic && k.Type == ConfigType.String);
        Assert.Contains(keys, k => k.Name == MattermostConnectorConfig.AccessToken && k.Type == ConfigType.Password);
        Assert.Contains(keys, k => k.Name == MattermostConnectorConfig.PollIntervalMs && k.Type == ConfigType.Int);
    }

    [Fact]
    public void SinkConnector_Config_DefaultsTheMessageFieldToMessage()
    {
        using var connector = new MattermostSinkConnector();

        var messageField = Assert.Single(connector.Config.Keys, k => k.Name == MattermostConnectorConfig.MessageField);

        // The sink task falls back to the same name when the key is absent.
        Assert.Equal(MattermostConnectorConfig.DefaultMessageField, messageField.DefaultValue);
        Assert.Contains(connector.Config.Keys, k => k.Name == MattermostConnectorConfig.ChannelId);
    }

    [Fact]
    public void Connectors_HandOutOneIndependentTaskConfig()
    {
        using var connector = new MattermostSourceConnector();
        connector.Start(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [MattermostConnectorConfig.Topic] = "mattermost-messages",
            [MattermostConnectorConfig.ChannelIds] = "channel-1,channel-2"
        });

        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        taskConfig[MattermostConnectorConfig.ChannelIds] = "tampered";

        var second = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("channel-1,channel-2", second[MattermostConnectorConfig.ChannelIds]);
        Assert.Equal(typeof(MattermostSourceTask), connector.TaskClass);
    }
}
