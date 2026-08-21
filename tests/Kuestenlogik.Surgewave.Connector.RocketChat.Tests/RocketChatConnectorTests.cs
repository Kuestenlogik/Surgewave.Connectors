using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.RocketChat.Tests;

/// <summary>
/// Covers the configuration contract both Rocket.Chat connectors publish and the task-configuration
/// handout.
/// </summary>
public class RocketChatConnectorTests
{
    [Fact]
    public void SourceConnector_Config_DeclaresTheRoomAndCredentialKeys()
    {
        using var connector = new RocketChatSourceConnector();
        var keys = connector.Config.Keys;

        Assert.Contains(keys, k => k.Name == RocketChatConnectorConfig.Topic && k.Type == ConfigType.String);
        Assert.Contains(keys, k => k.Name == RocketChatConnectorConfig.RoomIds && k.Type == ConfigType.String);
        Assert.Contains(keys, k => k.Name == RocketChatConnectorConfig.UserId && k.Type == ConfigType.String);
        Assert.Contains(keys, k => k.Name == RocketChatConnectorConfig.AuthToken && k.Type == ConfigType.Password);

        var serverUrl = Assert.Single(keys, k => k.Name == RocketChatConnectorConfig.ServerUrl);
        Assert.Equal(RocketChatConnectorConfig.DefaultServerUrl, serverUrl.DefaultValue);
    }

    [Fact]
    public void SinkConnector_Config_DefaultsTheTextFieldToText()
    {
        using var connector = new RocketChatSinkConnector();

        var textField = Assert.Single(connector.Config.Keys, k => k.Name == RocketChatConnectorConfig.TextField);

        // The sink task falls back to the same name when the key is absent.
        Assert.Equal("text", textField.DefaultValue);
    }

    [Fact]
    public void Connectors_HandOutOneIndependentTaskConfig()
    {
        using var connector = new RocketChatSourceConnector();
        connector.Start(new Dictionary<string, string>
        {
            [RocketChatConnectorConfig.Topic] = "rocketchat-messages",
            [RocketChatConnectorConfig.RoomIds] = "room-1,room-2"
        });

        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        taskConfig[RocketChatConnectorConfig.RoomIds] = "tampered";

        var second = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("room-1,room-2", second[RocketChatConnectorConfig.RoomIds]);
        Assert.Equal(typeof(RocketChatSourceTask), connector.TaskClass);
    }
}
