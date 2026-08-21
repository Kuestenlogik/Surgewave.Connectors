namespace Kuestenlogik.Surgewave.Connector.Discord.Tests;

/// <summary>
/// Configuration contract of the Discord source and sink connectors: every value the task
/// reads with an indexer must be rejected here, otherwise the task dies with a
/// KeyNotFoundException instead of a readable configuration error.
/// </summary>
public class DiscordConnectorTests
{
    [Fact]
    public void SourceConnector_Start_RejectsAMissingBotToken()
    {
        using var connector = new DiscordSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [DiscordConnectorConfig.Topic] = "discord-events"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(DiscordConnectorConfig.BotToken, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_RejectsABlankTopic()
    {
        using var connector = new DiscordSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [DiscordConnectorConfig.BotToken] = "bot-token",
            [DiscordConnectorConfig.Topic] = "   "
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(DiscordConnectorConfig.Topic, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_Start_RejectsAMissingDefaultChannel()
    {
        using var connector = new DiscordSinkConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [DiscordConnectorConfig.BotToken] = "bot-token",
            [DiscordConnectorConfig.Topics] = "orders"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(DiscordConnectorConfig.DefaultChannelId, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_TaskConfigs_ReturnsOneSnapshotOfTheStartConfig()
    {
        using var connector = new DiscordSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [DiscordConnectorConfig.BotToken] = "bot-token",
            [DiscordConnectorConfig.Topic] = "discord-events"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        // A single gateway connection means a single task, whatever maxTasks says.
        var taskConfig = Assert.Single(taskConfigs);
        Assert.Equal("discord-events", taskConfig[DiscordConnectorConfig.Topic]);

        config[DiscordConnectorConfig.Topic] = "changed-after-start";
        Assert.Equal("discord-events", taskConfig[DiscordConnectorConfig.Topic]);
    }
}
