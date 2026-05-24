using Kuestenlogik.Surgewave.Connector.Teams;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Teams.Tests;

/// <summary>
/// Tests for Microsoft Teams source and sink connectors.
/// </summary>
public sealed class TeamsConnectorTests
{
    [Fact]
    public void TeamsSourceConnector_HasCorrectVersion()
    {
        var connector = new TeamsSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TeamsSinkConnector_HasCorrectVersion()
    {
        var connector = new TeamsSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TeamsConnectorConfig_HasExpectedConstants()
    {
        Assert.Equal("teams.tenant.id", TeamsConnectorConfig.TenantId);
        Assert.Equal("teams.client.id", TeamsConnectorConfig.ClientId);
        Assert.Equal("teams.client.secret", TeamsConnectorConfig.ClientSecret);
        Assert.Equal("teams.team.id", TeamsConnectorConfig.TeamId);
        Assert.Equal("teams.channel.id", TeamsConnectorConfig.ChannelId);
        Assert.Equal("teams.chat.id", TeamsConnectorConfig.ChatId);
        Assert.Equal("message.format", TeamsConnectorConfig.MessageFormat);
        Assert.Equal("text", TeamsConnectorConfig.FormatText);
        Assert.Equal("html", TeamsConnectorConfig.FormatHtml);
        Assert.Equal("adaptivecard", TeamsConnectorConfig.FormatAdaptiveCard);
    }

    [Fact]
    public void TeamsSourceConnector_ThrowsOnMissingTopic()
    {
        var connector = new TeamsSourceConnector();
        var config = new Dictionary<string, string>();

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TeamsSourceConnector_ThrowsOnMissingTenantId()
    {
        var connector = new TeamsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topic] = "test"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TeamsSourceConnector_ThrowsOnMissingClientId()
    {
        var connector = new TeamsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topic] = "test",
            [TeamsConnectorConfig.TenantId] = "tenant-id"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TeamsSourceConnector_ThrowsOnMissingClientSecret()
    {
        var connector = new TeamsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topic] = "test",
            [TeamsConnectorConfig.TenantId] = "tenant-id",
            [TeamsConnectorConfig.ClientId] = "client-id"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TeamsSourceConnector_ThrowsOnMissingTeamId()
    {
        var connector = new TeamsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topic] = "test",
            [TeamsConnectorConfig.TenantId] = "tenant-id",
            [TeamsConnectorConfig.ClientId] = "client-id",
            [TeamsConnectorConfig.ClientSecret] = "secret"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TeamsSourceConnector_ThrowsOnMissingChannelId()
    {
        var connector = new TeamsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topic] = "test",
            [TeamsConnectorConfig.TenantId] = "tenant-id",
            [TeamsConnectorConfig.ClientId] = "client-id",
            [TeamsConnectorConfig.ClientSecret] = "secret",
            [TeamsConnectorConfig.TeamId] = "team-id"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TeamsSourceConnector_ProducesTaskConfigs()
    {
        var connector = new TeamsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topic] = "test-topic",
            [TeamsConnectorConfig.TenantId] = "tenant-id",
            [TeamsConnectorConfig.ClientId] = "client-id",
            [TeamsConnectorConfig.ClientSecret] = "secret",
            [TeamsConnectorConfig.TeamId] = "team-id",
            [TeamsConnectorConfig.ChannelId] = "channel-id"
        };

        connector.Start(config);
        var configs = connector.TaskConfigs(1);

        Assert.Single(configs);
        Assert.Equal("test-topic", configs[0][TeamsConnectorConfig.Topic]);
        Assert.Equal("tenant-id", configs[0][TeamsConnectorConfig.TenantId]);
        Assert.Equal("client-id", configs[0][TeamsConnectorConfig.ClientId]);
        Assert.Equal("team-id", configs[0][TeamsConnectorConfig.TeamId]);
        Assert.Equal("channel-id", configs[0][TeamsConnectorConfig.ChannelId]);
    }

    [Fact]
    public void TeamsSinkConnector_ThrowsOnMissingTopics()
    {
        var connector = new TeamsSinkConnector();
        var config = new Dictionary<string, string>();

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TeamsSinkConnector_ThrowsOnMissingTenantId()
    {
        var connector = new TeamsSinkConnector();
        var config = new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topics] = "test"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TeamsSinkConnector_ThrowsOnMissingDestination()
    {
        var connector = new TeamsSinkConnector();
        var config = new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topics] = "test",
            [TeamsConnectorConfig.TenantId] = "tenant-id",
            [TeamsConnectorConfig.ClientId] = "client-id",
            [TeamsConnectorConfig.ClientSecret] = "secret"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TeamsSinkConnector_AcceptsTeamAndChannel()
    {
        var connector = new TeamsSinkConnector();
        var config = new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topics] = "test",
            [TeamsConnectorConfig.TenantId] = "tenant-id",
            [TeamsConnectorConfig.ClientId] = "client-id",
            [TeamsConnectorConfig.ClientSecret] = "secret",
            [TeamsConnectorConfig.TeamId] = "team-id",
            [TeamsConnectorConfig.ChannelId] = "channel-id"
        };

        connector.Start(config);
        var configs = connector.TaskConfigs(1);

        Assert.Single(configs);
        Assert.Equal("team-id", configs[0][TeamsConnectorConfig.TeamId]);
        Assert.Equal("channel-id", configs[0][TeamsConnectorConfig.ChannelId]);
    }

    [Fact]
    public void TeamsSinkConnector_AcceptsChatId()
    {
        var connector = new TeamsSinkConnector();
        var config = new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topics] = "test",
            [TeamsConnectorConfig.TenantId] = "tenant-id",
            [TeamsConnectorConfig.ClientId] = "client-id",
            [TeamsConnectorConfig.ClientSecret] = "secret",
            [TeamsConnectorConfig.ChatId] = "chat-id"
        };

        connector.Start(config);
        var configs = connector.TaskConfigs(1);

        Assert.Single(configs);
        Assert.Equal("chat-id", configs[0][TeamsConnectorConfig.ChatId]);
    }

    [Fact]
    public void TeamsSourceConnector_HasCorrectTaskClass()
    {
        var connector = new TeamsSourceConnector();
        Assert.Equal(typeof(TeamsSourceTask), connector.TaskClass);
    }

    [Fact]
    public void TeamsSinkConnector_HasCorrectTaskClass()
    {
        var connector = new TeamsSinkConnector();
        Assert.Equal(typeof(TeamsSinkTask), connector.TaskClass);
    }

    [Fact]
    public void TeamsSourceConnector_HasConfig()
    {
        var connector = new TeamsSourceConnector();
        var configDef = connector.Config;
        Assert.NotNull(configDef);
        Assert.True(configDef.Keys.Count > 0);
    }

    [Fact]
    public void TeamsSinkConnector_HasConfig()
    {
        var connector = new TeamsSinkConnector();
        var configDef = connector.Config;
        Assert.NotNull(configDef);
        Assert.True(configDef.Keys.Count > 0);
    }

    [Fact]
    public void TeamsSourceConnector_StopsCleanly()
    {
        var connector = new TeamsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topic] = "test",
            [TeamsConnectorConfig.TenantId] = "tenant-id",
            [TeamsConnectorConfig.ClientId] = "client-id",
            [TeamsConnectorConfig.ClientSecret] = "secret",
            [TeamsConnectorConfig.TeamId] = "team-id",
            [TeamsConnectorConfig.ChannelId] = "channel-id"
        };

        connector.Start(config);
        connector.Stop();
        // Should not throw
    }

    [Fact]
    public void TeamsSinkConnector_StopsCleanly()
    {
        var connector = new TeamsSinkConnector();
        var config = new Dictionary<string, string>
        {
            [TeamsConnectorConfig.Topics] = "test",
            [TeamsConnectorConfig.TenantId] = "tenant-id",
            [TeamsConnectorConfig.ClientId] = "client-id",
            [TeamsConnectorConfig.ClientSecret] = "secret",
            [TeamsConnectorConfig.ChatId] = "chat-id"
        };

        connector.Start(config);
        connector.Stop();
        // Should not throw
    }
}
