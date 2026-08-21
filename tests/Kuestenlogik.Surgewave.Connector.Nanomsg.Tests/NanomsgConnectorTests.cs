using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Nanomsg.Tests;

/// <summary>
/// nanomsg sockets are one-directional per pattern, so the connector has to reject a socket type
/// the task could never use before a worker starts a task that can only fail. These tests pin the
/// validation and keep the advertised socket-type list in step with the accepted one.
/// </summary>
public class NanomsgConnectorTests
{
    [Fact]
    public void SourceConnector_Start_RequiresTheSurgewaveTopic()
    {
        using var connector = new NanomsgSourceConnector();

        var config = SourceConfig();
        config.Remove(NanomsgConnectorConfig.Topic);

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(NanomsgConnectorConfig.Topic, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_RequiresEndpoints()
    {
        using var connector = new NanomsgSourceConnector();

        var config = SourceConfig();
        config[NanomsgConnectorConfig.Endpoints] = "   ";

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(NanomsgConnectorConfig.Endpoints, error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("PUB")]
    [InlineData("PUSH")]
    [InlineData("SURVEYOR")]
    [InlineData("NOT-A-SOCKET")]
    public void SourceConnector_Start_RejectsSocketTypesTheSourceTaskCannotReceiveOn(string socketType)
    {
        using var connector = new NanomsgSourceConnector();

        var config = SourceConfig();
        config[NanomsgConnectorConfig.SocketType] = socketType;

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Theory]
    [InlineData("SUB")]
    [InlineData("PULL")]
    [InlineData("RESPONDENT")]
    [InlineData("NOT-A-SOCKET")]
    public void SinkConnector_Start_RejectsSocketTypesTheSinkTaskCannotSendOn(string socketType)
    {
        using var connector = new NanomsgSinkConnector();

        var config = SinkConfig();
        config[NanomsgConnectorConfig.SocketType] = socketType;

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void SinkConnector_Start_RequiresTopicsAndEndpoints()
    {
        using var connector = new NanomsgSinkConnector();

        var withoutTopics = SinkConfig();
        withoutTopics.Remove(NanomsgConnectorConfig.Topics);
        Assert.Throws<ArgumentException>(() => connector.Start(withoutTopics));

        var withoutEndpoints = SinkConfig();
        withoutEndpoints.Remove(NanomsgConnectorConfig.Endpoints);
        Assert.Throws<ArgumentException>(() => connector.Start(withoutEndpoints));
    }

    [Fact]
    public void SourceConnector_AcceptsEverySocketTypeItAdvertises()
    {
        using var connector = new NanomsgSourceConnector();

        foreach (var socketType in AdvertisedSocketTypes(connector.Config))
        {
            var config = SourceConfig();
            config[NanomsgConnectorConfig.SocketType] = socketType;

            // A type offered by the editor drop-down that Start rejects would be a dead option.
            connector.Start(config);
        }
    }

    [Fact]
    public void SinkConnector_AcceptsEverySocketTypeItAdvertises()
    {
        using var connector = new NanomsgSinkConnector();

        foreach (var socketType in AdvertisedSocketTypes(connector.Config))
        {
            var config = SinkConfig();
            config[NanomsgConnectorConfig.SocketType] = socketType;

            connector.Start(config);
        }
    }

    [Fact]
    public void SourceConnector_HandsOutOneIndependentTaskConfig()
    {
        using var connector = new NanomsgSourceConnector();
        connector.Start(SourceConfig());

        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        taskConfig[NanomsgConnectorConfig.Endpoints] = "tampered";

        var second = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("tcp://127.0.0.1:5555", second[NanomsgConnectorConfig.Endpoints]);
        Assert.Equal(typeof(NanomsgSourceTask), connector.TaskClass);
    }

    private static string[] AdvertisedSocketTypes(ConfigDef config)
    {
        var key = Assert.Single(config.Keys, k => k.Name == NanomsgConnectorConfig.SocketType);
        return Assert.IsType<string[]>(key.Options);
    }

    private static Dictionary<string, string> SourceConfig() => new(StringComparer.Ordinal)
    {
        [NanomsgConnectorConfig.Topic] = "nanomsg-events",
        [NanomsgConnectorConfig.Endpoints] = "tcp://127.0.0.1:5555",
        [NanomsgConnectorConfig.SocketType] = "SUB"
    };

    private static Dictionary<string, string> SinkConfig() => new(StringComparer.Ordinal)
    {
        [NanomsgConnectorConfig.Topics] = "nanomsg-out",
        [NanomsgConnectorConfig.Endpoints] = "tcp://127.0.0.1:5556",
        [NanomsgConnectorConfig.SocketType] = "PUB"
    };
}
