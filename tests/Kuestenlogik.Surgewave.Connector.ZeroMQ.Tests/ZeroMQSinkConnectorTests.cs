namespace Kuestenlogik.Surgewave.Connector.ZeroMQ.Tests;

/// <summary>
/// Startup validation for the ZeroMQ sink: the socket-type dropdown offers more patterns than
/// the sink implements, and the connector has to say so before any task is handed a config.
/// </summary>
public class ZeroMQSinkConnectorTests
{
    [Theory]
    [InlineData(ZeroMQConnectorConfig.Topics)]
    [InlineData(ZeroMQConnectorConfig.Endpoints)]
    public void Start_WithoutARequiredSetting_ThrowsNamingIt(string missingKey)
    {
        using var connector = new ZeroMQSinkConnector();

        var config = ValidConfig();
        config.Remove(missingKey);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(missingKey, ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("DEALER")]
    [InlineData("PAIR")]
    public void Start_WithASocketTypeTheDropdownOffersButTheSinkCannotUse_Throws(string socketType)
    {
        using var connector = new ZeroMQSinkConnector();

        var config = ValidConfig();
        config[ZeroMQConnectorConfig.SocketType] = socketType;

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("PUB, PUSH, or REQ", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("PUB")]
    [InlineData("PUSH")]
    [InlineData("REQ")]
    [InlineData("push")]
    public void Start_WithASocketTypeTheSinkImplements_IsAcceptedRegardlessOfCasing(string socketType)
    {
        using var connector = new ZeroMQSinkConnector();

        var config = ValidConfig();
        config[ZeroMQConnectorConfig.SocketType] = socketType;

        connector.Start(config);

        Assert.Equal(socketType, Assert.Single(connector.TaskConfigs(1))[ZeroMQConnectorConfig.SocketType]);
    }

    [Fact]
    public void TaskConfigs_HandsTheTaskACopySoLaterEditsCannotLeakIn()
    {
        using var connector = new ZeroMQSinkConnector();
        var config = ValidConfig();
        connector.Start(config);

        var taskConfig = Assert.Single(connector.TaskConfigs(4));

        Assert.NotSame(config, taskConfig);
        Assert.Equal("tcp://localhost:5556", taskConfig[ZeroMQConnectorConfig.Endpoints]);
        Assert.Equal(typeof(ZeroMQSinkTask), connector.TaskClass);
    }

    private static Dictionary<string, string> ValidConfig() => new()
    {
        [ZeroMQConnectorConfig.Topics] = "outbound",
        [ZeroMQConnectorConfig.Endpoints] = "tcp://localhost:5556",
        [ZeroMQConnectorConfig.SocketType] = "PUSH"
    };
}
