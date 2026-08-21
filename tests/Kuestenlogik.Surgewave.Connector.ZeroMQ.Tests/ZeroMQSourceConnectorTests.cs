namespace Kuestenlogik.Surgewave.Connector.ZeroMQ.Tests;

/// <summary>
/// Startup validation for the ZeroMQ source: same story as the sink - the dropdown advertises
/// DEALER and PAIR, but only SUB, PULL and REP are actually wired up.
/// </summary>
public class ZeroMQSourceConnectorTests
{
    [Theory]
    [InlineData(ZeroMQConnectorConfig.Topic)]
    [InlineData(ZeroMQConnectorConfig.Endpoints)]
    public void Start_WithoutARequiredSetting_ThrowsNamingIt(string missingKey)
    {
        using var connector = new ZeroMQSourceConnector();

        var config = ValidConfig();
        config.Remove(missingKey);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(missingKey, ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("DEALER")]
    [InlineData("PAIR")]
    public void Start_WithASocketTypeTheDropdownOffersButTheSourceCannotUse_Throws(string socketType)
    {
        using var connector = new ZeroMQSourceConnector();

        var config = ValidConfig();
        config[ZeroMQConnectorConfig.SocketType] = socketType;

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("SUB, PULL, or REP", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("SUB")]
    [InlineData("PULL")]
    [InlineData("REP")]
    [InlineData("pull")]
    public void Start_WithASocketTypeTheSourceImplements_IsAcceptedRegardlessOfCasing(string socketType)
    {
        using var connector = new ZeroMQSourceConnector();

        var config = ValidConfig();
        config[ZeroMQConnectorConfig.SocketType] = socketType;

        connector.Start(config);

        Assert.Equal(socketType, Assert.Single(connector.TaskConfigs(1))[ZeroMQConnectorConfig.SocketType]);
    }

    [Fact]
    public void Start_WithoutASocketType_FallsBackToOneTheTaskCanActuallyBuild()
    {
        using var connector = new ZeroMQSourceConnector();

        var config = ValidConfig();
        config.Remove(ZeroMQConnectorConfig.SocketType);

        // An omitted socket type must survive validation, and the task must be able to build the
        // default it then falls back to.
        connector.Start(config);

        var taskConfig = Assert.Single(connector.TaskConfigs(2));
        Assert.False(taskConfig.ContainsKey(ZeroMQConnectorConfig.SocketType));
        Assert.Equal(typeof(ZeroMQSourceTask), connector.TaskClass);
    }

    private static Dictionary<string, string> ValidConfig() => new()
    {
        [ZeroMQConnectorConfig.Topic] = "zeromq-in",
        [ZeroMQConnectorConfig.Endpoints] = "tcp://localhost:5555",
        [ZeroMQConnectorConfig.SocketType] = "SUB"
    };
}
