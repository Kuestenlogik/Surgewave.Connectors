namespace Kuestenlogik.Surgewave.Connector.SignalR.Tests;

/// <summary>
/// Tests for the SignalR connectors: what a pipeline author must supply, and whether the
/// choices the editor offers are the ones the task can actually honour.
/// </summary>
public class SignalRConnectorTests
{
    [Theory]
    [InlineData(SignalRConfig.HubUrl)]
    [InlineData(SignalRConfig.Topic)]
    public void SourceConnector_RefusesToStartWithoutARequiredKey(string missingKey)
    {
        using var connector = new SignalRSourceConnector();

        var config = SourceConfig();
        config.Remove(missingKey);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(missingKey, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_RefusesToStartWithoutTheHubUrl()
    {
        using var connector = new SignalRSinkConnector();

        var ex = Assert.Throws<ArgumentException>(
            () => connector.Start(new Dictionary<string, string> { ["topics"] = "events" }));

        Assert.Contains(SignalRConfig.HubUrl, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_OffersOnlyTheMessageShapesTheTaskCanBind()
    {
        using var connector = new SignalRSourceConnector();

        var format = Assert.Single(connector.Config.Keys, k => k.Name == SignalRConfig.MessageFormat);

        // The task registers exactly one hub handler, picked by this value, and rejects
        // anything else - so the editor must not offer a fourth choice.
        Assert.Equal(new[] { "key-value", "value-only", "json" }, format.Options);
        Assert.Equal(SignalRConfig.DefaultMessageFormat, Assert.IsType<string>(format.DefaultValue));
    }

    [Fact]
    public void SourceConnector_HandsTheWholeConfigurationToItsSingleTask()
    {
        using var connector = new SignalRSourceConnector();
        var config = SourceConfig();
        config[SignalRConfig.Method] = "OrderChanged";
        connector.Start(config);

        // A SignalR hub is a single stream, so more tasks would only duplicate messages.
        var taskConfig = Assert.Single(connector.TaskConfigs(8));

        Assert.Equal("OrderChanged", taskConfig[SignalRConfig.Method]);
        Assert.Equal(config.Count, taskConfig.Count);
        Assert.Equal(typeof(SignalRSourceTask), connector.TaskClass);
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [SignalRConfig.HubUrl] = "http://hub.invalid/events",
        [SignalRConfig.Topic] = "signalr-events"
    };
}
