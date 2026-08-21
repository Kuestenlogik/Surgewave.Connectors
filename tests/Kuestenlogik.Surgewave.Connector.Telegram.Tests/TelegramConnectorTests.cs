namespace Kuestenlogik.Surgewave.Connector.Telegram.Tests;

/// <summary>
/// Configuration validation for the Telegram connectors. The ConfigDef advertises a
/// <c>telegram.polling.mode</c> setting although only long polling exists - a connector that
/// silently long-polls when a webhook was configured looks healthy while ignoring the operator.
/// </summary>
public class TelegramConnectorTests
{
    private const string BotToken = "123456:AAHfake-token";

    [Fact]
    public void SourceConnector_Start_RejectsAPollingModeThatIsNotImplemented()
    {
        using var connector = new TelegramSourceConnector();

        var config = SourceConfig();
        config[TelegramConnectorConfig.PollingMode] = "webhook";

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TelegramConnectorConfig.PollingModeLongPolling, ex.Message, StringComparison.Ordinal);
        Assert.Contains("webhook", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_AcceptsLongPollingWhateverTheCasing()
    {
        using var connector = new TelegramSourceConnector();

        var config = SourceConfig();
        config[TelegramConnectorConfig.PollingMode] = "Long-Polling";

        connector.Start(config);

        Assert.Single(connector.TaskConfigs(4));
    }

    [Fact]
    public void SourceConnector_Start_TreatsAnEmptyPollingModeAsTheDefault()
    {
        using var connector = new TelegramSourceConnector();

        var config = SourceConfig();
        config[TelegramConnectorConfig.PollingMode] = "   ";

        connector.Start(config);

        Assert.Equal(typeof(TelegramSourceTask), connector.TaskClass);
    }

    [Fact]
    public void SourceConnector_Start_RequiresTheBotToken()
    {
        using var connector = new TelegramSourceConnector();

        var config = SourceConfig();
        config.Remove(TelegramConnectorConfig.BotToken);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TelegramConnectorConfig.BotToken, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_RequiresTheDestinationTopic()
    {
        using var connector = new TelegramSourceConnector();

        var config = SourceConfig();
        config.Remove(TelegramConnectorConfig.Topic);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TelegramConnectorConfig.Topic, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_Start_RequiresTheDefaultChatId()
    {
        using var connector = new TelegramSinkConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TelegramConnectorConfig.BotToken] = BotToken,
            [TelegramConnectorConfig.Topics] = "outbound"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TelegramConnectorConfig.DefaultChatId, ex.Message, StringComparison.Ordinal);
    }

    private static Dictionary<string, string> SourceConfig() => new(StringComparer.Ordinal)
    {
        [TelegramConnectorConfig.BotToken] = BotToken,
        [TelegramConnectorConfig.Topic] = "telegram-events"
    };
}
