namespace Kuestenlogik.Surgewave.Connector.Weather.Tests;

/// <summary>
/// The connector-level guards: a misconfigured weather source has to fail at startup instead of
/// polling forever and producing nothing.
/// </summary>
public class WeatherSourceConnectorTests
{
    [Fact]
    public void Start_WithoutTopic_Throws()
    {
        using var connector = new WeatherSourceConnector();

        var config = ValidConfig();
        config.Remove(WeatherConnectorConfig.Topic);

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_OpenWeatherMapWithoutApiKey_Throws()
    {
        using var connector = new WeatherSourceConnector();

        var config = ValidConfig();
        config[WeatherConnectorConfig.Provider] = "openweathermap";

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(WeatherConnectorConfig.ApiKey, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_WithProviderThatIsOnlyAdvertised_ThrowsNamingTheImplementedProviders()
    {
        using var connector = new WeatherSourceConnector();

        var config = ValidConfig();
        config[WeatherConnectorConfig.Provider] = "nws";

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(WeatherConnectorConfig.SupportedProviders, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_WithDataTypeThatIsOnlyAdvertised_ThrowsNamingTheImplementedDataTypes()
    {
        using var connector = new WeatherSourceConnector();

        var config = ValidConfig();
        config[WeatherConnectorConfig.DataTypes] = "current,alerts";

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(WeatherConnectorConfig.SupportedDataTypes, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_WithoutLocationsOrCoordinates_Throws()
    {
        using var connector = new WeatherSourceConnector();

        var config = ValidConfig();
        config.Remove(WeatherConnectorConfig.Locations);

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithCoordinatesInsteadOfLocations_IsAccepted()
    {
        using var connector = new WeatherSourceConnector();

        var config = ValidConfig();
        config.Remove(WeatherConnectorConfig.Locations);
        config[WeatherConnectorConfig.Latitude] = "52.52";
        config[WeatherConnectorConfig.Longitude] = "13.41";

        connector.Start(config);

        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("52.52", taskConfig[WeatherConnectorConfig.Latitude]);
        Assert.NotSame(config, taskConfig);
    }

    private static Dictionary<string, string> ValidConfig() => new()
    {
        [WeatherConnectorConfig.Topic] = "weather",
        [WeatherConnectorConfig.Provider] = "open-meteo",
        [WeatherConnectorConfig.Locations] = "52.52;13.41"
    };
}
