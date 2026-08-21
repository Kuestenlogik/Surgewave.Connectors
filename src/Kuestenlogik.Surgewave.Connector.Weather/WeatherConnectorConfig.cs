namespace Kuestenlogik.Surgewave.Connector.Weather;

/// <summary>
/// Configuration constants for Weather connector.
/// </summary>
public static class WeatherConnectorConfig
{
    // Provider selection
    public const string Provider = "weather.provider";  // openweathermap, open-meteo

    // OpenWeatherMap settings
    public const string ApiKey = "weather.api.key";
    public const string BaseUrl = "weather.base.url";

    // Location settings
    public const string Topic = "topic";
    public const string Locations = "weather.locations";  // city names or coordinates
    public const string Latitude = "weather.latitude";
    public const string Longitude = "weather.longitude";
    public const string Units = "weather.units";  // metric, imperial, standard

    // Polling settings
    public const string PollIntervalMs = "poll.interval.ms";
    public const string DataTypes = "weather.data.types";  // current, forecast, all

    // Forecast settings
    public const string ForecastDays = "weather.forecast.days";
    public const string ForecastHourly = "weather.forecast.hourly";

    // Defaults
    public const string DefaultProvider = "openweathermap";
    public const string DefaultOpenWeatherMapUrl = "https://api.openweathermap.org/data/2.5";
    public const string DefaultOpenMeteoUrl = "https://api.open-meteo.com/v1";
    public const string DefaultUnits = "metric";
    public const string DefaultDataTypes = "current";
    public const int DefaultPollIntervalMs = 300000; // 5 minutes
    public const int DefaultForecastDays = 5;

    // Implemented values - anything else is rejected at startup instead of silently ignored
    public const string SupportedProviders = "openweathermap, open-meteo";
    public const string SupportedDataTypes = "current, forecast, all";

    /// <summary>
    /// Throws when the configured provider is not implemented by this connector.
    /// </summary>
    public static void ValidateProvider(string provider)
    {
        if (provider is not ("openweathermap" or "open-meteo"))
        {
            throw new ArgumentException(
                $"'{Provider}' value '{provider}' is not supported. Supported providers: {SupportedProviders}",
                nameof(provider));
        }
    }

    /// <summary>
    /// Throws when the configured data types contain an entry that is not implemented.
    /// </summary>
    public static void ValidateDataTypes(string dataTypes)
    {
        foreach (var dataType in dataTypes.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (dataType is not ("current" or "forecast" or "all"))
            {
                throw new ArgumentException(
                    $"'{DataTypes}' value '{dataType}' is not supported. Supported data types: {SupportedDataTypes}",
                    nameof(dataTypes));
            }
        }
    }
}
