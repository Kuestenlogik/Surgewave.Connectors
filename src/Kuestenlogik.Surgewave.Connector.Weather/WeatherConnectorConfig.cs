namespace Kuestenlogik.Surgewave.Connector.Weather;

/// <summary>
/// Configuration constants for Weather connector.
/// </summary>
public static class WeatherConnectorConfig
{
    // Provider selection
    public const string Provider = "weather.provider";  // openweathermap, open-meteo, nws

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
    public const string DataTypes = "weather.data.types";  // current, forecast, alerts, all

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
}
