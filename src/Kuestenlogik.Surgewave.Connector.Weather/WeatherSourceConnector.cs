using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Weather;

/// <summary>
/// Source connector that polls weather data from various providers.
/// </summary>
[ConnectorMetadata(
    Name = "weather-source",
    Description = "Polls weather conditions, forecasts, and alerts from weather APIs",
    Author = "Surgewave",
    Tags = "weather, api, source, forecast, conditions")]
public sealed class WeatherSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(WeatherConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce weather data to", EditorHint.Topic)
        .Define(WeatherConnectorConfig.Provider, ConfigType.String, WeatherConnectorConfig.DefaultProvider,
            Importance.High, "Weather provider: openweathermap, open-meteo, nws")
        .Define(WeatherConnectorConfig.ApiKey, ConfigType.Password, "", Importance.High,
            "API key for weather provider (required for OpenWeatherMap)")
        .Define(WeatherConnectorConfig.Locations, ConfigType.List, "", Importance.High,
            "Comma-separated list of locations (city names or lat,lon)")
        .Define(WeatherConnectorConfig.Latitude, ConfigType.String, "", Importance.Medium,
            "Latitude for single location")
        .Define(WeatherConnectorConfig.Longitude, ConfigType.String, "", Importance.Medium,
            "Longitude for single location")
        .Define(WeatherConnectorConfig.Units, ConfigType.String, WeatherConnectorConfig.DefaultUnits,
            Importance.Medium, "Units: metric, imperial, standard")
        .Define(WeatherConnectorConfig.DataTypes, ConfigType.String, WeatherConnectorConfig.DefaultDataTypes,
            Importance.Medium, "Data types: current, forecast, alerts, all")
        .Define(WeatherConnectorConfig.PollIntervalMs, ConfigType.Int,
            WeatherConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(WeatherConnectorConfig.ForecastDays, ConfigType.Int,
            WeatherConnectorConfig.DefaultForecastDays.ToString(), Importance.Low,
            "Number of forecast days")
        .Define(WeatherConnectorConfig.ForecastHourly, ConfigType.Boolean, "false", Importance.Low,
            "Include hourly forecast");

    public override Type TaskClass => typeof(WeatherSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(WeatherConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{WeatherConnectorConfig.Topic}' is required");
        }

        var provider = config.TryGetValue(WeatherConnectorConfig.Provider, out var prov) ? prov : "openweathermap";
        if (provider == "openweathermap")
        {
            if (!config.TryGetValue(WeatherConnectorConfig.ApiKey, out var apiKey) ||
                string.IsNullOrWhiteSpace(apiKey))
            {
                throw new ArgumentException($"'{WeatherConnectorConfig.ApiKey}' is required for OpenWeatherMap");
            }
        }

        var hasLocations = config.TryGetValue(WeatherConnectorConfig.Locations, out var locs) && !string.IsNullOrWhiteSpace(locs);
        var hasCoords = config.TryGetValue(WeatherConnectorConfig.Latitude, out var lat) && !string.IsNullOrWhiteSpace(lat) &&
                        config.TryGetValue(WeatherConnectorConfig.Longitude, out var lon) && !string.IsNullOrWhiteSpace(lon);

        if (!hasLocations && !hasCoords)
        {
            throw new ArgumentException("Either locations list or latitude/longitude is required");
        }
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }

    public override void Stop()
    {
    }
}
