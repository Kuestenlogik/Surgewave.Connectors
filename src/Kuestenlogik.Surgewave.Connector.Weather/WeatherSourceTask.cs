using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Weather;

/// <summary>
/// Task that polls weather data from APIs.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
[SuppressMessage("Usage", "CA2234:Pass System.Uri objects instead of strings", Justification = "String URLs are more practical for API calls")]
public sealed class WeatherSourceTask : SourceTask
{
    private readonly List<(string name, double lat, double lon, bool resolved)> _locations = [];
    private HttpClient? _httpClient;
    private string _topic = null!;
    private string _provider = null!;
    private string? _apiKey;
    private string _units = WeatherConnectorConfig.DefaultUnits;
    private string _dataTypes = WeatherConnectorConfig.DefaultDataTypes;
    private int _pollIntervalMs;
    private int _forecastDays;
    private bool _forecastHourly;
    private DateTime _lastPoll = DateTime.MinValue;
    private long _messageId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[WeatherConnectorConfig.Topic];
        _provider = config.TryGetValue(WeatherConnectorConfig.Provider, out var provider) && !string.IsNullOrWhiteSpace(provider)
            ? provider : WeatherConnectorConfig.DefaultProvider;
        WeatherConnectorConfig.ValidateProvider(_provider);
        _apiKey = config.TryGetValue(WeatherConnectorConfig.ApiKey, out var apiKey) ? apiKey : null;
        _units = config.TryGetValue(WeatherConnectorConfig.Units, out var units) && !string.IsNullOrWhiteSpace(units)
            ? units : WeatherConnectorConfig.DefaultUnits;
        _dataTypes = config.TryGetValue(WeatherConnectorConfig.DataTypes, out var dataTypes) && !string.IsNullOrWhiteSpace(dataTypes)
            ? dataTypes : WeatherConnectorConfig.DefaultDataTypes;
        WeatherConnectorConfig.ValidateDataTypes(_dataTypes);
        _pollIntervalMs = config.TryGetValue(WeatherConnectorConfig.PollIntervalMs, out var pollInterval) && !string.IsNullOrWhiteSpace(pollInterval)
            ? int.Parse(pollInterval, CultureInfo.InvariantCulture)
            : WeatherConnectorConfig.DefaultPollIntervalMs;
        _forecastDays = config.TryGetValue(WeatherConnectorConfig.ForecastDays, out var forecastDays) && !string.IsNullOrWhiteSpace(forecastDays)
            ? int.Parse(forecastDays, CultureInfo.InvariantCulture)
            : WeatherConnectorConfig.DefaultForecastDays;
        _forecastHourly = (config.TryGetValue(WeatherConnectorConfig.ForecastHourly, out var forecastHourly) ? forecastHourly : "false") == "true";

        // Parse locations
        if (config.TryGetValue(WeatherConnectorConfig.Locations, out var locs) && !string.IsNullOrWhiteSpace(locs))
        {
            ParseLocations(locs);
        }
        else if (config.TryGetValue(WeatherConnectorConfig.Latitude, out var latValue) &&
                 config.TryGetValue(WeatherConnectorConfig.Longitude, out var lonValue) &&
                 TryParseCoordinate(latValue, out var lat) &&
                 TryParseCoordinate(lonValue, out var lon))
        {
            _locations.Add((FormattableString.Invariant($"{lat},{lon}"), lat, lon, true));
        }

        if (_locations.Count == 0)
        {
            throw new ArgumentException(
                $"No usable location: set '{WeatherConnectorConfig.Locations}' or '{WeatherConnectorConfig.Latitude}'/'{WeatherConnectorConfig.Longitude}'",
                nameof(config));
        }

        _httpClient = new HttpClient();
    }

    /// <summary>
    /// Parses the configured location list. An entry is a city name, a single 'lat;lon' pair, or the
    /// documented 'lat,lon' pair - the latter is torn apart by the list split and glued back together here.
    /// </summary>
    private void ParseLocations(string locations)
    {
        var tokens = locations.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        for (var i = 0; i < tokens.Length; i++)
        {
            var parts = tokens[i].Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (parts.Length == 2 && TryParseCoordinate(parts[0], out var lat) && TryParseCoordinate(parts[1], out var lon))
            {
                _locations.Add((tokens[i], lat, lon, true));
                continue;
            }

            if (i + 1 < tokens.Length && TryParseCoordinate(tokens[i], out lat) && TryParseCoordinate(tokens[i + 1], out lon))
            {
                _locations.Add((FormattableString.Invariant($"{lat},{lon}"), lat, lon, true));
                i++;
                continue;
            }

            // City name - geocoded on the first poll, then cached
            _locations.Add((tokens[i], 0, 0, false));
        }
    }

    private static bool TryParseCoordinate(string value, out double coordinate) =>
        double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out coordinate);

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if ((DateTime.UtcNow - _lastPoll).TotalMilliseconds < _pollIntervalMs)
        {
            return [];
        }

        _lastPoll = DateTime.UtcNow;
        var records = new List<SourceRecord>();

        for (var i = 0; i < _locations.Count; i++)
        {
            var location = _locations[i];
            try
            {
                if (!location.resolved)
                {
                    var (geocodedLat, geocodedLon) = await GeocodeLocationAsync(location.name, cancellationToken);
                    location = (location.name, geocodedLat, geocodedLon, true);
                    _locations[i] = location;
                }

                if (_dataTypes.Contains("current") || _dataTypes == "all")
                {
                    using var currentWeather = await FetchCurrentWeatherAsync(location.lat, location.lon, cancellationToken);
                    if (currentWeather != null)
                    {
                        records.Add(CreateRecord(location.name, "current", currentWeather));
                    }
                }

                if (_dataTypes.Contains("forecast") || _dataTypes == "all")
                {
                    using var forecast = await FetchForecastAsync(location.lat, location.lon, cancellationToken);
                    if (forecast != null)
                    {
                        records.Add(CreateRecord(location.name, "forecast", forecast));
                    }
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                // Surface the failure to the framework, then continue with the next location
                Context?.RaiseError?.Invoke(ex);
            }
        }

        return records;
    }

    private async Task<(double lat, double lon)> GeocodeLocationAsync(string location, CancellationToken cancellationToken)
    {
        if (_provider == "openweathermap" && !string.IsNullOrEmpty(_apiKey))
        {
            var url = $"https://api.openweathermap.org/geo/1.0/direct?q={Uri.EscapeDataString(location)}&limit=1&appid={_apiKey}";
            var response = await _httpClient!.GetStringAsync(url, cancellationToken);
            using var doc = JsonDocument.Parse(response);
            var arr = doc.RootElement;
            if (arr.GetArrayLength() > 0)
            {
                var first = arr[0];
                return (first.GetProperty("lat").GetDouble(), first.GetProperty("lon").GetDouble());
            }
        }
        else
        {
            // Open-Meteo geocoding needs no API key
            var url = $"https://geocoding-api.open-meteo.com/v1/search?name={Uri.EscapeDataString(location)}&count=1&format=json";
            var response = await _httpClient!.GetStringAsync(url, cancellationToken);
            using var doc = JsonDocument.Parse(response);
            if (doc.RootElement.TryGetProperty("results", out var results) && results.GetArrayLength() > 0)
            {
                var first = results[0];
                return (first.GetProperty("latitude").GetDouble(), first.GetProperty("longitude").GetDouble());
            }
        }

        throw new InvalidOperationException($"Could not geocode location: {location}");
    }

    private async Task<JsonDocument?> FetchCurrentWeatherAsync(double lat, double lon, CancellationToken cancellationToken)
    {
        string url;

        if (_provider == "openweathermap")
        {
            url = FormattableString.Invariant(
                $"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units={_units}&appid={_apiKey}");
        }
        else // open-meteo
        {
            url = FormattableString.Invariant(
                $"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true");
        }

        var response = await _httpClient!.GetStringAsync(url, cancellationToken);
        return JsonDocument.Parse(response);
    }

    private async Task<JsonDocument?> FetchForecastAsync(double lat, double lon, CancellationToken cancellationToken)
    {
        string url;

        if (_provider == "openweathermap")
        {
            url = FormattableString.Invariant(
                $"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&units={_units}&cnt={_forecastDays * 8}&appid={_apiKey}");
        }
        else // open-meteo
        {
            var hourly = _forecastHourly ? "&hourly=temperature_2m,precipitation,weathercode" : "";
            url = FormattableString.Invariant(
                $"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=weathercode,temperature_2m_max,temperature_2m_min&forecast_days={_forecastDays}{hourly}");
        }

        var response = await _httpClient!.GetStringAsync(url, cancellationToken);
        return JsonDocument.Parse(response);
    }

    private SourceRecord CreateRecord(string location, string dataType, JsonDocument data)
    {
        var headers = new Dictionary<string, byte[]>
        {
            ["weather.provider"] = Encoding.UTF8.GetBytes(_provider),
            ["weather.location"] = Encoding.UTF8.GetBytes(location),
            ["weather.data.type"] = Encoding.UTF8.GetBytes(dataType),
            ["weather.units"] = Encoding.UTF8.GetBytes(_units)
        };

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["source"] = "weather",
                ["location"] = location
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = Interlocked.Increment(ref _messageId),
                ["timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes($"{location}:{dataType}"),
            Value = JsonSerializer.SerializeToUtf8Bytes(data.RootElement),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = headers
        };
    }

    public override void Stop()
    {
        _httpClient?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }
}
