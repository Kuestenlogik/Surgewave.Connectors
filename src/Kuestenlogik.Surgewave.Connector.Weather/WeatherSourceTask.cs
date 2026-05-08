using System.Diagnostics.CodeAnalysis;
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
    private HttpClient? _httpClient;
    private string _topic = null!;
    private string _provider = null!;
    private string? _apiKey;
    private string _units = "metric";
    private List<(string name, double lat, double lon)> _locations = [];
    private string _dataTypes = "current";
    private int _pollIntervalMs;
    private int _forecastDays;
    private bool _forecastHourly;
    private DateTime _lastPoll = DateTime.MinValue;
    private long _messageId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[WeatherConnectorConfig.Topic];
        _provider = config.TryGetValue(WeatherConnectorConfig.Provider, out var provider) ? provider : "openweathermap";
        _apiKey = config.TryGetValue(WeatherConnectorConfig.ApiKey, out var apiKey) ? apiKey : null;
        _units = config.TryGetValue(WeatherConnectorConfig.Units, out var units) ? units : "metric";
        _dataTypes = config.TryGetValue(WeatherConnectorConfig.DataTypes, out var dataTypes) ? dataTypes : "current";
        _pollIntervalMs = int.Parse(config.TryGetValue(WeatherConnectorConfig.PollIntervalMs, out var pollInterval)
            ? pollInterval : WeatherConnectorConfig.DefaultPollIntervalMs.ToString());
        _forecastDays = int.Parse(config.TryGetValue(WeatherConnectorConfig.ForecastDays, out var forecastDays)
            ? forecastDays : WeatherConnectorConfig.DefaultForecastDays.ToString());
        _forecastHourly = (config.TryGetValue(WeatherConnectorConfig.ForecastHourly, out var forecastHourly) ? forecastHourly : "false") == "true";

        // Parse locations
        if (config.TryGetValue(WeatherConnectorConfig.Locations, out var locs) && !string.IsNullOrWhiteSpace(locs))
        {
            foreach (var loc in locs.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                // Check if it's coordinates (lat,lon) or city name
                var parts = loc.Split(';');
                if (parts.Length == 2 && double.TryParse(parts[0], out var lat) && double.TryParse(parts[1], out var lon))
                {
                    _locations.Add((loc, lat, lon));
                }
                else
                {
                    // City name - will geocode later
                    _locations.Add((loc, 0, 0));
                }
            }
        }
        else
        {
            var lat = double.Parse(config[WeatherConnectorConfig.Latitude]);
            var lon = double.Parse(config[WeatherConnectorConfig.Longitude]);
            _locations.Add(($"{lat},{lon}", lat, lon));
        }

        _httpClient = new HttpClient();
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if ((DateTime.UtcNow - _lastPoll).TotalMilliseconds < _pollIntervalMs)
        {
            return [];
        }

        _lastPoll = DateTime.UtcNow;
        var records = new List<SourceRecord>();

        foreach (var location in _locations)
        {
            try
            {
                var (lat, lon) = location.lat != 0
                    ? (location.lat, location.lon)
                    : await GeocodeLocationAsync(location.name, cancellationToken);

                if (_dataTypes.Contains("current") || _dataTypes == "all")
                {
                    var currentWeather = await FetchCurrentWeatherAsync(lat, lon, cancellationToken);
                    if (currentWeather != null)
                    {
                        records.Add(CreateRecord(location.name, "current", currentWeather));
                    }
                }

                if (_dataTypes.Contains("forecast") || _dataTypes == "all")
                {
                    var forecast = await FetchForecastAsync(lat, lon, cancellationToken);
                    if (forecast != null)
                    {
                        records.Add(CreateRecord(location.name, "forecast", forecast));
                    }
                }
            }
            catch (Exception)
            {
                // Log and continue with next location
            }
        }

        return records;
    }

    private async Task<(double lat, double lon)> GeocodeLocationAsync(string location, CancellationToken cancellationToken)
    {
        if (_provider == "openweathermap" && !string.IsNullOrEmpty(_apiKey))
        {
            var url = $"http://api.openweathermap.org/geo/1.0/direct?q={Uri.EscapeDataString(location)}&limit=1&appid={_apiKey}";
            var response = await _httpClient!.GetStringAsync(url, cancellationToken);
            using var doc = JsonDocument.Parse(response);
            var arr = doc.RootElement;
            if (arr.GetArrayLength() > 0)
            {
                var first = arr[0];
                return (first.GetProperty("lat").GetDouble(), first.GetProperty("lon").GetDouble());
            }
        }

        throw new Exception($"Could not geocode location: {location}");
    }

    private async Task<JsonDocument?> FetchCurrentWeatherAsync(double lat, double lon, CancellationToken cancellationToken)
    {
        string url;

        if (_provider == "openweathermap")
        {
            url = $"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units={_units}&appid={_apiKey}";
        }
        else // open-meteo
        {
            url = $"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true";
        }

        var response = await _httpClient!.GetStringAsync(url, cancellationToken);
        return JsonDocument.Parse(response);
    }

    private async Task<JsonDocument?> FetchForecastAsync(double lat, double lon, CancellationToken cancellationToken)
    {
        string url;

        if (_provider == "openweathermap")
        {
            url = $"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&units={_units}&cnt={_forecastDays * 8}&appid={_apiKey}";
        }
        else // open-meteo
        {
            var hourly = _forecastHourly ? "&hourly=temperature_2m,precipitation,weathercode" : "";
            url = $"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=weathercode,temperature_2m_max,temperature_2m_min&forecast_days={_forecastDays}{hourly}";
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
