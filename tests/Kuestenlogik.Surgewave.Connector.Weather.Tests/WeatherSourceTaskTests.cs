using System.Globalization;
using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Weather.Tests;

/// <summary>
/// Drives the task against a stubbed HTTP handler: no network, but the real URL building,
/// location parsing, caching and error handling.
/// </summary>
public class WeatherSourceTaskTests
{
    private const string CurrentWeatherJson = """{"current_weather":{"temperature":21.5}}""";
    private const string ForecastJson = """{"daily":{"temperature_2m_max":[22.0]}}""";
    private const string GeocodeJson = """{"results":[{"latitude":52.52,"longitude":13.41}]}""";

    [Fact]
    public async Task PollAsync_CoordinatePair_StaysInvariantUnderALocaleWithACommaDecimalSeparator()
    {
        var previousCulture = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = new CultureInfo("de-DE");
        try
        {
            using var handler = new StubHttpHandler(_ => JsonResponse(CurrentWeatherJson));
            using var task = new WeatherSourceTask(handler);
            task.Start(OpenMeteoConfig("52.52;13.41"));

            var records = await task.PollAsync(TestContext.Current.CancellationToken);

            // A culture-sensitive parse or format would either drop the location or ask
            // Open-Meteo for latitude=52,52 - i.e. weather for somewhere else entirely.
            var url = Assert.Single(handler.Requests);
            Assert.Contains("latitude=52.52&longitude=13.41", url, StringComparison.Ordinal);
            var record = Assert.Single(records);
            Assert.Equal("52.52;13.41:current", Encoding.UTF8.GetString(record.Key!));
        }
        finally
        {
            CultureInfo.CurrentCulture = previousCulture;
        }
    }

    [Fact]
    public async Task PollAsync_DocumentedLatCommaLonPair_IsOneLocationNotTwoCityNames()
    {
        using var handler = new StubHttpHandler(_ => JsonResponse(CurrentWeatherJson));
        using var task = new WeatherSourceTask(handler);
        task.Start(OpenMeteoConfig("48.85,2.35"));

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        var url = Assert.Single(handler.Requests);
        Assert.Contains("latitude=48.85&longitude=2.35", url, StringComparison.Ordinal);
        Assert.DoesNotContain("geocoding", url, StringComparison.Ordinal);
        var record = Assert.Single(records);
        Assert.Equal("48.85,2.35:current", Encoding.UTF8.GetString(record.Key!));
    }

    [Fact]
    public async Task PollAsync_ProducesRecordCarryingProviderLocationAndUnits()
    {
        using var handler = new StubHttpHandler(_ => JsonResponse(CurrentWeatherJson));
        using var task = new WeatherSourceTask(handler);
        var config = OpenMeteoConfig("52.52;13.41");
        config[WeatherConnectorConfig.Units] = "imperial";
        task.Start(config);

        var record = Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));

        Assert.Equal("weather", record.Topic);
        Assert.Equal("open-meteo", HeaderValue(record, "weather.provider"));
        Assert.Equal("52.52;13.41", HeaderValue(record, "weather.location"));
        Assert.Equal("current", HeaderValue(record, "weather.data.type"));
        Assert.Equal("imperial", HeaderValue(record, "weather.units"));
        Assert.Contains("\"temperature\":21.5", Encoding.UTF8.GetString(record.Value), StringComparison.Ordinal);
        Assert.Equal("weather", record.SourcePartition["source"]);
        Assert.Equal("52.52;13.41", record.SourcePartition["location"]);
        Assert.Equal(1L, record.SourceOffset["message_id"]);
    }

    [Fact]
    public async Task PollAsync_CityName_IsGeocodedOnceAndThenServedFromTheCachedCoordinates()
    {
        using var handler = new StubHttpHandler(uri =>
            uri.Host.Contains("geocoding", StringComparison.Ordinal)
                ? JsonResponse(GeocodeJson)
                : JsonResponse(CurrentWeatherJson));
        using var task = new WeatherSourceTask(handler);
        task.Start(OpenMeteoConfig("Berlin"));

        await task.PollAsync(TestContext.Current.CancellationToken);
        await task.PollAsync(TestContext.Current.CancellationToken);

        // One geocode, then two weather calls - the resolved coordinates are kept.
        Assert.Equal(3, handler.Requests.Count);
        Assert.Contains("geocoding-api.open-meteo.com", handler.Requests[0], StringComparison.Ordinal);
        Assert.DoesNotContain(handler.Requests.Skip(1), r => r.Contains("geocoding", StringComparison.Ordinal));
        Assert.Contains("latitude=52.52&longitude=13.41", handler.Requests[2], StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_WhenOneLocationFails_RaisesTheErrorAndStillPollsTheNextOne()
    {
        var errors = new List<Exception>();
        using var handler = new StubHttpHandler(uri =>
            uri.Query.Contains("latitude=52.52", StringComparison.Ordinal)
                ? new HttpResponseMessage(HttpStatusCode.InternalServerError)
                : JsonResponse(CurrentWeatherJson));
        using var task = new WeatherSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(OpenMeteoConfig("52.52;13.41,48.85;2.35"));

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        var error = Assert.Single(errors);
        Assert.IsType<HttpRequestException>(error);
        var record = Assert.Single(records);
        Assert.Equal("48.85;2.35:current", Encoding.UTF8.GetString(record.Key!));
    }

    [Fact]
    public async Task PollAsync_BeforeTheIntervalElapsed_ReturnsEmptyWithoutCallingTheApiAgain()
    {
        using var handler = new StubHttpHandler(_ => JsonResponse(CurrentWeatherJson));
        using var task = new WeatherSourceTask(handler);
        var config = OpenMeteoConfig("52.52;13.41");
        config[WeatherConnectorConfig.PollIntervalMs] = "600000";
        task.Start(config);

        var first = await task.PollAsync(TestContext.Current.CancellationToken);
        var second = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Single(first);
        Assert.Empty(second);
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task PollAsync_ForecastDataType_AsksForTheConfiguredDaysAndHourlyFields()
    {
        using var handler = new StubHttpHandler(_ => JsonResponse(ForecastJson));
        using var task = new WeatherSourceTask(handler);
        var config = OpenMeteoConfig("52.52;13.41");
        config[WeatherConnectorConfig.DataTypes] = "forecast";
        config[WeatherConnectorConfig.ForecastDays] = "3";
        config[WeatherConnectorConfig.ForecastHourly] = "true";
        task.Start(config);

        var record = Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));

        var url = Assert.Single(handler.Requests);
        Assert.Contains("forecast_days=3", url, StringComparison.Ordinal);
        Assert.Contains("hourly=temperature_2m,precipitation,weathercode", url, StringComparison.Ordinal);
        Assert.Equal("52.52;13.41:forecast", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("forecast", HeaderValue(record, "weather.data.type"));
    }

    [Fact]
    public async Task PollAsync_OpenWeatherMap_PutsUnitsAndApiKeyIntoTheQuery()
    {
        using var handler = new StubHttpHandler(_ => JsonResponse(CurrentWeatherJson));
        using var task = new WeatherSourceTask(handler);
        var config = OpenMeteoConfig("52.52;13.41");
        config[WeatherConnectorConfig.Provider] = "openweathermap";
        config[WeatherConnectorConfig.ApiKey] = "secret-key";
        task.Start(config);

        await task.PollAsync(TestContext.Current.CancellationToken);

        var url = Assert.Single(handler.Requests);
        Assert.Contains("api.openweathermap.org/data/2.5/weather", url, StringComparison.Ordinal);
        Assert.Contains("appid=secret-key", url, StringComparison.Ordinal);
        Assert.Contains("units=metric", url, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_WithProviderThatIsOnlyAdvertised_Throws()
    {
        using var task = new WeatherSourceTask();

        var config = OpenMeteoConfig("52.52;13.41");
        config[WeatherConnectorConfig.Provider] = "nws";

        var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
        Assert.Contains(WeatherConnectorConfig.SupportedProviders, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_WithLocationsThatAreAllUnusable_ThrowsInsteadOfPollingNothing()
    {
        using var task = new WeatherSourceTask();

        var config = OpenMeteoConfig("52.52;13.41");
        config.Remove(WeatherConnectorConfig.Locations);

        var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
        Assert.Contains(WeatherConnectorConfig.Locations, ex.Message, StringComparison.Ordinal);
    }

    private static string HeaderValue(SourceRecord record, string name) =>
        Encoding.UTF8.GetString(record.Headers![name]);

    private static HttpResponseMessage JsonResponse(string body) =>
        new(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    private static Dictionary<string, string> OpenMeteoConfig(string locations) => new()
    {
        [WeatherConnectorConfig.Topic] = "weather",
        [WeatherConnectorConfig.Provider] = "open-meteo",
        [WeatherConnectorConfig.Locations] = locations,
        [WeatherConnectorConfig.PollIntervalMs] = "0"
    };

    /// <summary>Answers every request from a canned responder and records the URLs it saw.</summary>
    private sealed class StubHttpHandler : HttpMessageHandler
    {
        private readonly Func<Uri, HttpResponseMessage> _respond;

        public StubHttpHandler(Func<Uri, HttpResponseMessage> respond) => _respond = respond;

        public List<string> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var uri = request.RequestUri!;
            Requests.Add(Uri.UnescapeDataString(uri.ToString()));
            return Task.FromResult(_respond(uri));
        }
    }
}
