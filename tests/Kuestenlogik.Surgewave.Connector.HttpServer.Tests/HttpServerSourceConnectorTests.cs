using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.HttpServer.Tests;

public class HttpServerSourceConnectorTests : IDisposable
{
    private readonly HttpServerSourceTask _task;
    private readonly HttpClient _httpClient;
    private readonly int _port;

    public HttpServerSourceConnectorTests()
    {
        _port = 18080 + Random.Shared.Next(1000);
        _task = new HttpServerSourceTask();
        _httpClient = new HttpClient { BaseAddress = new Uri($"http://localhost:{_port}") };

        var config = new Dictionary<string, string>
        {
            [HttpServerConnectorConfig.Host] = "localhost",
            [HttpServerConnectorConfig.Port] = _port.ToString(),
            [HttpServerConnectorConfig.BasePath] = "/api",
            [HttpServerConnectorConfig.SourceTopic] = "test-topic",
            [HttpServerConnectorConfig.SourcePath] = "/ingest",
            [HttpServerConnectorConfig.SourceMethods] = "POST,PUT",
            [HttpServerConnectorConfig.SourceIncludeHeaders] = "true",
            [HttpServerConnectorConfig.SourceIncludeQueryParams] = "true"
        };

        _task.Start(config);
    }

    public void Dispose()
    {
        _task.Stop();
        _task.Dispose();
        _httpClient.Dispose();
    }

    [Fact]
    public async Task HealthEndpoint_ReturnsOk()
    {
        var response = await _httpClient.GetAsync("/api/health");
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync();
        Assert.Contains("healthy", content);
    }

    [Fact]
    public async Task PostRequest_ProducesRecord()
    {
        var payload = new { message = "test" };
        var response = await _httpClient.PostAsJsonAsync("/api/ingest", payload);

        Assert.Equal(System.Net.HttpStatusCode.Accepted, response.StatusCode);

        var content = await response.Content.ReadAsStringAsync();
        Assert.Contains("accepted", content);

        // Poll for the record
        var records = await _task.PollAsync(CancellationToken.None);
        Assert.Single(records);

        var record = records[0];
        Assert.Equal("test-topic", record.Topic);
        Assert.NotNull(record.Value);

        var body = Encoding.UTF8.GetString(record.Value);
        var message = JsonSerializer.Deserialize<JsonElement>(body);
        Assert.Equal("POST", message.GetProperty("method").GetString());
    }

    [Fact]
    public async Task PutRequest_ProducesRecord()
    {
        var payload = new { data = "put-test" };
        var response = await _httpClient.PutAsJsonAsync("/api/ingest", payload);

        Assert.Equal(System.Net.HttpStatusCode.Accepted, response.StatusCode);

        var records = await _task.PollAsync(CancellationToken.None);
        Assert.Single(records);

        var body = Encoding.UTF8.GetString(records[0].Value!);
        var message = JsonSerializer.Deserialize<JsonElement>(body);
        Assert.Equal("PUT", message.GetProperty("method").GetString());
    }

    [Fact]
    public async Task GetRequest_ReturnsMethodNotAllowed()
    {
        var response = await _httpClient.GetAsync("/api/ingest");
        Assert.Equal(System.Net.HttpStatusCode.MethodNotAllowed, response.StatusCode);
    }

    [Fact]
    public async Task QueryParams_IncludedInMessage()
    {
        var payload = new { test = true };
        var response = await _httpClient.PostAsJsonAsync("/api/ingest?foo=bar&baz=123", payload);
        response.EnsureSuccessStatusCode();

        var records = await _task.PollAsync(CancellationToken.None);
        Assert.Single(records);

        var body = Encoding.UTF8.GetString(records[0].Value!);
        var message = JsonSerializer.Deserialize<JsonElement>(body);

        Assert.True(message.TryGetProperty("query", out var query));
        Assert.Equal("bar", query.GetProperty("foo").GetString());
        Assert.Equal("123", query.GetProperty("baz").GetString());
    }

    [Fact]
    public async Task Headers_IncludedInMessage()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/api/ingest");
        request.Headers.Add("X-Custom-Header", "custom-value");
        request.Content = new StringContent("{}", Encoding.UTF8, "application/json");

        var response = await _httpClient.SendAsync(request);
        response.EnsureSuccessStatusCode();

        var records = await _task.PollAsync(CancellationToken.None);
        Assert.Single(records);

        var body = Encoding.UTF8.GetString(records[0].Value!);
        var message = JsonSerializer.Deserialize<JsonElement>(body);

        Assert.True(message.TryGetProperty("headers", out var headers));
        Assert.Equal("custom-value", headers.GetProperty("X-Custom-Header").GetString());
    }

    [Fact]
    public async Task MultipleRequests_ProduceMultipleRecords()
    {
        for (int i = 0; i < 5; i++)
        {
            var payload = new { index = i };
            await _httpClient.PostAsJsonAsync("/api/ingest", payload);
        }

        var records = await _task.PollAsync(CancellationToken.None);
        Assert.Equal(5, records.Count);
    }

    [Fact]
    public async Task SequentialPolls_ReturnsRecordsOnce()
    {
        await _httpClient.PostAsJsonAsync("/api/ingest", new { test = 1 });

        var records1 = await _task.PollAsync(CancellationToken.None);
        Assert.Single(records1);

        var records2 = await _task.PollAsync(CancellationToken.None);
        Assert.Empty(records2);
    }
}

public class HttpServerSourceConnectorAuthTests : IDisposable
{
    private readonly HttpServerSourceTask _task;
    private readonly HttpClient _httpClient;
    private readonly int _port;

    public HttpServerSourceConnectorAuthTests()
    {
        _port = 19080 + Random.Shared.Next(1000);
        _task = new HttpServerSourceTask();
        _httpClient = new HttpClient { BaseAddress = new Uri($"http://localhost:{_port}") };

        var config = new Dictionary<string, string>
        {
            [HttpServerConnectorConfig.Host] = "localhost",
            [HttpServerConnectorConfig.Port] = _port.ToString(),
            [HttpServerConnectorConfig.BasePath] = "/api",
            [HttpServerConnectorConfig.SourceTopic] = "test-topic",
            [HttpServerConnectorConfig.SourcePath] = "/ingest",
            [HttpServerConnectorConfig.SourceMethods] = "POST",
            [HttpServerConnectorConfig.AuthEnabled] = "true",
            [HttpServerConnectorConfig.AuthType] = "api_key",
            [HttpServerConnectorConfig.AuthApiKeys] = "secret-key-1,secret-key-2",
            [HttpServerConnectorConfig.AuthApiKeyHeader] = "X-API-Key"
        };

        _task.Start(config);
    }

    public void Dispose()
    {
        _task.Stop();
        _task.Dispose();
        _httpClient.Dispose();
    }

    [Fact]
    public async Task Request_WithoutApiKey_ReturnsUnauthorized()
    {
        var response = await _httpClient.PostAsJsonAsync("/api/ingest", new { test = true });
        Assert.Equal(System.Net.HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task Request_WithInvalidApiKey_ReturnsUnauthorized()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/api/ingest");
        request.Headers.Add("X-API-Key", "invalid-key");
        request.Content = new StringContent("{}", Encoding.UTF8, "application/json");

        var response = await _httpClient.SendAsync(request);
        Assert.Equal(System.Net.HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task Request_WithValidApiKey_ReturnsAccepted()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/api/ingest");
        request.Headers.Add("X-API-Key", "secret-key-1");
        request.Content = new StringContent("{}", Encoding.UTF8, "application/json");

        var response = await _httpClient.SendAsync(request);
        Assert.Equal(System.Net.HttpStatusCode.Accepted, response.StatusCode);
    }

    [Fact]
    public async Task ApiKeyHeader_NotIncludedInRecordHeaders()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/api/ingest");
        request.Headers.Add("X-API-Key", "secret-key-1");
        request.Headers.Add("X-Other-Header", "visible");
        request.Content = new StringContent("{}", Encoding.UTF8, "application/json");

        await _httpClient.SendAsync(request);

        var records = await _task.PollAsync(CancellationToken.None);
        var body = Encoding.UTF8.GetString(records[0].Value!);
        var message = JsonSerializer.Deserialize<JsonElement>(body);

        var headers = message.GetProperty("headers");
        Assert.False(headers.TryGetProperty("X-API-Key", out _));
        Assert.True(headers.TryGetProperty("X-Other-Header", out _));
    }
}
