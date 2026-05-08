using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.HttpServer.Tests;

public class HttpServerSinkConnectorTests : IDisposable
{
    private readonly HttpServerSinkTask _task;
    private readonly HttpClient _httpClient;
    private readonly int _port;

    public HttpServerSinkConnectorTests()
    {
        _port = 28080 + Random.Shared.Next(1000);
        _task = new HttpServerSinkTask();
        _httpClient = new HttpClient { BaseAddress = new Uri($"http://localhost:{_port}") };

        var config = new Dictionary<string, string>
        {
            [HttpServerConnectorConfig.Host] = "localhost",
            [HttpServerConnectorConfig.Port] = _port.ToString(),
            [HttpServerConnectorConfig.BasePath] = "/api",
            [HttpServerConnectorConfig.SinkTopics] = "topic-a,topic-b",
            [HttpServerConnectorConfig.SinkMaxMessages] = "100",
            [HttpServerConnectorConfig.SinkDefaultLimit] = "10"
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
    public async Task ListTopics_ReturnsConfiguredTopics()
    {
        var response = await _httpClient.GetAsync("/api/topics");
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync();
        var result = JsonSerializer.Deserialize<JsonElement>(content);

        var topics = result.GetProperty("topics").EnumerateArray().Select(t => t.GetProperty("name").GetString()).ToList();
        Assert.Contains("topic-a", topics);
        Assert.Contains("topic-b", topics);
    }

    [Fact]
    public async Task GetMessages_FromEmptyTopic_ReturnsEmpty()
    {
        var response = await _httpClient.GetAsync("/api/topics/topic-a");
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync();
        var result = JsonSerializer.Deserialize<JsonElement>(content);

        Assert.Equal(0, result.GetProperty("count").GetInt32());
        Assert.Empty(result.GetProperty("messages").EnumerateArray());
    }

    [Fact]
    public async Task GetMessages_AfterPut_ReturnsMessages()
    {
        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "topic-a",
                Partition = 0,
                Offset = 1,
                Timestamp = DateTimeOffset.UtcNow,
                Value = Encoding.UTF8.GetBytes("{\"msg\":\"hello\"}")
            }
        };

        await _task.PutAsync(records, CancellationToken.None);

        var response = await _httpClient.GetAsync("/api/topics/topic-a");
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync();
        var result = JsonSerializer.Deserialize<JsonElement>(content);

        Assert.Equal(1, result.GetProperty("count").GetInt32());
        var messages = result.GetProperty("messages").EnumerateArray().ToList();
        Assert.Single(messages);
        Assert.Equal(1, messages[0].GetProperty("offset").GetInt64());
    }

    [Fact]
    public async Task GetMessages_WithLimit_RespectsLimit()
    {
        var records = new List<SinkRecord>();
        for (int i = 0; i < 20; i++)
        {
            records.Add(new SinkRecord
            {
                Topic = "topic-a",
                Partition = 0,
                Offset = i,
                Timestamp = DateTimeOffset.UtcNow,
                Value = Encoding.UTF8.GetBytes($"{{\"index\":{i}}}")
            });
        }

        await _task.PutAsync(records, CancellationToken.None);

        var response = await _httpClient.GetAsync("/api/topics/topic-a?limit=5");
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync();
        var result = JsonSerializer.Deserialize<JsonElement>(content);

        Assert.Equal(5, result.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task GetMessages_WithFromOffset_StartsFromOffset()
    {
        var records = new List<SinkRecord>();
        for (int i = 0; i < 10; i++)
        {
            records.Add(new SinkRecord
            {
                Topic = "topic-a",
                Partition = 0,
                Offset = i,
                Timestamp = DateTimeOffset.UtcNow,
                Value = Encoding.UTF8.GetBytes($"{{\"index\":{i}}}")
            });
        }

        await _task.PutAsync(records, CancellationToken.None);

        var response = await _httpClient.GetAsync("/api/topics/topic-a?from=5");
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync();
        var result = JsonSerializer.Deserialize<JsonElement>(content);

        var messages = result.GetProperty("messages").EnumerateArray().ToList();
        Assert.Equal(5, messages.Count);
        Assert.Equal(5, messages[0].GetProperty("offset").GetInt64());
    }

    [Fact]
    public async Task GetSingleMessage_ByOffset_ReturnsMessage()
    {
        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "topic-a",
                Partition = 0,
                Offset = 42,
                Timestamp = DateTimeOffset.UtcNow,
                Value = Encoding.UTF8.GetBytes("{\"special\":\"message\"}")
            }
        };

        await _task.PutAsync(records, CancellationToken.None);

        var response = await _httpClient.GetAsync("/api/topics/topic-a/42");
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync();
        var result = JsonSerializer.Deserialize<JsonElement>(content);

        Assert.Equal(42, result.GetProperty("offset").GetInt64());
    }

    [Fact]
    public async Task GetSingleMessage_NonExistent_ReturnsNotFound()
    {
        var response = await _httpClient.GetAsync("/api/topics/topic-a/999");
        Assert.Equal(System.Net.HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task GetMessages_NonExistentTopic_ReturnsNotFound()
    {
        var response = await _httpClient.GetAsync("/api/topics/nonexistent");
        Assert.Equal(System.Net.HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task RingBuffer_EvictsOldMessages()
    {
        var task = new HttpServerSinkTask();
        var port = 28180 + Random.Shared.Next(100);

        var config = new Dictionary<string, string>
        {
            [HttpServerConnectorConfig.Host] = "localhost",
            [HttpServerConnectorConfig.Port] = port.ToString(),
            [HttpServerConnectorConfig.BasePath] = "/api",
            [HttpServerConnectorConfig.SinkTopics] = "small-buffer",
            [HttpServerConnectorConfig.SinkMaxMessages] = "5"
        };

        task.Start(config);

        try
        {
            using var client = new HttpClient { BaseAddress = new Uri($"http://localhost:{port}") };

            // Add 10 messages to a buffer of size 5
            var records = new List<SinkRecord>();
            for (int i = 0; i < 10; i++)
            {
                records.Add(new SinkRecord
                {
                    Topic = "small-buffer",
                    Partition = 0,
                    Offset = i,
                    Timestamp = DateTimeOffset.UtcNow,
                    Value = Encoding.UTF8.GetBytes($"{{\"index\":{i}}}")
                });
            }

            await task.PutAsync(records, CancellationToken.None);

            var response = await client.GetAsync("/api/topics/small-buffer");
            response.EnsureSuccessStatusCode();

            var content = await response.Content.ReadAsStringAsync();
            var result = JsonSerializer.Deserialize<JsonElement>(content);

            // Should only have the last 5 messages (offsets 5-9)
            var messages = result.GetProperty("messages").EnumerateArray().ToList();
            Assert.Equal(5, messages.Count);
            Assert.Equal(5, messages[0].GetProperty("offset").GetInt64());
            Assert.Equal(9, messages[4].GetProperty("offset").GetInt64());
        }
        finally
        {
            task.Stop();
            task.Dispose();
        }
    }

    [Fact]
    public async Task MessageWithKey_IncludesKeyInResponse()
    {
        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "topic-a",
                Partition = 0,
                Offset = 1,
                Timestamp = DateTimeOffset.UtcNow,
                Key = Encoding.UTF8.GetBytes("my-key"),
                Value = Encoding.UTF8.GetBytes("{\"msg\":\"test\"}")
            }
        };

        await _task.PutAsync(records, CancellationToken.None);

        var response = await _httpClient.GetAsync("/api/topics/topic-a");
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync();
        var result = JsonSerializer.Deserialize<JsonElement>(content);

        var message = result.GetProperty("messages").EnumerateArray().First();
        Assert.Equal("my-key", message.GetProperty("key").GetString());
    }

    [Fact]
    public async Task MessageWithHeaders_IncludesHeadersInResponse()
    {
        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "topic-a",
                Partition = 0,
                Offset = 1,
                Timestamp = DateTimeOffset.UtcNow,
                Value = Encoding.UTF8.GetBytes("{\"msg\":\"test\"}"),
                Headers = new Dictionary<string, byte[]>
                {
                    ["X-Custom"] = Encoding.UTF8.GetBytes("header-value")
                }
            }
        };

        await _task.PutAsync(records, CancellationToken.None);

        var response = await _httpClient.GetAsync("/api/topics/topic-a");
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync();
        var result = JsonSerializer.Deserialize<JsonElement>(content);

        var message = result.GetProperty("messages").EnumerateArray().First();
        var headers = message.GetProperty("headers");
        Assert.Equal("header-value", headers.GetProperty("X-Custom").GetString());
    }
}
