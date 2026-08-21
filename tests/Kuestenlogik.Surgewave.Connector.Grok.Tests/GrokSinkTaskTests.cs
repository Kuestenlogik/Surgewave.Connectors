namespace Kuestenlogik.Surgewave.Connector.Grok.Tests;

using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class GrokSinkTaskTests
{
    [Fact]
    public void GrokSinkTask_HasCorrectVersion()
    {
        using var task = new GrokSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void GrokSinkTask_Start_ThrowsOnMissingApiKey()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        // Clear environment variable if set
        var originalApiKey = Environment.GetEnvironmentVariable("XAI_API_KEY");
        try
        {
            Environment.SetEnvironmentVariable("XAI_API_KEY", null);

            var config = new Dictionary<string, string>
            {
                [GrokConnectorConfig.TopicsConfig] = "test-topic"
            };

            var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
            Assert.Contains(GrokConnectorConfig.ApiKeyConfig, ex.Message);
        }
        finally
        {
            Environment.SetEnvironmentVariable("XAI_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public void GrokSinkTask_Start_AcceptsValidConfig()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void GrokSinkTask_Start_AcceptsApiKeyFromEnvironment()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var originalApiKey = Environment.GetEnvironmentVariable("XAI_API_KEY");
        try
        {
            Environment.SetEnvironmentVariable("XAI_API_KEY", "test-env-api-key");

            var config = new Dictionary<string, string>
            {
                [GrokConnectorConfig.TopicsConfig] = "test-topic",
                [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt"
            };

            // Should not throw - API key from environment
            task.Start(config);
            task.Stop();
        }
        finally
        {
            Environment.SetEnvironmentVariable("XAI_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public void GrokSinkTask_Start_AcceptsCustomBaseUrl()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt",
            [GrokConnectorConfig.BaseUrlConfig] = "https://custom.api.x.ai/v1"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void GrokSinkTask_Start_AppliesConfigValues()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt",
            [GrokConnectorConfig.ModelConfig] = "grok-3-mini",
            [GrokConnectorConfig.MaxTokensConfig] = "2048",
            [GrokConnectorConfig.TemperatureConfig] = "0.5",
            [GrokConnectorConfig.TopPConfig] = "0.9",
            [GrokConnectorConfig.BatchSizeConfig] = "20",
            [GrokConnectorConfig.BatchTimeoutMsConfig] = "3000",
            [GrokConnectorConfig.RetryMaxConfig] = "5",
            [GrokConnectorConfig.RetryBackoffMsConfig] = "2000"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void GrokSinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        task.Start(config);
        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void GrokSinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        task.Start(config);
        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public async Task GrokSinkTask_PutAsync_HandlesEmptyRecords()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        task.Start(config);

        // Should not throw on empty records
        await task.PutAsync(Array.Empty<SinkRecord>());

        task.Stop();
    }

    [Fact]
    public async Task GrokSinkTask_FlushAsync_DrainsBufferedRecordsToWebhook()
    {
        var port = 27600 + (Environment.ProcessId % 200);
        using var listener = new HttpListener();
        listener.Prefixes.Add($"http://localhost:{port}/");
        listener.Start();

        var webhookHits = 0;
        var serverTask = Task.Run(() => RunMockServerAsync(listener, webhookStatusCode: 200, onWebhookHit: () => Interlocked.Increment(ref webhookHits)));

        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());
        task.Start(CreateMockServerConfig(port));

        try
        {
            // Below batch size: PutAsync only buffers, nothing is delivered yet
            await task.PutAsync([CreateRecord()]);
            Assert.Equal(0, Volatile.Read(ref webhookHits));

            // FlushAsync must drain the buffer - the worker commits consumer offsets
            // right after it, so anything left in the buffer would be lost on restart
            await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
            Assert.Equal(1, Volatile.Read(ref webhookHits));
        }
        finally
        {
            task.Stop();
            listener.Stop();
            await serverTask;
        }
    }

    [Fact]
    public async Task GrokSinkTask_FlushAsync_ThrowsWhenWebhookDeliveryFails()
    {
        var port = 27800 + (Environment.ProcessId % 200);
        using var listener = new HttpListener();
        listener.Prefixes.Add($"http://localhost:{port}/");
        listener.Start();

        var serverTask = Task.Run(() => RunMockServerAsync(listener, webhookStatusCode: 500, onWebhookHit: () => { }));

        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());
        task.Start(CreateMockServerConfig(port));

        try
        {
            await task.PutAsync([CreateRecord()]);

            // A failing webhook means the result was NOT delivered - the flush must
            // throw so the worker retries/DLQs instead of committing the offset
            await Assert.ThrowsAsync<HttpRequestException>(
                () => task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None));
        }
        finally
        {
            task.Stop();
            listener.Stop();
            await serverTask;
        }
    }

    private static Dictionary<string, string> CreateMockServerConfig(int port)
    {
        return new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt",
            [GrokConnectorConfig.BaseUrlConfig] = $"http://localhost:{port}/v1",
            [GrokConnectorConfig.WebhookUrlConfig] = $"http://localhost:{port}/webhook",
            [GrokConnectorConfig.BatchSizeConfig] = "100",
            [GrokConnectorConfig.BatchTimeoutMsConfig] = "600000",
            [GrokConnectorConfig.RetryMaxConfig] = "0",
            [GrokConnectorConfig.RetryBackoffMsConfig] = "1"
        };
    }

    private static SinkRecord CreateRecord()
    {
        return new SinkRecord
        {
            Topic = "test-topic",
            Partition = 0,
            Offset = 0,
            Value = Encoding.UTF8.GetBytes("{\"text\":\"hello\"}"),
            Timestamp = DateTimeOffset.UtcNow
        };
    }

    private static async Task RunMockServerAsync(HttpListener listener, int webhookStatusCode, Action onWebhookHit)
    {
        const string completionJson = """
            {
              "id": "chatcmpl-test",
              "object": "chat.completion",
              "created": 1700000000,
              "model": "grok-3",
              "choices": [
                {
                  "index": 0,
                  "message": { "role": "assistant", "content": "Hello from mock" },
                  "finish_reason": "stop"
                }
              ],
              "usage": { "prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3 }
            }
            """;

        try
        {
            while (listener.IsListening)
            {
                var ctx = await listener.GetContextAsync();

                if (ctx.Request.Url!.AbsolutePath.EndsWith("/chat/completions", StringComparison.Ordinal))
                {
                    var body = Encoding.UTF8.GetBytes(completionJson);
                    ctx.Response.ContentType = "application/json";
                    ctx.Response.StatusCode = 200;
                    await ctx.Response.OutputStream.WriteAsync(body);
                }
                else
                {
                    onWebhookHit();
                    ctx.Response.StatusCode = webhookStatusCode;
                }

                ctx.Response.Close();
            }
        }
        catch (HttpListenerException)
        {
            // Listener stopped
        }
        catch (ObjectDisposedException)
        {
            // Listener disposed
        }
    }

    private static TaskContext CreateTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
