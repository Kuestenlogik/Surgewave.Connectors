using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.Http;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Http.Tests;

/// <summary>
/// Tests for HTTP source and sink connectors.
/// </summary>
public sealed class HttpConnectorTests
{
    [Fact]
    public void HttpSourceConnector_HasCorrectConfig()
    {
        // Arrange
        using var connector = new HttpSourceConnector();

        // Assert
        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(HttpSourceTask), connector.TaskClass);
        Assert.NotNull(connector.Config);

        // Verify key config definitions
        var configKeys = connector.Config.Keys;
        Assert.Contains(configKeys, k => k.Name == "http.url");
        Assert.Contains(configKeys, k => k.Name == "topic");
        Assert.Contains(configKeys, k => k.Name == "http.method");
        Assert.Contains(configKeys, k => k.Name == "poll.interval.ms");
    }

    [Fact]
    public void HttpSinkConnector_HasCorrectConfig()
    {
        // Arrange
        using var connector = new HttpSinkConnector();

        // Assert
        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(HttpSinkTask), connector.TaskClass);
        Assert.NotNull(connector.Config);

        // Verify key config definitions
        var configKeys = connector.Config.Keys;
        Assert.Contains(configKeys, k => k.Name == "http.url");
        Assert.Contains(configKeys, k => k.Name == "topics");
        Assert.Contains(configKeys, k => k.Name == "http.method");
        Assert.Contains(configKeys, k => k.Name == "batch.mode");
    }

    [Fact]
    public void HttpSourceConnector_ThrowsOnMissingUrl()
    {
        // Arrange
        using var connector = new HttpSourceConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            ["topic"] = "http-data"
            // Missing url config
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void HttpSourceConnector_ThrowsOnMissingTopic()
    {
        // Arrange
        using var connector = new HttpSourceConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            ["http.url"] = "https://api.example.com/data"
            // Missing topic config
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void HttpSinkConnector_ThrowsOnMissingUrl()
    {
        // Arrange
        using var connector = new HttpSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            ["topics"] = "topic1,topic2"
            // Missing url config
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void HttpSinkConnector_ThrowsOnMissingTopics()
    {
        // Arrange
        using var connector = new HttpSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            ["http.url"] = "https://api.example.com/sink"
            // Missing topics config
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void HttpSourceConnector_ProducesTaskConfigs()
    {
        // Arrange
        using var connector = new HttpSourceConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            ["http.url"] = "https://api.example.com/data",
            ["topic"] = "http-data",
            ["poll.interval.ms"] = "30000",
            ["http.method"] = "POST"
        };

        // Act
        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        // Assert
        Assert.Single(taskConfigs);
        Assert.Equal("https://api.example.com/data", taskConfigs[0]["http.url"]);
        Assert.Equal("http-data", taskConfigs[0]["topic"]);
        Assert.Equal("30000", taskConfigs[0]["poll.interval.ms"]);

        // Cleanup
        connector.Stop();
    }

    [Fact]
    public void HttpSinkConnector_ProducesTaskConfigs()
    {
        // Arrange
        using var connector = new HttpSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            ["http.url"] = "https://api.example.com/sink",
            ["topics"] = "topic1,topic2",
            ["batch.mode"] = "array",
            ["http.method"] = "PUT"
        };

        // Act
        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        // Assert
        Assert.Single(taskConfigs);
        Assert.Equal("https://api.example.com/sink", taskConfigs[0]["http.url"]);
        Assert.Equal("topic1,topic2", taskConfigs[0]["topics"]);
        Assert.Equal("array", taskConfigs[0]["batch.mode"]);

        // Cleanup
        connector.Stop();
    }

    [Fact]
    public void HttpSourceTask_StartsWithValidConfig()
    {
        // Arrange
        var task = new HttpSourceTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            ["http.url"] = "https://api.example.com/data",
            ["topic"] = "http-data",
            ["poll.interval.ms"] = "5000",
            ["http.method"] = "GET",
            ["response.mode"] = "raw",
            ["http.headers"] = ""
        };

        // Act
        task.Start(config);

        // Assert - should not throw
        Assert.Equal("1.0.0", task.Version);

        // Cleanup
        task.Stop();
        task.Dispose();
    }

    [Fact]
    public void HttpSinkTask_StartsWithValidConfig()
    {
        // Arrange
        var task = new HttpSinkTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            ["http.url"] = "https://api.example.com/sink",
            ["http.method"] = "POST",
            ["batch.mode"] = "single",
            ["content.type"] = "application/json",
            ["http.headers"] = "",
            ["retry.max"] = "3",
            ["retry.backoff.ms"] = "1000"
        };

        // Act
        task.Start(config);

        // Assert - should not throw
        Assert.Equal("1.0.0", task.Version);

        // Cleanup
        task.Stop();
        task.Dispose();
    }

    [Fact]
    public void HttpSourceConnector_DefaultPollingInterval()
    {
        // Verify the default polling config key exists
        using var connector = new HttpSourceConnector();
        var configKeys = connector.Config.Keys;
        var pollIntervalConfig = configKeys.First(k => k.Name == "poll.interval.ms");

        Assert.Equal(5000L, pollIntervalConfig.DefaultValue);
    }

    [Fact]
    public void HttpSinkConnector_DefaultBatchMode()
    {
        // Verify the default batch mode config key exists
        using var connector = new HttpSinkConnector();
        var configKeys = connector.Config.Keys;
        var batchModeConfig = configKeys.First(k => k.Name == "batch.mode");

        Assert.Equal("single", batchModeConfig.DefaultValue);
    }

    [Fact]
    public void HttpSourceConnector_SupportsCustomHeaders()
    {
        // Verify headers config key exists
        using var connector = new HttpSourceConnector();
        var configKeys = connector.Config.Keys;

        Assert.Contains(configKeys, k => k.Name == "http.headers");
    }

    [Fact]
    public void HttpSinkConnector_SupportsRetryConfig()
    {
        // Verify retry config keys exist
        using var connector = new HttpSinkConnector();
        var configKeys = connector.Config.Keys;

        Assert.Contains(configKeys, k => k.Name == "retry.max");
        Assert.Contains(configKeys, k => k.Name == "retry.backoff.ms");
    }

    [Fact]
    public void HttpSourceConnector_SupportsWebhookMode()
    {
        // Verify webhook config keys exist
        using var connector = new HttpSourceConnector();
        var configKeys = connector.Config.Keys;

        Assert.Contains(configKeys, k => k.Name == HttpConnectorConfig.SourceMode);
        Assert.Contains(configKeys, k => k.Name == HttpConnectorConfig.WebhookPath);
        Assert.Contains(configKeys, k => k.Name == HttpConnectorConfig.WebhookSecret);
        Assert.Contains(configKeys, k => k.Name == HttpConnectorConfig.WebhookSignatureHeader);
        Assert.Contains(configKeys, k => k.Name == HttpConnectorConfig.WebhookValidateTimestamp);
    }

    [Fact]
    public void HttpSourceConnector_WebhookMode_DoesNotRequireUrl()
    {
        // Arrange
        using var connector = new HttpSourceConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.SourceMode] = HttpConnectorConfig.SourceModeWebhook,
            [HttpConnectorConfig.Topic] = "webhook-events"
            // Note: no URL required for webhook mode
        };

        // Act - should not throw
        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        // Assert
        Assert.Single(taskConfigs);
        Assert.Equal(HttpConnectorConfig.SourceModeWebhook, taskConfigs[0][HttpConnectorConfig.SourceMode]);

        // Cleanup
        connector.Stop();
    }

    [Fact]
    public void HttpSinkConnector_SupportsAuthentication()
    {
        // Verify auth config keys exist
        using var connector = new HttpSinkConnector();
        var configKeys = connector.Config.Keys;

        Assert.Contains(configKeys, k => k.Name == HttpConnectorConfig.AuthType);
        Assert.Contains(configKeys, k => k.Name == HttpConnectorConfig.AuthUsername);
        Assert.Contains(configKeys, k => k.Name == HttpConnectorConfig.AuthPassword);
        Assert.Contains(configKeys, k => k.Name == HttpConnectorConfig.AuthToken);
        Assert.Contains(configKeys, k => k.Name == HttpConnectorConfig.AuthApiKey);
        Assert.Contains(configKeys, k => k.Name == HttpConnectorConfig.AuthHmacSecret);
    }

    [Fact]
    public void HttpSinkConnector_ThrowsOnMissingBasicAuthUsername()
    {
        // Arrange
        using var connector = new HttpSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.Url] = "https://api.example.com/sink",
            [HttpConnectorConfig.Topics] = "topic1",
            [HttpConnectorConfig.AuthType] = HttpConnectorConfig.AuthTypeBasic
            // Missing username
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void HttpSinkConnector_ThrowsOnMissingBearerToken()
    {
        // Arrange
        using var connector = new HttpSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.Url] = "https://api.example.com/sink",
            [HttpConnectorConfig.Topics] = "topic1",
            [HttpConnectorConfig.AuthType] = HttpConnectorConfig.AuthTypeBearer
            // Missing token
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void HttpSinkConnector_StartsWithValidAuthConfig()
    {
        // Arrange
        using var connector = new HttpSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.Url] = "https://api.example.com/sink",
            [HttpConnectorConfig.Topics] = "topic1",
            [HttpConnectorConfig.AuthType] = HttpConnectorConfig.AuthTypeBearer,
            [HttpConnectorConfig.AuthToken] = "my-token"
        };

        // Act - should not throw
        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        // Assert
        Assert.Single(taskConfigs);
        Assert.Equal(HttpConnectorConfig.AuthTypeBearer, taskConfigs[0][HttpConnectorConfig.AuthType]);

        // Cleanup
        connector.Stop();
    }

    [Fact]
    public void HttpSinkTask_StartsWithAuthConfig()
    {
        // Arrange
        var task = new HttpSinkTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.Url] = "https://api.example.com/sink",
            [HttpConnectorConfig.AuthType] = HttpConnectorConfig.AuthTypeApiKey,
            [HttpConnectorConfig.AuthApiKey] = "my-api-key"
        };

        // Act - should not throw
        task.Start(config);

        // Assert
        Assert.Equal("1.0.0", task.Version);

        // Cleanup
        task.Stop();
        task.Dispose();
    }

    [Fact]
    public void HttpSourceTask_StartsInWebhookMode()
    {
        // Arrange
        var task = new HttpSourceTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.Topic] = "webhook-events",
            [HttpConnectorConfig.SourceMode] = HttpConnectorConfig.SourceModeWebhook,
            ["name"] = "test-webhook-task-" + Guid.NewGuid()
        };

        // Act - should not throw
        task.Start(config);

        // Assert
        Assert.Equal("1.0.0", task.Version);

        // Cleanup
        task.Stop();
        task.Dispose();
    }

    [Fact]
    public void HttpConnectorConfig_HasSseConstants()
    {
        // Verify SSE-related config constants exist
        Assert.Equal("text/event-stream", HttpConnectorConfig.SseContentType);
        Assert.Equal("sse.reconnect.delay.ms", HttpConnectorConfig.SseReconnectDelayMs);
        Assert.Equal(3000L, HttpConnectorConfig.DefaultSseReconnectDelayMs);
        Assert.Equal("last_event_id", HttpConnectorConfig.OffsetLastEventId);
    }

    [Fact]
    public void SseEventBuilder_BuildsSimpleEvent()
    {
        // Arrange
        var builder = new SseEventBuilder();
        builder.AppendData("Hello, World!");

        // Act
        var sseEvent = builder.Build();

        // Assert
        Assert.Equal("Hello, World!", sseEvent.Data);
        Assert.Null(sseEvent.EventType);
        Assert.Null(sseEvent.Id);
        Assert.True(builder.HasData);
    }

    [Fact]
    public void SseEventBuilder_ConcatenatesMultilineData()
    {
        // Arrange
        var builder = new SseEventBuilder();
        builder.AppendData("Line 1");
        builder.AppendData("Line 2");
        builder.AppendData("Line 3");

        // Act
        var sseEvent = builder.Build();

        // Assert
        Assert.Equal("Line 1\nLine 2\nLine 3", sseEvent.Data);
    }

    [Fact]
    public void SseEventBuilder_IncludesEventTypeAndId()
    {
        // Arrange
        var builder = new SseEventBuilder();
        builder.AppendData("{\"key\": \"value\"}");
        builder.EventType = "message";
        builder.Id = "event-123";

        // Act
        var sseEvent = builder.Build();

        // Assert
        Assert.Equal("{\"key\": \"value\"}", sseEvent.Data);
        Assert.Equal("message", sseEvent.EventType);
        Assert.Equal("event-123", sseEvent.Id);
    }

    [Fact]
    public void SseEventBuilder_HasDataReturnsFalseWhenEmpty()
    {
        // Arrange
        var builder = new SseEventBuilder();

        // Assert
        Assert.False(builder.HasData);
    }

    [Fact]
    public void SseEvent_RecordEquality()
    {
        // Arrange
        var timestamp = DateTimeOffset.UtcNow;
        var event1 = new SseEvent("data", "message", "123", timestamp);
        var event2 = new SseEvent("data", "message", "123", timestamp);
        var event3 = new SseEvent("different", "message", "123", timestamp);

        // Assert
        Assert.Equal(event1, event2);
        Assert.NotEqual(event1, event3);
    }

    [Fact]
    public void HttpSourceTask_StartsWithSseReconnectConfig()
    {
        // Arrange
        var task = new HttpSourceTask();
        var context = new TaskContext
        {
            RaiseError = _ => { }
        };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            ["http.url"] = "https://api.example.com/events",
            ["topic"] = "sse-events",
            [HttpConnectorConfig.SseReconnectDelayMs] = "5000"
        };

        // Act - should not throw
        task.Start(config);

        // Assert
        Assert.Equal("1.0.0", task.Version);

        // Cleanup
        task.Stop();
        task.Dispose();
    }
}
