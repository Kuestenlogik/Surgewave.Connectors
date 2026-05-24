using System.Security.Cryptography;
using System.Text;
using Kuestenlogik.Surgewave.Connector.Http;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Http.Tests;

/// <summary>
/// Tests for webhook registry functionality.
/// </summary>
public sealed class WebhookRegistryTests
{
    [Fact]
    public void WebhookRegistry_IsSingleton()
    {
        // Assert
        Assert.Same(WebhookRegistry.Instance, WebhookRegistry.Instance);
    }

    [Fact]
    public void Register_CreatesChannelReader()
    {
        // Arrange
        var name = "test-webhook-" + Guid.NewGuid();
        var config = new Dictionary<string, string>();

        try
        {
            // Act
            var reader = WebhookRegistry.Instance.Register(name, "/webhooks/test", config);

            // Assert
            Assert.NotNull(reader);
            Assert.True(WebhookRegistry.Instance.IsRegistered(name));
        }
        finally
        {
            WebhookRegistry.Instance.Unregister(name);
        }
    }

    [Fact]
    public void Register_ThrowsOnDuplicateName()
    {
        // Arrange
        var name = "test-webhook-duplicate-" + Guid.NewGuid();
        var config = new Dictionary<string, string>();

        try
        {
            WebhookRegistry.Instance.Register(name, "/webhooks/test1", config);

            // Act & Assert
            Assert.Throws<InvalidOperationException>(() =>
                WebhookRegistry.Instance.Register(name, "/webhooks/test2", config));
        }
        finally
        {
            WebhookRegistry.Instance.Unregister(name);
        }
    }

    [Fact]
    public void Unregister_RemovesWebhook()
    {
        // Arrange
        var name = "test-webhook-unregister-" + Guid.NewGuid();
        var config = new Dictionary<string, string>();
        WebhookRegistry.Instance.Register(name, "/webhooks/test", config);

        // Act
        WebhookRegistry.Instance.Unregister(name);

        // Assert
        Assert.False(WebhookRegistry.Instance.IsRegistered(name));
    }

    [Fact]
    public void GetReader_ReturnsNullForUnregistered()
    {
        // Act
        var reader = WebhookRegistry.Instance.GetReader("non-existent-webhook");

        // Assert
        Assert.Null(reader);
    }

    [Fact]
    public void FindConnectorByPath_ReturnsConnectorName()
    {
        // Arrange
        var name = "test-webhook-find-" + Guid.NewGuid();
        var path = "/webhooks/findtest-" + Guid.NewGuid();
        var config = new Dictionary<string, string>();

        try
        {
            WebhookRegistry.Instance.Register(name, path, config);

            // Act
            var found = WebhookRegistry.Instance.FindConnectorByPath(path);

            // Assert
            Assert.Equal(name, found);
        }
        finally
        {
            WebhookRegistry.Instance.Unregister(name);
        }
    }

    [Fact]
    public void FindConnectorByPath_ReturnsNullForUnknownPath()
    {
        // Act
        var found = WebhookRegistry.Instance.FindConnectorByPath("/unknown/path");

        // Assert
        Assert.Null(found);
    }

    [Fact]
    public void FindConnectorByPath_NormalizesPath()
    {
        // Arrange
        var name = "test-webhook-normalize-" + Guid.NewGuid();
        var config = new Dictionary<string, string>();

        try
        {
            WebhookRegistry.Instance.Register(name, "webhooks/normalize", config);

            // Act - search with leading slash
            var found = WebhookRegistry.Instance.FindConnectorByPath("/webhooks/normalize/");

            // Assert
            Assert.Equal(name, found);
        }
        finally
        {
            WebhookRegistry.Instance.Unregister(name);
        }
    }

    [Fact]
    public async Task ValidateAndEnqueue_ReturnsNotFound_WhenNotRegistered()
    {
        // Act
        var result = await WebhookRegistry.Instance.ValidateAndEnqueueAsync(
            "non-existent",
            [],
            new Dictionary<string, string>());

        // Assert
        Assert.Equal(WebhookValidationResult.NotFound, result);
    }

    [Fact]
    public async Task ValidateAndEnqueue_ReturnsMissingSignature_WhenSecretConfiguredButNoHeader()
    {
        // Arrange
        var name = "test-webhook-missing-sig-" + Guid.NewGuid();
        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.WebhookSecret] = "my-secret"
        };

        try
        {
            WebhookRegistry.Instance.Register(name, "/webhooks/test", config);
            var headers = new Dictionary<string, string>();

            // Act
            var result = await WebhookRegistry.Instance.ValidateAndEnqueueAsync(
                name,
                "test"u8.ToArray(),
                headers);

            // Assert
            Assert.Equal(WebhookValidationResult.MissingSignature, result);
        }
        finally
        {
            WebhookRegistry.Instance.Unregister(name);
        }
    }

    [Fact]
    public async Task ValidateAndEnqueue_ReturnsInvalidSignature_WhenSignatureDoesNotMatch()
    {
        // Arrange
        var name = "test-webhook-invalid-sig-" + Guid.NewGuid();
        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.WebhookSecret] = "my-secret"
        };

        try
        {
            WebhookRegistry.Instance.Register(name, "/webhooks/test", config);
            var headers = new Dictionary<string, string>
            {
                ["X-Signature-256"] = "sha256=invalid"
            };

            // Act
            var result = await WebhookRegistry.Instance.ValidateAndEnqueueAsync(
                name,
                "test"u8.ToArray(),
                headers);

            // Assert
            Assert.Equal(WebhookValidationResult.InvalidSignature, result);
        }
        finally
        {
            WebhookRegistry.Instance.Unregister(name);
        }
    }

    [Fact]
    public async Task ValidateAndEnqueue_ReturnsSuccess_WhenSignatureIsValid()
    {
        // Arrange
        var name = "test-webhook-valid-sig-" + Guid.NewGuid();
        var secret = "my-secret";
        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.WebhookSecret] = secret
        };

        try
        {
            WebhookRegistry.Instance.Register(name, "/webhooks/test", config);

            var body = "test-body"u8.ToArray();
            using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
            var signature = "sha256=" + Convert.ToHexString(hmac.ComputeHash(body)).ToLowerInvariant();

            var headers = new Dictionary<string, string>
            {
                ["X-Signature-256"] = signature
            };

            // Act
            var result = await WebhookRegistry.Instance.ValidateAndEnqueueAsync(
                name,
                body,
                headers);

            // Assert
            Assert.Equal(WebhookValidationResult.Success, result);
        }
        finally
        {
            WebhookRegistry.Instance.Unregister(name);
        }
    }

    [Fact]
    public async Task ValidateAndEnqueue_ReturnsSuccess_WhenNoSecretConfigured()
    {
        // Arrange
        var name = "test-webhook-no-secret-" + Guid.NewGuid();
        var config = new Dictionary<string, string>();

        try
        {
            WebhookRegistry.Instance.Register(name, "/webhooks/test", config);

            // Act
            var result = await WebhookRegistry.Instance.ValidateAndEnqueueAsync(
                name,
                "test"u8.ToArray(),
                new Dictionary<string, string>());

            // Assert
            Assert.Equal(WebhookValidationResult.Success, result);
        }
        finally
        {
            WebhookRegistry.Instance.Unregister(name);
        }
    }

    [Fact]
    public async Task ValidateAndEnqueue_ReturnsInvalidTimestamp_WhenTimestampExpired()
    {
        // Arrange
        var name = "test-webhook-timestamp-" + Guid.NewGuid();
        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.WebhookValidateTimestamp] = "true",
            [HttpConnectorConfig.WebhookTimestampToleranceMs] = "1000" // 1 second
        };

        try
        {
            WebhookRegistry.Instance.Register(name, "/webhooks/test", config);

            var oldTimestamp = DateTimeOffset.UtcNow.AddMinutes(-5).ToUnixTimeMilliseconds().ToString();
            var headers = new Dictionary<string, string>
            {
                ["X-Timestamp"] = oldTimestamp
            };

            // Act
            var result = await WebhookRegistry.Instance.ValidateAndEnqueueAsync(
                name,
                "test"u8.ToArray(),
                headers);

            // Assert
            Assert.Equal(WebhookValidationResult.InvalidTimestamp, result);
        }
        finally
        {
            WebhookRegistry.Instance.Unregister(name);
        }
    }

    [Fact]
    public async Task ValidateAndEnqueue_ReturnsSuccess_WhenTimestampIsValid()
    {
        // Arrange
        var name = "test-webhook-valid-timestamp-" + Guid.NewGuid();
        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.WebhookValidateTimestamp] = "true"
        };

        try
        {
            WebhookRegistry.Instance.Register(name, "/webhooks/test", config);

            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();
            var headers = new Dictionary<string, string>
            {
                ["X-Timestamp"] = timestamp
            };

            // Act
            var result = await WebhookRegistry.Instance.ValidateAndEnqueueAsync(
                name,
                "test"u8.ToArray(),
                headers);

            // Assert
            Assert.Equal(WebhookValidationResult.Success, result);
        }
        finally
        {
            WebhookRegistry.Instance.Unregister(name);
        }
    }

    [Fact]
    public async Task ValidateAndEnqueue_EventsCanBeRead()
    {
        // Arrange
        var name = "test-webhook-read-" + Guid.NewGuid();
        var config = new Dictionary<string, string>();

        try
        {
            var reader = WebhookRegistry.Instance.Register(name, "/webhooks/test", config);

            var body = "{\"message\":\"hello\"}"u8.ToArray();
            var headers = new Dictionary<string, string>
            {
                ["Content-Type"] = "application/json"
            };

            // Act
            var result = await WebhookRegistry.Instance.ValidateAndEnqueueAsync(name, body, headers);
            Assert.Equal(WebhookValidationResult.Success, result);

            // Read the event
            var eventRead = reader.TryRead(out var webhookEvent);

            // Assert
            Assert.True(eventRead);
            Assert.NotNull(webhookEvent);
            Assert.Equal(name, webhookEvent.ConnectorName);
            Assert.Equal(body, webhookEvent.Body);
            Assert.True(webhookEvent.Headers.ContainsKey("Content-Type"));
        }
        finally
        {
            WebhookRegistry.Instance.Unregister(name);
        }
    }

    [Fact]
    public void WebhookEvent_ContainsAllProperties()
    {
        // Arrange
        var body = "test"u8.ToArray();
        var headers = new Dictionary<string, string> { ["key"] = "value" };
        var timestamp = DateTimeOffset.UtcNow;

        // Act
        var webhookEvent = new WebhookEvent("connector", body, headers, timestamp);

        // Assert
        Assert.Equal("connector", webhookEvent.ConnectorName);
        Assert.Equal(body, webhookEvent.Body);
        Assert.Equal(headers, webhookEvent.Headers);
        Assert.Equal(timestamp, webhookEvent.ReceivedAt);
    }

    [Fact]
    public async Task ValidateAndEnqueue_ReturnsChannelClosed_WhenUnregistered()
    {
        // Arrange
        var name = "test-webhook-closed-" + Guid.NewGuid();
        var config = new Dictionary<string, string>();

        // Register and immediately unregister
        WebhookRegistry.Instance.Register(name, "/webhooks/test", config);
        WebhookRegistry.Instance.Unregister(name);

        // Act
        var result = await WebhookRegistry.Instance.ValidateAndEnqueueAsync(
            name,
            "test"u8.ToArray(),
            new Dictionary<string, string>());

        // Assert - now returns NotFound since webhook is unregistered
        Assert.Equal(WebhookValidationResult.NotFound, result);
    }

    [Fact]
    public void GetRegisteredPaths_ReturnsAllPaths()
    {
        // Arrange
        var name1 = "test-webhook-paths1-" + Guid.NewGuid();
        var name2 = "test-webhook-paths2-" + Guid.NewGuid();
        var path1 = "/webhooks/path1-" + Guid.NewGuid();
        var path2 = "/webhooks/path2-" + Guid.NewGuid();
        var config = new Dictionary<string, string>();

        try
        {
            WebhookRegistry.Instance.Register(name1, path1, config);
            WebhookRegistry.Instance.Register(name2, path2, config);

            // Act
            var paths = WebhookRegistry.Instance.GetRegisteredPaths();

            // Assert
            Assert.Contains(paths, kv => kv.Value == name1);
            Assert.Contains(paths, kv => kv.Value == name2);
        }
        finally
        {
            WebhookRegistry.Instance.Unregister(name1);
            WebhookRegistry.Instance.Unregister(name2);
        }
    }
}
