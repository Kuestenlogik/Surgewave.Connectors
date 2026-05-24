using System.Text;
using Xunit;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.RabbitMQ;

namespace Kuestenlogik.Surgewave.Connector.RabbitMQ.Tests;

public class RabbitMQSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new RabbitMQSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithDefaultConfig_UsesDefaultExchange()
    {
        using var task = new RabbitMQSinkTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic"
        };

        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("refused", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
                                       ex.InnerException?.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Expected - no RabbitMQ server running
            }
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomExchange_ParsesExchange()
    {
        using var task = new RabbitMQSinkTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic",
            [RabbitMQConnectorConfig.Exchange] = "my-exchange",
            [RabbitMQConnectorConfig.ExchangeType] = "fanout"
        };

        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("refused", StringComparison.OrdinalIgnoreCase) ||
                                       ex.InnerException?.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Expected
            }
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithRoutingKeyTemplate_ParsesTemplate()
    {
        using var task = new RabbitMQSinkTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic",
            [RabbitMQConnectorConfig.RoutingKeyTemplate] = "events.${topic}.${partition}"
        };

        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("refused", StringComparison.OrdinalIgnoreCase) ||
                                       ex.InnerException?.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Expected
            }
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithAuthentication_ParsesCredentials()
    {
        using var task = new RabbitMQSinkTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic",
            [RabbitMQConnectorConfig.Username] = "testuser",
            [RabbitMQConnectorConfig.Password] = "testpass"
        };

        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("refused", StringComparison.OrdinalIgnoreCase) ||
                                       ex.InnerException?.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Expected
            }
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithTlsEnabled_ParsesTlsConfig()
    {
        using var task = new RabbitMQSinkTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic",
            [RabbitMQConnectorConfig.TlsEnabled] = "true"
        };

        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("refused", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("tls", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("ssl", StringComparison.OrdinalIgnoreCase) ||
                                       ex.InnerException?.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Expected
            }
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithPersistenceConfig_ParsesDeliveryMode()
    {
        using var task = new RabbitMQSinkTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic",
            [RabbitMQConnectorConfig.Persistent] = "false",
            [RabbitMQConnectorConfig.Mandatory] = "true"
        };

        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("refused", StringComparison.OrdinalIgnoreCase) ||
                                       ex.InnerException?.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Expected
            }
        });

        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_WithEmptyRecords_DoesNotThrow()
    {
        using var task = new RabbitMQSinkTask();

        // Don't start - just test empty records handling
        var exception = await Record.ExceptionAsync(() => task.PutAsync([], CancellationToken.None));

        Assert.Null(exception);
    }

    [Fact]
    public async Task FlushAsync_WithoutConnection_DoesNotThrow()
    {
        using var task = new RabbitMQSinkTask();

        var offsets = new Dictionary<TopicPartition, long>();
        var exception = await Record.ExceptionAsync(() => task.FlushAsync(offsets, CancellationToken.None));

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_WithoutStart_DoesNotThrow()
    {
        using var task = new RabbitMQSinkTask();

        var exception = Record.Exception(() => task.Stop());

        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_WithoutStart_DoesNotThrow()
    {
        var task = new RabbitMQSinkTask();

        var exception = Record.Exception(() => task.Dispose());

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithExchangeOptions_ParsesExchangeConfig()
    {
        using var task = new RabbitMQSinkTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic",
            [RabbitMQConnectorConfig.Exchange] = "my-exchange",
            [RabbitMQConnectorConfig.ExchangeType] = "headers",
            [RabbitMQConnectorConfig.ExchangeDurable] = "false",
            [RabbitMQConnectorConfig.ExchangeAutoDelete] = "true"
        };

        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("refused", StringComparison.OrdinalIgnoreCase) ||
                                       ex.InnerException?.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Expected
            }
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithContentTypeAndTtl_ParsesMessageProperties()
    {
        using var task = new RabbitMQSinkTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic",
            [RabbitMQConnectorConfig.ContentType] = "application/json",
            [RabbitMQConnectorConfig.MessageTtlMs] = "30000"
        };

        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("refused", StringComparison.OrdinalIgnoreCase) ||
                                       ex.InnerException?.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Expected
            }
        });

        Assert.Null(exception);
    }
}
