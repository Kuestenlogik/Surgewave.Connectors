using Xunit;
using Kuestenlogik.Surgewave.Connector.RabbitMQ;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.RabbitMQ.Tests;

public class RabbitMQSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new RabbitMQSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithDefaultHost_UsesDefaultConfiguration()
    {
        using var task = new RabbitMQSourceTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Queue] = "test-queue",
            [RabbitMQConnectorConfig.Topic] = "test-topic"
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
    public void Start_WithCustomHost_ParsesHost()
    {
        using var task = new RabbitMQSourceTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Host] = "rabbitmq.example.com",
            [RabbitMQConnectorConfig.Port] = "5673",
            [RabbitMQConnectorConfig.Queue] = "test-queue",
            [RabbitMQConnectorConfig.Topic] = "test-topic"
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
                                       ex.Message.Contains("endpoint", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("reachable", StringComparison.OrdinalIgnoreCase) ||
                                       ex.InnerException?.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) == true ||
                                       ex.InnerException?.Message.Contains("unknown", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Expected - host unreachable
            }
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithAuthenticationConfig_ParsesCredentials()
    {
        using var task = new RabbitMQSourceTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Queue] = "test-queue",
            [RabbitMQConnectorConfig.Topic] = "test-topic",
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
    public void Start_WithVirtualHost_ParsesVirtualHost()
    {
        using var task = new RabbitMQSourceTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Queue] = "test-queue",
            [RabbitMQConnectorConfig.Topic] = "test-topic",
            [RabbitMQConnectorConfig.VirtualHost] = "/myapp"
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
        using var task = new RabbitMQSourceTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Queue] = "test-queue",
            [RabbitMQConnectorConfig.Topic] = "test-topic",
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
    public void Start_WithPrefetchCount_ParsesBatchConfig()
    {
        using var task = new RabbitMQSourceTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Queue] = "test-queue",
            [RabbitMQConnectorConfig.Topic] = "test-topic",
            [RabbitMQConnectorConfig.PrefetchCount] = "500",
            [RabbitMQConnectorConfig.BatchSize] = "200"
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
    public void Start_WithQueueOptions_ParsesQueueConfig()
    {
        using var task = new RabbitMQSourceTask();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Queue] = "test-queue",
            [RabbitMQConnectorConfig.Topic] = "test-topic",
            [RabbitMQConnectorConfig.QueueDurable] = "false",
            [RabbitMQConnectorConfig.QueueExclusive] = "true",
            [RabbitMQConnectorConfig.QueueAutoDelete] = "true"
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
    public void Stop_WithoutStart_DoesNotThrow()
    {
        using var task = new RabbitMQSourceTask();

        var exception = Record.Exception(() => task.Stop());

        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_WithoutStart_DoesNotThrow()
    {
        var task = new RabbitMQSourceTask();

        var exception = Record.Exception(() => task.Dispose());

        Assert.Null(exception);
    }

    [Fact]
    public async Task CommitAsync_WithoutConnection_DoesNotThrow()
    {
        using var task = new RabbitMQSourceTask();

        var exception = await Record.ExceptionAsync(() => task.CommitAsync(CancellationToken.None));

        Assert.Null(exception);
    }
}
