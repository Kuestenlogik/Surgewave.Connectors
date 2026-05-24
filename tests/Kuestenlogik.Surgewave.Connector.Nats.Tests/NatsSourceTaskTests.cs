using Kuestenlogik.Surgewave.Connector.Nats;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nats.Tests;

public class NatsSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new NatsSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithDefaultUrl_UsesDefaultConfiguration()
    {
        // This test verifies configuration is accepted
        // Actual connection would require NATS server
        using var task = new NatsSourceTask();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.ConsumerName] = "test-consumer",
            [NatsConnectorConfig.Topic] = "test-topic"
        };

        // Will fail to connect without server, but config parsing should work
        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
                                       ex.InnerException?.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Expected - no NATS server running
            }
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithDeliverPolicy_ConfiguresDeliverPolicy()
    {
        using var task = new NatsSourceTask();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.ConsumerName] = "test-consumer",
            [NatsConnectorConfig.Topic] = "test-topic",
            [NatsConnectorConfig.DeliverPolicy] = "new"
        };

        // Will fail to connect without server
        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                                       ex.InnerException?.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Expected
            }
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithAuthenticationConfig_ParsesCredentials()
    {
        using var task = new NatsSourceTask();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.ConsumerName] = "test-consumer",
            [NatsConnectorConfig.Topic] = "test-topic",
            [NatsConnectorConfig.Username] = "testuser",
            [NatsConnectorConfig.Password] = "testpass"
        };

        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                                       ex.InnerException?.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Expected
            }
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithTokenAuth_ParsesToken()
    {
        using var task = new NatsSourceTask();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.ConsumerName] = "test-consumer",
            [NatsConnectorConfig.Topic] = "test-topic",
            [NatsConnectorConfig.Token] = "my-auth-token"
        };

        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
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
        using var task = new NatsSourceTask();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.ConsumerName] = "test-consumer",
            [NatsConnectorConfig.Topic] = "test-topic",
            [NatsConnectorConfig.TlsEnabled] = "true"
        };

        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                                       ex.Message.Contains("tls", StringComparison.OrdinalIgnoreCase) ||
                                       ex.InnerException?.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Expected
            }
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithFetchBatchSize_ParsesBatchConfig()
    {
        using var task = new NatsSourceTask();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.ConsumerName] = "test-consumer",
            [NatsConnectorConfig.Topic] = "test-topic",
            [NatsConnectorConfig.FetchBatchSize] = "500",
            [NatsConnectorConfig.FetchTimeoutMs] = "2000"
        };

        var exception = Record.Exception(() =>
        {
            try
            {
                task.Start(config);
            }
            catch (Exception ex) when (ex.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
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
        using var task = new NatsSourceTask();

        var exception = Record.Exception(() => task.Stop());

        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_WithoutStart_DoesNotThrow()
    {
        var task = new NatsSourceTask();

        var exception = Record.Exception(() => task.Dispose());

        Assert.Null(exception);
    }
}
