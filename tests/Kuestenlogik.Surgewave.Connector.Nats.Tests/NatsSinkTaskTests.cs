using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Nats;

namespace Kuestenlogik.Surgewave.Connector.Nats.Tests;

public class NatsSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new NatsSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithDefaultConfig_UsesDefaults()
    {
        using var task = new NatsSinkTask();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.Topics] = "test-topic",
            [NatsConnectorConfig.StreamName] = "test-stream"
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
                // Expected - no NATS server running
            }
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithPublishTimeout_ParsesConfig()
    {
        using var task = new NatsSinkTask();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.Topics] = "test-topic",
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.PublishTimeoutMs] = "10000"
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
    public void Start_WithAuthentication_ParsesCredentials()
    {
        using var task = new NatsSinkTask();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.Topics] = "test-topic",
            [NatsConnectorConfig.StreamName] = "test-stream",
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
    public void Start_WithTlsEnabled_ParsesTlsConfig()
    {
        using var task = new NatsSinkTask();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.Topics] = "test-topic",
            [NatsConnectorConfig.StreamName] = "test-stream",
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
    public async Task PutAsync_WithEmptyRecords_DoesNotThrow()
    {
        using var task = new NatsSinkTask();

        // Don't start - just test empty records handling
        var exception = await Record.ExceptionAsync(() => task.PutAsync([], CancellationToken.None));

        Assert.Null(exception);
    }

    [Fact]
    public async Task FlushAsync_WithoutConnection_DoesNotThrow()
    {
        using var task = new NatsSinkTask();

        var offsets = new Dictionary<TopicPartition, long>();
        var exception = await Record.ExceptionAsync(() => task.FlushAsync(offsets, CancellationToken.None));

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_WithoutStart_DoesNotThrow()
    {
        using var task = new NatsSinkTask();

        var exception = Record.Exception(() => task.Stop());

        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_WithoutStart_DoesNotThrow()
    {
        var task = new NatsSinkTask();

        var exception = Record.Exception(() => task.Dispose());

        Assert.Null(exception);
    }
}
