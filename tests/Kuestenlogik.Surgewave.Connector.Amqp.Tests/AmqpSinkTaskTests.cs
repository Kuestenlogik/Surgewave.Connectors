using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Amqp.Tests;

/// <summary>
/// Tests for <see cref="AmqpSinkTask"/>: the routing-key precedence and the property mapping
/// are verified through the publication builder, without a broker connection.
/// </summary>
public class AmqpSinkTaskTests
{
    private static Dictionary<string, string> Config(string routingKey = "default.key", string persistent = "true") =>
        new()
        {
            [AmqpConnectorConfig.TargetExchange] = "events",
            [AmqpConnectorConfig.TargetRoutingKey] = routingKey,
            [AmqpConnectorConfig.Persistent] = persistent
        };

    private static SinkRecord CreateRecord(
        string value = "payload",
        byte[]? key = null,
        IReadOnlyDictionary<string, byte[]>? headers = null) =>
        new()
        {
            Topic = "commands",
            Partition = 0,
            Offset = 0,
            Key = key,
            Value = Encoding.UTF8.GetBytes(value),
            Headers = headers
        };

    [Fact]
    public void BuildPublication_UsesTheConfiguredRoutingKeyWhenTheRecordCarriesNone()
    {
        using var task = new AmqpSinkTask();
        task.ReadConfig(Config());

        var (routingKey, _) = task.BuildPublication(CreateRecord());

        Assert.Equal("default.key", routingKey);
    }

    [Fact]
    public void BuildPublication_TakesTheRoutingKeyFromTheRecordKey()
    {
        using var task = new AmqpSinkTask();
        task.ReadConfig(Config());

        var (routingKey, _) = task.BuildPublication(CreateRecord(key: Encoding.UTF8.GetBytes("orders.created")));

        Assert.Equal("orders.created", routingKey);
    }

    [Fact]
    public void BuildPublication_PrefersTheRoutingKeyHeaderOverTheRecordKey()
    {
        using var task = new AmqpSinkTask();
        task.ReadConfig(Config());

        var headers = new Dictionary<string, byte[]>
        {
            ["amqp.routing_key"] = Encoding.UTF8.GetBytes("orders.shipped")
        };

        var (routingKey, _) = task.BuildPublication(
            CreateRecord(key: Encoding.UTF8.GetBytes("orders.created"), headers: headers));

        Assert.Equal("orders.shipped", routingKey);
    }

    [Fact]
    public void BuildPublication_RestoresTheAmqpPropertiesFromTheRecordHeaders()
    {
        using var task = new AmqpSinkTask();
        task.ReadConfig(Config());

        var headers = new Dictionary<string, byte[]>
        {
            ["amqp.content_type"] = Encoding.UTF8.GetBytes("application/json"),
            ["amqp.correlation_id"] = Encoding.UTF8.GetBytes("corr-9"),
            ["amqp.message_id"] = Encoding.UTF8.GetBytes("msg-9")
        };

        var (_, properties) = task.BuildPublication(CreateRecord(headers: headers));

        Assert.Equal("application/json", properties.ContentType);
        Assert.Equal("corr-9", properties.CorrelationId);
        Assert.Equal("msg-9", properties.MessageId);
        // Only 'amqp.header.*' entries become AMQP headers.
        Assert.Null(properties.Headers);
    }

    [Fact]
    public void BuildPublication_ForwardsPrefixedHeadersWithTheirPrefixStripped()
    {
        using var task = new AmqpSinkTask();
        task.ReadConfig(Config());

        var headers = new Dictionary<string, byte[]>
        {
            ["amqp.header.trace"] = Encoding.UTF8.GetBytes("t-1"),
            ["surgewave.internal"] = Encoding.UTF8.GetBytes("ignored")
        };

        var (_, properties) = task.BuildPublication(CreateRecord(headers: headers));

        var forwarded = Assert.Single(properties.Headers!);
        Assert.Equal("trace", forwarded.Key);
        Assert.Equal("t-1", Encoding.UTF8.GetString(Assert.IsType<byte[]>(forwarded.Value)));
    }

    [Fact]
    public void BuildPublication_MarksMessagesPersistentByDefault()
    {
        using var task = new AmqpSinkTask();
        task.ReadConfig(Config());

        var (_, properties) = task.BuildPublication(CreateRecord());

        Assert.True(properties.Persistent);
    }

    [Fact]
    public void BuildPublication_HonoursANonPersistentConfiguration()
    {
        using var task = new AmqpSinkTask();
        task.ReadConfig(Config(persistent: "false"));

        var (_, properties) = task.BuildPublication(CreateRecord());

        Assert.False(properties.Persistent);
    }

    [Fact]
    public async Task PutAsync_DoesNotSwallowAPublishFailure()
    {
        // Failed publishes used to be caught per record and acked anyway; the batch must
        // fail instead so the worker retries or dead-letters it.
        using var task = new AmqpSinkTask();
        task.ReadConfig(Config());

        await Assert.ThrowsAsync<NullReferenceException>(
            () => task.PutAsync([CreateRecord()], CancellationToken.None));
    }
}
