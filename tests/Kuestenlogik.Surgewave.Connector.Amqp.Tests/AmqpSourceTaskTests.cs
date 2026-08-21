using System.Text;
using Kuestenlogik.Surgewave.Connect;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Kuestenlogik.Surgewave.Connector.Amqp.Tests;

/// <summary>
/// Tests for <see cref="AmqpSourceTask"/>. Deliveries are handed to the consumer callback
/// directly, so the buffering, record mapping and ack bookkeeping run without a broker.
/// </summary>
public class AmqpSourceTaskTests
{
    private static Dictionary<string, string> Config(string autoAck = "false") =>
        new()
        {
            [AmqpConnectorConfig.Topic] = "amqp-events",
            [AmqpConnectorConfig.SourceQueue] = "orders",
            [AmqpConnectorConfig.AutoAck] = autoAck
        };

    private static BasicDeliverEventArgs Delivery(
        string routingKey = "orders.created",
        ulong deliveryTag = 3,
        bool redelivered = false,
        string exchange = "events",
        BasicProperties? properties = null,
        string body = "payload")
        => new(
            "consumer-1",
            deliveryTag,
            redelivered,
            exchange,
            routingKey,
            properties ?? new BasicProperties(),
            Encoding.UTF8.GetBytes(body),
            CancellationToken.None);

    [Fact]
    public void CreateRecord_MapsADeliveryOntoTheConfiguredTopic()
    {
        using var task = new AmqpSourceTask();
        task.ReadConfig(Config());

        var record = task.CreateRecord(Delivery(redelivered: true), Encoding.UTF8.GetBytes("payload"));

        Assert.Equal("amqp-events", record.Topic);
        Assert.Equal("orders.created", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("payload", Encoding.UTF8.GetString(record.Value));
        Assert.Equal("orders", record.SourcePartition["queue"]);
        Assert.Equal(1L, record.SourceOffset["message_id"]);
        Assert.Equal(3UL, record.SourceOffset["delivery_tag"]);
        Assert.Equal("events", Encoding.UTF8.GetString(record.Headers!["amqp.exchange"]));
        Assert.Equal("orders.created", Encoding.UTF8.GetString(record.Headers["amqp.routing_key"]));
        Assert.Equal("3", Encoding.UTF8.GetString(record.Headers["amqp.delivery_tag"]));
        Assert.Equal("True", Encoding.UTF8.GetString(record.Headers["amqp.redelivered"]));
    }

    [Fact]
    public void CreateRecord_CopiesTheAmqpPropertiesAndHeaders()
    {
        using var task = new AmqpSourceTask();
        task.ReadConfig(Config());

        var properties = new BasicProperties
        {
            ContentType = "application/json",
            CorrelationId = "corr-9",
            MessageId = "msg-9",
            Headers = new Dictionary<string, object?>
            {
                ["trace"] = Encoding.UTF8.GetBytes("t-1"),
                ["retries"] = 4
            }
        };

        var record = task.CreateRecord(Delivery(properties: properties), Encoding.UTF8.GetBytes("payload"));

        Assert.Equal("application/json", Encoding.UTF8.GetString(record.Headers!["amqp.content_type"]));
        Assert.Equal("corr-9", Encoding.UTF8.GetString(record.Headers["amqp.correlation_id"]));
        Assert.Equal("msg-9", Encoding.UTF8.GetString(record.Headers["amqp.message_id"]));
        Assert.Equal("t-1", Encoding.UTF8.GetString(record.Headers["amqp.header.trace"]));
        Assert.Equal("4", Encoding.UTF8.GetString(record.Headers["amqp.header.retries"]));
    }

    [Fact]
    public void CreateRecord_FallsBackToTheMessageIdWhenThereIsNoRoutingKey()
    {
        using var task = new AmqpSourceTask();
        task.ReadConfig(Config());

        var record = task.CreateRecord(
            Delivery(routingKey: "", properties: new BasicProperties { MessageId = "msg-7" }),
            Encoding.UTF8.GetBytes("payload"));

        Assert.Equal("msg-7", Encoding.UTF8.GetString(record.Key!));
    }

    [Fact]
    public void CreateRecord_LeavesTheKeyUnsetWhenTheDeliveryCarriesNoIdentity()
    {
        using var task = new AmqpSourceTask();
        task.ReadConfig(Config());

        var record = task.CreateRecord(Delivery(routingKey: ""), Encoding.UTF8.GetBytes("payload"));

        Assert.Null(record.Key);
    }

    [Fact]
    public async Task PollAsync_ReturnsEveryBufferedDeliveryExactlyOnce()
    {
        using var task = new AmqpSourceTask();
        task.ReadConfig(Config());

        await task.OnMessageReceivedAsync(this, Delivery(deliveryTag: 1, body: "one"));
        await task.OnMessageReceivedAsync(this, Delivery(deliveryTag: 2, body: "two"));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(2, records.Count);
        Assert.Equal("one", Encoding.UTF8.GetString(records[0].Value));
        Assert.Equal("two", Encoding.UTF8.GetString(records[1].Value));
        Assert.Empty(await task.PollAsync(CancellationToken.None));
    }

    [Fact]
    public async Task PollAsync_ReturnsNothingWhenNoDeliveryArrived()
    {
        using var task = new AmqpSourceTask();
        task.ReadConfig(Config());

        Assert.Empty(await task.PollAsync(CancellationToken.None));
    }

    [Fact]
    public async Task CommitAsync_DropsTheDeliveryTagsWhenTheAckFails()
    {
        // Delivery tags are channel-scoped: keeping them after a failed ack would let a later
        // cumulative ack on a recreated channel (whose tags restart at 1) ack the wrong messages.
        var errors = new List<Exception>();
        using var task = new AmqpSourceTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ReadConfig(Config());

        await task.OnMessageReceivedAsync(this, Delivery());
        await task.PollAsync(CancellationToken.None);

        await task.CommitAsync(CancellationToken.None);
        await task.CommitAsync(CancellationToken.None);

        // The failure is reported once; the second commit finds nothing left to ack.
        Assert.Single(errors);
    }

    [Fact]
    public async Task CommitAsync_AcksNothingWhenTheConsumerAcknowledgesAutomatically()
    {
        var errors = new List<Exception>();
        using var task = new AmqpSourceTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ReadConfig(Config(autoAck: "true"));

        await task.OnMessageReceivedAsync(this, Delivery());
        await task.PollAsync(CancellationToken.None);
        await task.CommitAsync(CancellationToken.None);

        Assert.Empty(errors);
    }
}
