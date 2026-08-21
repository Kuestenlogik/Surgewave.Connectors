using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.WhatsApp.Tests;

/// <summary>
/// Feeds webhook bodies straight into the task's buffer - no listener port, no network - and
/// checks what the next poll makes of them, including that an unparseable body fails loudly
/// instead of being acknowledged to Meta.
/// </summary>
public class WhatsAppSourceTaskTests
{
    private const long MessageEpochSeconds = 1755600000L;

    [Fact]
    public async Task PollAsync_AfterAWebhookPost_ProducesARecordCarryingTheMessageAndItsOrigin()
    {
        using var task = new WhatsAppSourceTask();
        task.ApplyConfig(SourceConfig());

        await task.EnqueueWebhookBodyAsync(
            Payload(InboundMessage("wamid.1", "4915100000", "hello")),
            TestContext.Current.CancellationToken);

        var record = Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));

        Assert.Equal("inbound", record.Topic);
        Assert.Equal("wamid.1", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("BIZ-1", record.SourcePartition["business_id"]);
        Assert.Equal("wamid.1", record.SourceOffset["message_id"]);
        Assert.Equal("4915100000", HeaderValue(record, "whatsapp.from"));
        Assert.Equal("wamid.1", HeaderValue(record, "whatsapp.message.id"));
        Assert.Equal("text", HeaderValue(record, "whatsapp.type"));

        using var document = JsonDocument.Parse(record.Value);
        Assert.Equal("hello", document.RootElement.GetProperty("text").GetString());
        Assert.Equal("4915100000", document.RootElement.GetProperty("from").GetString());
    }

    [Fact]
    public async Task PollAsync_TurnsTheWhatsAppUnixTimestampIntoTheRecordTimestamp()
    {
        using var task = new WhatsAppSourceTask();
        task.ApplyConfig(SourceConfig());

        await task.EnqueueWebhookBodyAsync(
            Payload(InboundMessage("wamid.1", "4915100000", "hello")),
            TestContext.Current.CancellationToken);

        var record = Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));

        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(MessageEpochSeconds), record.Timestamp!.Value);
    }

    [Fact]
    public async Task PollAsync_WithSeveralMessagesInOnePost_GivesEachOneItsOwnOffset()
    {
        using var task = new WhatsAppSourceTask();
        task.ApplyConfig(SourceConfig());

        await task.EnqueueWebhookBodyAsync(
            Payload(
                InboundMessage("wamid.1", "4915100000", "first"),
                InboundMessage("wamid.2", "4915100001", "second")),
            TestContext.Current.CancellationToken);

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, records.Count);
        Assert.Equal("wamid.1", records[0].SourceOffset["message_id"]);
        Assert.Equal(1L, records[0].SourceOffset["offset"]);
        Assert.Equal("wamid.2", records[1].SourceOffset["message_id"]);
        Assert.Equal(2L, records[1].SourceOffset["offset"]);
    }

    [Fact]
    public async Task PollAsync_DrainsTheBufferSoTheSameMessageIsNotProducedTwice()
    {
        using var task = new WhatsAppSourceTask();
        task.ApplyConfig(SourceConfig());

        await task.EnqueueWebhookBodyAsync(
            Payload(InboundMessage("wamid.1", "4915100000", "hello")),
            TestContext.Current.CancellationToken);

        Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));
        Assert.Empty(await PollWithoutIdleWaitAsync(task));
    }

    [Fact]
    public async Task EnqueueWebhookBodyAsync_WithAnUnparseableBody_ThrowsSoTheWebhookIsNotAcked()
    {
        using var task = new WhatsAppSourceTask();
        task.ApplyConfig(SourceConfig());

        // The caller answers 500 on this, which makes Meta redeliver; swallowing it here would
        // acknowledge a message that never reached the buffer.
        await Assert.ThrowsAnyAsync<JsonException>(
            () => task.EnqueueWebhookBodyAsync("this is not json", TestContext.Current.CancellationToken));

        Assert.Empty(await PollWithoutIdleWaitAsync(task));
    }

    [Fact]
    public async Task EnqueueWebhookBodyAsync_WithADeliveryStatusOnly_BuffersNothingToProduce()
    {
        using var task = new WhatsAppSourceTask();
        task.ApplyConfig(SourceConfig());

        await task.EnqueueWebhookBodyAsync(StatusOnlyPayload(), TestContext.Current.CancellationToken);

        Assert.Empty(await PollWithoutIdleWaitAsync(task));
    }

    [Fact]
    public async Task PollAsync_WithoutAnyWebhookTraffic_ReturnsEmptyInsteadOfFailing()
    {
        using var task = new WhatsAppSourceTask();
        task.ApplyConfig(SourceConfig());

        Assert.Empty(await PollWithoutIdleWaitAsync(task));
    }

    /// <summary>
    /// Polls without sitting out the task's one-second idle wait: an already-cancelled token ends
    /// that wait at once, which is exactly the path a shutting-down worker takes.
    /// </summary>
    private static async Task<IReadOnlyList<SourceRecord>> PollWithoutIdleWaitAsync(WhatsAppSourceTask task)
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        return await task.PollAsync(cts.Token);
    }

    private static string HeaderValue(SourceRecord record, string name) =>
        Encoding.UTF8.GetString(record.Headers![name]);

    /// <summary>Builds a Cloud API webhook body around the given inbound messages.</summary>
    private static string Payload(params object[] messages) => JsonSerializer.Serialize(new
    {
        entry = new[]
        {
            new
            {
                id = "BIZ-1",
                changes = new[]
                {
                    new
                    {
                        field = "messages",
                        value = new { messaging_product = "whatsapp", messages }
                    }
                }
            }
        }
    });

    /// <summary>A webhook body that only reports delivery status - nothing to produce.</summary>
    private static string StatusOnlyPayload() => JsonSerializer.Serialize(new
    {
        entry = new[]
        {
            new
            {
                id = "BIZ-1",
                changes = new[]
                {
                    new
                    {
                        field = "statuses",
                        value = new { messaging_product = "whatsapp" }
                    }
                }
            }
        }
    });

    private static object InboundMessage(string id, string from, string text) => new
    {
        id,
        from,
        timestamp = MessageEpochSeconds.ToString(CultureInfo.InvariantCulture),
        type = "text",
        text = new { body = text }
    };

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [WhatsAppConnectorConfig.Topic] = "inbound",
        [WhatsAppConnectorConfig.WebhookVerifyToken] = "verify-me"
    };
}
