using Kuestenlogik.Surgewave.Connector.Nats;

namespace Kuestenlogik.Surgewave.Connector.Nats.Tests;

public class PendingAckQueueTests
{
    private sealed class FakePendingAck : IPendingAck
    {
        public int AckCount { get; private set; }

        public int NakCount { get; private set; }

        public bool ThrowOnNak { get; init; }

        public ValueTask AckAsync(CancellationToken cancellationToken = default)
        {
            AckCount++;
            return ValueTask.CompletedTask;
        }

        public ValueTask NakAsync(CancellationToken cancellationToken = default)
        {
            NakCount++;
            return ThrowOnNak
                ? ValueTask.FromException(new InvalidOperationException("nak failed"))
                : ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task AckAllAsync_AcknowledgesAndDrainsEveryPendingMessage()
    {
        var queue = new PendingAckQueue();
        var first = new FakePendingAck();
        var second = new FakePendingAck();
        queue.Enqueue(first);
        queue.Enqueue(second);

        await queue.AckAllAsync(CancellationToken.None);

        Assert.Equal(1, first.AckCount);
        Assert.Equal(1, second.AckCount);
        Assert.Equal(0, queue.Count);
    }

    [Fact]
    public async Task NakAllAsync_NaksUncommittedMessagesInsteadOfAckingThem()
    {
        var queue = new PendingAckQueue();
        var pending = new FakePendingAck();
        queue.Enqueue(pending);

        await queue.NakAllAsync(CancellationToken.None);

        Assert.Equal(0, pending.AckCount);
        Assert.Equal(1, pending.NakCount);
        Assert.Equal(0, queue.Count);
    }

    [Fact]
    public async Task NakAllAsync_WhenNakFails_StillDrainsTheQueue()
    {
        var queue = new PendingAckQueue();
        queue.Enqueue(new FakePendingAck { ThrowOnNak = true });
        queue.Enqueue(new FakePendingAck { ThrowOnNak = true });

        await queue.NakAllAsync(CancellationToken.None);

        Assert.Equal(0, queue.Count);
    }
}
