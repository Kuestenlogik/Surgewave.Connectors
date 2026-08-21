using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Connector.Nats;

/// <summary>
/// Tracks JetStream messages that were handed to the Connect worker but not committed yet.
/// Messages are acknowledged when the worker commits them; on shutdown the outstanding
/// messages are negatively acknowledged so JetStream redelivers them instead of losing them.
/// </summary>
public sealed class PendingAckQueue
{
    private readonly ConcurrentQueue<IPendingAck> _pending = new();

    /// <summary>
    /// Number of messages still waiting for a commit.
    /// </summary>
    public int Count => _pending.Count;

    /// <summary>
    /// Track a message that was emitted but not committed yet.
    /// </summary>
    public void Enqueue(IPendingAck pending) => _pending.Enqueue(pending);

    /// <summary>
    /// Acknowledge every outstanding message. Called once the records were durably committed.
    /// </summary>
    public async Task AckAllAsync(CancellationToken cancellationToken = default)
    {
        while (_pending.TryDequeue(out var pending))
        {
            await pending.AckAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Negatively acknowledge every outstanding message so JetStream redelivers it.
    /// Used on shutdown, where a failing NAK is harmless: the message simply stays un-acked
    /// and is redelivered once the consumer's ack wait expires.
    /// </summary>
    public async Task NakAllAsync(CancellationToken cancellationToken = default)
    {
        while (_pending.TryDequeue(out var pending))
        {
            try
            {
                await pending.NakAsync(cancellationToken);
            }
            catch (Exception)
            {
                // Best effort: without the NAK redelivery is only delayed until the ack wait expires.
            }
        }
    }
}
