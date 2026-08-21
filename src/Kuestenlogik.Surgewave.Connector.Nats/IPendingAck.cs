namespace Kuestenlogik.Surgewave.Connector.Nats;

/// <summary>
/// A fetched JetStream message whose acknowledgement is still outstanding.
/// </summary>
public interface IPendingAck
{
    /// <summary>
    /// Acknowledge the message - it will not be redelivered.
    /// </summary>
    ValueTask AckAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Negatively acknowledge the message so JetStream redelivers it immediately.
    /// </summary>
    ValueTask NakAsync(CancellationToken cancellationToken = default);
}
