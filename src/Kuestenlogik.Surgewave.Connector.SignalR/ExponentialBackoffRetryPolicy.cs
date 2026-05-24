using Microsoft.AspNetCore.SignalR.Client;

namespace Kuestenlogik.Surgewave.Connector.SignalR;

/// <summary>
/// Exponential backoff retry policy for SignalR connections.
/// </summary>
internal sealed class ExponentialBackoffRetryPolicy : IRetryPolicy
{
    private readonly TimeSpan _initialDelay;
    private readonly TimeSpan _maxDelay;

    public ExponentialBackoffRetryPolicy(TimeSpan initialDelay, TimeSpan maxDelay)
    {
        _initialDelay = initialDelay;
        _maxDelay = maxDelay;
    }

    public TimeSpan? NextRetryDelay(RetryContext retryContext)
    {
        // Calculate exponential backoff: initialDelay * 2^retryCount
        var delay = TimeSpan.FromMilliseconds(
            _initialDelay.TotalMilliseconds * Math.Pow(2, retryContext.PreviousRetryCount));

        // Cap at max delay
        if (delay > _maxDelay)
        {
            delay = _maxDelay;
        }

        return delay;
    }
}
