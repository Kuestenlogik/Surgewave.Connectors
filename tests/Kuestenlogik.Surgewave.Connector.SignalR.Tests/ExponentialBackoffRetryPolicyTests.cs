using Microsoft.AspNetCore.SignalR.Client;

namespace Kuestenlogik.Surgewave.Connector.SignalR.Tests;

/// <summary>
/// Tests for <see cref="ExponentialBackoffRetryPolicy"/>, the policy both SignalR tasks hand
/// to the client for automatic reconnects.
/// </summary>
public class ExponentialBackoffRetryPolicyTests
{
    [Theory]
    [InlineData(0, 1000)]
    [InlineData(1, 2000)]
    [InlineData(2, 4000)]
    [InlineData(5, 32000)]
    [InlineData(6, 60000)]
    public void NextRetryDelay_DoublesWithEveryAttemptUpToTheCeiling(int previousRetries, int expectedMs)
    {
        var policy = new ExponentialBackoffRetryPolicy(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1));

        var delay = policy.NextRetryDelay(new RetryContext { PreviousRetryCount = previousRetries });

        Assert.NotNull(delay);
        Assert.Equal(TimeSpan.FromMilliseconds(expectedMs), delay.Value);
    }

    [Fact]
    public void NextRetryDelay_KeepsRetryingAfterALongOutage()
    {
        var policy = new ExponentialBackoffRetryPolicy(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30));

        var delay = policy.NextRetryDelay(new RetryContext { PreviousRetryCount = 30 });

        // A null delay tells the SignalR client to stop reconnecting for good, which would
        // leave the connector dead after an outage it should have ridden out.
        Assert.NotNull(delay);
        Assert.Equal(TimeSpan.FromSeconds(30), delay.Value);
    }
}
