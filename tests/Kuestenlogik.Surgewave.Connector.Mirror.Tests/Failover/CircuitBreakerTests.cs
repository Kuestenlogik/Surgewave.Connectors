using Kuestenlogik.Surgewave.Connector.Mirror.Failover;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Tests.Failover;

public class CircuitBreakerTests
{
    [Fact]
    public void InitialState_ShouldBeClosed()
    {
        var breaker = new CircuitBreaker();
        Assert.Equal(CircuitState.Closed, breaker.State);
    }

    [Fact]
    public void AllowRequest_ShouldReturnTrueWhenClosed()
    {
        var breaker = new CircuitBreaker();
        Assert.True(breaker.AllowRequest());
    }

    [Fact]
    public void RecordSuccess_ShouldResetFailureCount()
    {
        var breaker = new CircuitBreaker(failureThreshold: 5);

        breaker.RecordFailure();
        breaker.RecordFailure();
        Assert.Equal(2, breaker.FailureCount);

        breaker.RecordSuccess();
        Assert.Equal(0, breaker.FailureCount);
    }

    [Fact]
    public void RecordFailure_ShouldIncrementFailureCount()
    {
        var breaker = new CircuitBreaker(failureThreshold: 5);

        breaker.RecordFailure();
        Assert.Equal(1, breaker.FailureCount);

        breaker.RecordFailure();
        Assert.Equal(2, breaker.FailureCount);
    }

    [Fact]
    public void RecordFailure_ShouldOpenCircuitAfterThreshold()
    {
        var breaker = new CircuitBreaker(failureThreshold: 3);

        breaker.RecordFailure();
        breaker.RecordFailure();
        Assert.Equal(CircuitState.Closed, breaker.State);

        breaker.RecordFailure(); // Third failure
        Assert.Equal(CircuitState.Open, breaker.State);
    }

    [Fact]
    public void AllowRequest_ShouldReturnFalseWhenOpen()
    {
        var breaker = new CircuitBreaker(failureThreshold: 1);
        breaker.RecordFailure(); // Opens the circuit

        Assert.False(breaker.AllowRequest());
    }

    [Fact]
    public void State_ShouldTransitionToHalfOpenAfterDuration()
    {
        var breaker = new CircuitBreaker(failureThreshold: 1, openDuration: TimeSpan.FromMilliseconds(50));
        breaker.RecordFailure(); // Opens the circuit

        Assert.Equal(CircuitState.Open, breaker.State);

        Thread.Sleep(100); // Wait for open duration to pass

        Assert.Equal(CircuitState.HalfOpen, breaker.State);
    }

    [Fact]
    public void AllowRequest_ShouldReturnTrueWhenHalfOpen()
    {
        var breaker = new CircuitBreaker(failureThreshold: 1, openDuration: TimeSpan.FromMilliseconds(50));
        breaker.RecordFailure();

        Thread.Sleep(100);

        Assert.True(breaker.AllowRequest());
    }

    [Fact]
    public void RecordSuccess_InHalfOpen_ShouldCloseCircuit()
    {
        var breaker = new CircuitBreaker(failureThreshold: 1, openDuration: TimeSpan.FromMilliseconds(50));
        breaker.RecordFailure();

        Thread.Sleep(100);
        Assert.Equal(CircuitState.HalfOpen, breaker.State);

        breaker.RecordSuccess();
        Assert.Equal(CircuitState.Closed, breaker.State);
    }

    [Fact]
    public void RecordFailure_InHalfOpen_ShouldOpenCircuit()
    {
        var breaker = new CircuitBreaker(failureThreshold: 1, openDuration: TimeSpan.FromMilliseconds(50));
        breaker.RecordFailure(); // Opens

        Thread.Sleep(100);
        Assert.Equal(CircuitState.HalfOpen, breaker.State);

        breaker.RecordFailure(); // Should reopen
        Assert.Equal(CircuitState.Open, breaker.State);
    }

    [Fact]
    public void Reset_ShouldCloseCircuitAndResetCount()
    {
        var breaker = new CircuitBreaker(failureThreshold: 1);
        breaker.RecordFailure();
        Assert.Equal(CircuitState.Open, breaker.State);

        breaker.Reset();

        Assert.Equal(CircuitState.Closed, breaker.State);
        Assert.Equal(0, breaker.FailureCount);
    }

    [Fact]
    public void Trip_ShouldOpenCircuitImmediately()
    {
        var breaker = new CircuitBreaker(failureThreshold: 10);
        Assert.Equal(CircuitState.Closed, breaker.State);

        breaker.Trip();

        Assert.Equal(CircuitState.Open, breaker.State);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldExecuteOperationWhenClosed()
    {
        var breaker = new CircuitBreaker();
        var executed = false;

        await breaker.ExecuteAsync(async () =>
        {
            executed = true;
            await Task.CompletedTask;
        });

        Assert.True(executed);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldThrowWhenOpen()
    {
        var breaker = new CircuitBreaker(failureThreshold: 1);
        breaker.RecordFailure(); // Opens circuit

        await Assert.ThrowsAsync<CircuitBreakerOpenException>(() =>
            breaker.ExecuteAsync(async () =>
            {
                await Task.CompletedTask;
            }));
    }

    [Fact]
    public async Task ExecuteAsync_ShouldRecordSuccessOnSuccess()
    {
        var breaker = new CircuitBreaker(failureThreshold: 5);
        breaker.RecordFailure();
        breaker.RecordFailure();
        Assert.Equal(2, breaker.FailureCount);

        await breaker.ExecuteAsync(async () =>
        {
            await Task.CompletedTask;
        });

        Assert.Equal(0, breaker.FailureCount);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldRecordFailureOnException()
    {
        var breaker = new CircuitBreaker(failureThreshold: 5);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            breaker.ExecuteAsync<int>(async () =>
            {
                await Task.CompletedTask;
                throw new InvalidOperationException("Test failure");
            }));

        Assert.Equal(1, breaker.FailureCount);
    }

    [Fact]
    public async Task ExecuteAsync_WithResult_ShouldReturnResult()
    {
        var breaker = new CircuitBreaker();

        var result = await breaker.ExecuteAsync(async () =>
        {
            await Task.CompletedTask;
            return 42;
        });

        Assert.Equal(42, result);
    }

    [Fact]
    public void DefaultThreshold_ShouldBe5()
    {
        var breaker = new CircuitBreaker();

        // Should still be closed after 4 failures
        for (int i = 0; i < 4; i++)
        {
            breaker.RecordFailure();
        }
        Assert.Equal(CircuitState.Closed, breaker.State);

        // 5th failure should open
        breaker.RecordFailure();
        Assert.Equal(CircuitState.Open, breaker.State);
    }

    [Fact]
    public void DefaultOpenDuration_ShouldBe30Seconds()
    {
        var breaker = new CircuitBreaker(failureThreshold: 1);
        breaker.RecordFailure();

        // Should still be open after a short time
        Thread.Sleep(50);
        Assert.Equal(CircuitState.Open, breaker.State);
    }

    [Fact]
    public async Task ThreadSafety_ShouldHandleConcurrentAccess()
    {
        var breaker = new CircuitBreaker(failureThreshold: 100);
        var tasks = new List<Task>();

        for (int i = 0; i < 50; i++)
        {
            tasks.Add(Task.Run(() => breaker.RecordFailure()));
            tasks.Add(Task.Run(() => breaker.RecordSuccess()));
        }

        await Task.WhenAll(tasks);

        // Should not throw or deadlock
        _ = breaker.State;
        _ = breaker.FailureCount;
    }
}
