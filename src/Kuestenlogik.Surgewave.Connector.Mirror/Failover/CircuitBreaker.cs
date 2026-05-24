namespace Kuestenlogik.Surgewave.Connector.Mirror.Failover;

/// <summary>
/// Circuit breaker for handling network partitions and failures.
/// Implements the circuit breaker pattern to prevent cascading failures.
/// </summary>
public sealed class CircuitBreaker
{
    private readonly int _failureThreshold;
    private readonly TimeSpan _openDuration;
    private readonly object _lock = new();

    private CircuitState _state = CircuitState.Closed;
    private int _failureCount;
    private DateTime _lastFailureTime;
    private DateTime _openedAt;

    public CircuitBreaker(int failureThreshold = 5, TimeSpan? openDuration = null)
    {
        _failureThreshold = failureThreshold;
        _openDuration = openDuration ?? TimeSpan.FromSeconds(30);
    }

    /// <summary>
    /// Current state of the circuit breaker.
    /// </summary>
    public CircuitState State
    {
        get
        {
            lock (_lock)
            {
                if (_state == CircuitState.Open && DateTime.UtcNow - _openedAt > _openDuration)
                {
                    _state = CircuitState.HalfOpen;
                }
                return _state;
            }
        }
    }

    /// <summary>
    /// Number of consecutive failures.
    /// </summary>
    public int FailureCount
    {
        get
        {
            lock (_lock) return _failureCount;
        }
    }

    /// <summary>
    /// Check if the circuit allows the operation.
    /// </summary>
    public bool AllowRequest()
    {
        var state = State;
        return state == CircuitState.Closed || state == CircuitState.HalfOpen;
    }

    /// <summary>
    /// Execute an operation with circuit breaker protection.
    /// </summary>
    public async Task<T> ExecuteAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken = default)
    {
        if (!AllowRequest())
        {
            throw new CircuitBreakerOpenException($"Circuit breaker is open. Will retry after {_openDuration}");
        }

        try
        {
            var result = await operation();
            RecordSuccess();
            return result;
        }
        catch (Exception)
        {
            RecordFailure();
            throw;
        }
    }

    /// <summary>
    /// Execute an operation with circuit breaker protection.
    /// </summary>
    public async Task ExecuteAsync(Func<Task> operation, CancellationToken cancellationToken = default)
    {
        if (!AllowRequest())
        {
            throw new CircuitBreakerOpenException($"Circuit breaker is open. Will retry after {_openDuration}");
        }

        try
        {
            await operation();
            RecordSuccess();
        }
        catch (Exception)
        {
            RecordFailure();
            throw;
        }
    }

    /// <summary>
    /// Record a successful operation.
    /// </summary>
    public void RecordSuccess()
    {
        lock (_lock)
        {
            _failureCount = 0;
            _state = CircuitState.Closed;
        }
    }

    /// <summary>
    /// Record a failed operation.
    /// </summary>
    public void RecordFailure()
    {
        lock (_lock)
        {
            _failureCount++;
            _lastFailureTime = DateTime.UtcNow;

            if (_failureCount >= _failureThreshold)
            {
                _state = CircuitState.Open;
                _openedAt = DateTime.UtcNow;
            }
        }
    }

    /// <summary>
    /// Reset the circuit breaker to closed state.
    /// </summary>
    public void Reset()
    {
        lock (_lock)
        {
            _failureCount = 0;
            _state = CircuitState.Closed;
        }
    }

    /// <summary>
    /// Force the circuit breaker to open state.
    /// </summary>
    public void Trip()
    {
        lock (_lock)
        {
            _state = CircuitState.Open;
            _openedAt = DateTime.UtcNow;
        }
    }
}

/// <summary>
/// State of the circuit breaker.
/// </summary>
public enum CircuitState
{
    /// <summary>
    /// Circuit is closed - all operations allowed.
    /// </summary>
    Closed,

    /// <summary>
    /// Circuit is open - operations are blocked.
    /// </summary>
    Open,

    /// <summary>
    /// Circuit is half-open - allowing test operations.
    /// </summary>
    HalfOpen
}

/// <summary>
/// Exception thrown when the circuit breaker is open.
/// </summary>
public sealed class CircuitBreakerOpenException : Exception
{
    public CircuitBreakerOpenException()
    {
    }

    public CircuitBreakerOpenException(string message) : base(message)
    {
    }

    public CircuitBreakerOpenException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
