using Kuestenlogik.Surgewave.Connector.Gcp.BigQuery;

namespace Kuestenlogik.Surgewave.Connector.Gcp.BigQuery.Tests;

public class BigQuerySinkTaskTests
{
    [Fact]
    public void IsRetriableException_ClassifiesBackendErrorAsRetriable()
    {
        var ex = new InvalidOperationException("The service returned backendError, please retry");
        Assert.True(BigQuerySinkTask.IsRetriableException(ex));
    }

    [Fact]
    public void IsRetriableException_ClassifiesRateLimitAsRetriable()
    {
        var ex = new InvalidOperationException("rateLimitExceeded");
        Assert.True(BigQuerySinkTask.IsRetriableException(ex));
    }

    [Fact]
    public void IsRetriableException_DoesNotRetryPermanentErrors()
    {
        var ex = new InvalidOperationException("invalid field name");
        Assert.False(BigQuerySinkTask.IsRetriableException(ex));
    }
}
