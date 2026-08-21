using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Neptune.Tests;

/// <summary>
/// Tests for <see cref="NeptuneSourceTask"/> that need no Gremlin server: the start-up
/// validation and the way a failing query is reported.
/// </summary>
public class NeptuneSourceTaskTests
{
    [Fact]
    public void Start_RejectsAConfigurationWithoutAnEndpoint()
    {
        using var task = new NeptuneSourceTask();

        Assert.Throws<KeyNotFoundException>(() => task.Start(new Dictionary<string, string>
        {
            [NeptuneConnectorConfig.Topic] = "graph",
            [NeptuneConnectorConfig.Query] = "g.V()"
        }));
    }

    [Fact]
    public void Start_RejectsAConfigurationWithoutAQuery()
    {
        using var task = new NeptuneSourceTask();

        Assert.Throws<KeyNotFoundException>(() => task.Start(new Dictionary<string, string>
        {
            [NeptuneConnectorConfig.Endpoint] = "neptune.example.com",
            [NeptuneConnectorConfig.Topic] = "graph"
        }));
    }

    [Fact]
    public async Task PollAsync_SurfacesAQueryFailureInsteadOfReportingAnIdleSource()
    {
        // A permanently failing endpoint used to be indistinguishable from an empty graph.
        var errors = new List<Exception>();
        using var task = new NeptuneSourceTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });

        var thrown = await Assert.ThrowsAnyAsync<Exception>(() => task.PollAsync(CancellationToken.None));

        // The very failure that aborted the poll is the one the task reports.
        Assert.Same(thrown, Assert.Single(errors));
    }
}
