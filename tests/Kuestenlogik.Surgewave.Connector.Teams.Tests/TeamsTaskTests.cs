using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Teams;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Teams.Tests;

/// <summary>
/// Tests for Microsoft Teams source and sink tasks.
/// </summary>
public sealed class TeamsTaskTests
{
    [Fact]
    public void TeamsSourceTask_HasCorrectVersion()
    {
        var task = new TeamsSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void TeamsSinkTask_HasCorrectVersion()
    {
        var task = new TeamsSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task TeamsSinkTask_HandlesEmptyRecords()
    {
        var task = new TeamsSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        // Task needs Graph client which requires real credentials
        // Without Start(), PutAsync should handle empty records gracefully
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await task.PutAsync([], cts.Token);
        // Should not throw
    }

    [Fact]
    public void TeamsSourceTask_DisposesCleanly()
    {
        var task = new TeamsSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        task.Dispose();
        // Should not throw
    }

    [Fact]
    public void TeamsSinkTask_DisposesCleanly()
    {
        var task = new TeamsSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        task.Dispose();
        // Should not throw
    }

    [Fact]
    public void TeamsSourceTask_StopsCleanly()
    {
        var task = new TeamsSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        task.Stop();
        // Should not throw
    }

    [Fact]
    public void TeamsSinkTask_StopsCleanly()
    {
        var task = new TeamsSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        task.Stop();
        // Should not throw
    }
}
