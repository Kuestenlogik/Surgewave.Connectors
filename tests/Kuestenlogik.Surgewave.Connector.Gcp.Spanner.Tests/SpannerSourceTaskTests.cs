using Google.Cloud.Spanner.Data;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Spanner.Tests;

/// <summary>
/// Exercises the read side without a Spanner connection: the SQL a poll issues and the
/// timestamp bound it reads under.
/// </summary>
public class SpannerSourceTaskTests
{
    [Fact]
    public void BuildQuery_FromATable_SelectsEverythingUpToTheRowLimit()
    {
        using var task = ConfiguredTask(c =>
        {
            c[SpannerConnectorConfig.Table] = "Orders";
            c[SpannerConnectorConfig.RowLimit] = "250";
        });

        Assert.Equal("SELECT * FROM Orders LIMIT 250", task.BuildQuery(null));
    }

    [Fact]
    public void BuildQuery_FromATableWithColumns_ProjectsOnlyThose()
    {
        using var task = ConfiguredTask(c =>
        {
            c[SpannerConnectorConfig.Table] = "Orders";
            c[SpannerConnectorConfig.Columns] = "OrderId, Total";
            c[SpannerConnectorConfig.RowLimit] = "10";
        });

        Assert.Equal("SELECT OrderId, Total FROM Orders LIMIT 10", task.BuildQuery(null));
    }

    [Fact]
    public void BuildQuery_WithAnIncrementalColumnAndNoProgressYet_OrdersByItWithoutFiltering()
    {
        using var task = ConfiguredTask(c =>
        {
            c[SpannerConnectorConfig.Table] = "Orders";
            c[SpannerConnectorConfig.IncrementalColumn] = "UpdatedAt";
            c[SpannerConnectorConfig.RowLimit] = "10";
        });

        Assert.Equal("SELECT * FROM Orders ORDER BY UpdatedAt LIMIT 10", task.BuildQuery(null));
    }

    [Fact]
    public void BuildQuery_WithAnIncrementalColumnAfterProgress_FiltersOnTheLastValue()
    {
        using var task = ConfiguredTask(c =>
        {
            c[SpannerConnectorConfig.Table] = "Orders";
            c[SpannerConnectorConfig.IncrementalColumn] = "UpdatedAt";
            c[SpannerConnectorConfig.RowLimit] = "10";
        });

        Assert.Equal(
            "SELECT * FROM Orders WHERE UpdatedAt > @lastValue ORDER BY UpdatedAt LIMIT 10",
            task.BuildQuery(42L));
    }

    [Fact]
    public void BuildQuery_CustomQueryWithItsOwnWhere_AppendsAndInsteadOfASecondWhere()
    {
        using var task = ConfiguredTask(c =>
        {
            c[SpannerConnectorConfig.Query] = "SELECT * FROM Orders WHERE Status = 'OPEN'";
            c[SpannerConnectorConfig.IncrementalColumn] = "UpdatedAt";
            c[SpannerConnectorConfig.RowLimit] = "10";
        });

        Assert.Equal(
            "SELECT * FROM Orders WHERE Status = 'OPEN' AND UpdatedAt > @lastValue LIMIT 10",
            task.BuildQuery(42L));
    }

    [Fact]
    public void BuildQuery_CustomQueryThatAlreadyLimits_IsNotGivenASecondLimit()
    {
        using var task = ConfiguredTask(c =>
        {
            c[SpannerConnectorConfig.Query] = "SELECT * FROM Orders LIMIT 5";
            c[SpannerConnectorConfig.RowLimit] = "1000";
        });

        Assert.Equal("SELECT * FROM Orders LIMIT 5", task.BuildQuery(null));
    }

    [Fact]
    public void GetTimestampBound_ByDefault_ReadsStrong()
    {
        using var task = ConfiguredTask(c => c[SpannerConnectorConfig.Table] = "Orders");

        Assert.Equal(TimestampBoundMode.Strong, task.GetTimestampBound().Mode);
    }

    [Fact]
    public void GetTimestampBound_BoundedStaleness_UsesTheConfiguredMaxStaleness()
    {
        // spanner.timestamp.bound and spanner.max.staleness.seconds were parsed and then
        // dropped on the floor - the computed bound was never handed to the transaction.
        using var task = ConfiguredTask(c =>
        {
            c[SpannerConnectorConfig.Table] = "Orders";
            c[SpannerConnectorConfig.TimestampBound] = "bounded_staleness";
            c[SpannerConnectorConfig.MaxStalenessSeconds] = "30";
        });

        var bound = task.GetTimestampBound();

        Assert.Equal(TimestampBoundMode.MaxStaleness, bound.Mode);
        Assert.Equal(TimeSpan.FromSeconds(30), bound.Staleness);
    }

    [Fact]
    public void GetTimestampBound_ExactStaleness_UsesTheConfiguredMaxStaleness()
    {
        using var task = ConfiguredTask(c =>
        {
            c[SpannerConnectorConfig.Table] = "Orders";
            c[SpannerConnectorConfig.TimestampBound] = "EXACT";
            c[SpannerConnectorConfig.MaxStalenessSeconds] = "15";
        });

        var bound = task.GetTimestampBound();

        Assert.Equal(TimestampBoundMode.ExactStaleness, bound.Mode);
        Assert.Equal(TimeSpan.FromSeconds(15), bound.Staleness);
    }

    [Fact]
    public void GetTimestampBound_WithAnUnknownMode_FallsBackToStrong()
    {
        using var task = ConfiguredTask(c =>
        {
            c[SpannerConnectorConfig.Table] = "Orders";
            c[SpannerConnectorConfig.TimestampBound] = "whenever";
        });

        Assert.Equal(TimestampBoundMode.Strong, task.GetTimestampBound().Mode);
    }

    [Fact]
    public async Task PollAsync_WhenTheReadFails_SurfacesTheErrorInsteadOfSpinningSilently()
    {
        // The empty catch made a permanently failing source look like an idle one.
        var errors = new List<Exception>();
        using var task = new SpannerSourceTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfig(SourceConfig());

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Empty(records);
        Assert.Single(errors);
    }

    private static SpannerSourceTask ConfiguredTask(Action<Dictionary<string, string>> configure)
    {
        var config = SourceConfig();
        configure(config);

        var task = new SpannerSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.ApplyConfig(config);
        return task;
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [SpannerConnectorConfig.ProjectId] = "demo-project",
        [SpannerConnectorConfig.InstanceId] = "demo-instance",
        [SpannerConnectorConfig.DatabaseId] = "demo-database",
        [SpannerConnectorConfig.Topic] = "spanner-rows",
        [SpannerConnectorConfig.Table] = "Orders",
        [SpannerConnectorConfig.PollIntervalMs] = "0"
    };
}
