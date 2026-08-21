using Kuestenlogik.Surgewave.Connector.Gcp.BigQuery;

namespace Kuestenlogik.Surgewave.Connector.Gcp.BigQuery.Tests;

public class BigQuerySourceTaskTests
{
    [Fact]
    public void BuildQuery_QueryMode_WrapsUserQueryAndAppendsLimit()
    {
        var sql = BigQuerySourceTask.BuildQuery(
            "query", "SELECT * FROM t WHERE id > 5", "p", "d", "", "", null, 100);

        Assert.Equal("SELECT * FROM (SELECT * FROM t WHERE id > 5) LIMIT 100", sql);
    }

    [Fact]
    public void BuildQuery_QueryMode_DoesNotInjectTimestampFilterIntoSubqueries()
    {
        var userQuery = "SELECT * FROM t WHERE id IN (SELECT id FROM u WHERE active)";
        var lastTimestamp = new DateTimeOffset(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

        var sql = BigQuerySourceTask.BuildQuery(
            "query", userQuery, "p", "d", "", "updated_at", lastTimestamp, 100);

        // The user query stays untouched inside the wrapper - filter and limit apply outside
        Assert.Equal(
            $"SELECT * FROM ({userQuery}) WHERE updated_at > TIMESTAMP('2026-08-01 12:00:00.000000') LIMIT 100",
            sql);
    }

    [Fact]
    public void BuildQuery_QueryMode_ProducesSingleOuterLimitWhenUserQueryHasLimit()
    {
        var sql = BigQuerySourceTask.BuildQuery(
            "query", "SELECT * FROM t LIMIT 5;", "p", "d", "", "", null, 100);

        Assert.Equal("SELECT * FROM (SELECT * FROM t LIMIT 5) LIMIT 100", sql);
    }

    [Fact]
    public void BuildQuery_TableMode_FiltersAndOrdersByTimestampColumn()
    {
        var lastTimestamp = new DateTimeOffset(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

        var sql = BigQuerySourceTask.BuildQuery(
            "table", "", "proj", "ds", "tbl", "updated_at", lastTimestamp, 50);

        Assert.Equal(
            "SELECT * FROM `proj.ds.tbl` WHERE updated_at > TIMESTAMP('2026-08-01 12:00:00.000000') ORDER BY updated_at LIMIT 50",
            sql);
    }
}
