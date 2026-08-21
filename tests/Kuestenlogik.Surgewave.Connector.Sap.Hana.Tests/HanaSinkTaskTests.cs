using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sap.Hana.Tests;

public class HanaSinkTaskTests
{
    [Fact]
    public void Start_RejectsAWriteModeTheSinkCannotPerform()
    {
        using var task = new HanaSinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var config = SinkConfig();
        config[HanaConnectorConfig.WriteMode] = "update";

        // "update" used to fall through to a plain INSERT, so an update-configured sink
        // quietly duplicated rows instead of changing them.
        var ex = Assert.Throws<ArgumentException>(() => task.Start(config));

        Assert.Contains("insert, upsert, merge", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("insert")]
    [InlineData("upsert")]
    [InlineData("MERGE")]
    public void Start_AcceptsEverySupportedWriteMode(string writeMode)
    {
        using var task = new HanaSinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var config = SinkConfig();
        config[HanaConnectorConfig.WriteMode] = writeMode;

        // Getting as far as the driver check means the mode passed validation.
        Assert.Throws<NotSupportedException>(() => task.Start(config));
    }

    [Fact]
    public void Start_WithoutTheDriver_NamesThePackageAndTheDefine()
    {
        using var task = new HanaSinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var ex = Assert.Throws<NotSupportedException>(() => task.Start(SinkConfig()));

        Assert.Contains("Sap.Data.Hana.Core.v2.1", ex.Message, StringComparison.Ordinal);
        Assert.Contains("SAP_HANA_AVAILABLE", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_WhenTheBatchCannotBeWritten_ThrowsTheErrorItSurfaced()
    {
        var errors = new List<Exception>();
        using var task = StartWithoutDriver(SinkConfig(), errors);

        var thrown = await Assert.ThrowsAnyAsync<Exception>(
            () => task.PutAsync([Record("""{"ID":1,"NAME":"ACME"}""")], CancellationToken.None));

        // A swallowed batch would let the worker commit consumer offsets for rows that
        // never reached HANA, so the failure has to leave PutAsync.
        Assert.Same(Assert.Single(errors), thrown);
    }

    [Fact]
    public async Task PutAsync_WithValuesThatAreNotRows_SurfacesThemAndWritesNothing()
    {
        var errors = new List<Exception>();
        using var task = StartWithoutDriver(SinkConfig(), errors);

        // Neither value can ever become a row, so retrying is pointless: they are
        // reported and skipped, and nothing is left to write (hence no failure).
        await task.PutAsync(
            [Record("this is not json"), Record("[1, 2, 3]")],
            CancellationToken.None);

        Assert.Equal(2, errors.Count);
    }

    [Fact]
    public void BuildMergeCommand_MatchesOnTheKeysAndUpdatesTheOtherColumns()
    {
        var config = SinkConfig();
        config[HanaConnectorConfig.WriteMode] = "upsert";
        config[HanaConnectorConfig.KeyColumns] = "ID";

        using var task = StartWithoutDriver(config, []);

        var sql = task.BuildMergeCommand(
            "\"ORDERS\"",
            ["ID", "NAME", "AMOUNT"],
            "\"ID\", \"NAME\", \"AMOUNT\"",
            ":p0, :p1, :p2");

        Assert.Contains("""MERGE INTO "ORDERS" AS t""", sql, StringComparison.Ordinal);
        Assert.Contains("""ON t."ID" = :p0""", sql, StringComparison.Ordinal);
        Assert.Contains("""UPDATE SET t."NAME" = :p1, t."AMOUNT" = :p2""", sql, StringComparison.Ordinal);

        // Listing the key column in the SET clause would make the merge update the very
        // column it matched on.
        Assert.DoesNotContain("SET t.\"ID\"", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildMergeCommand_WithoutKeyColumns_FallsBackToAnInsert()
    {
        var config = SinkConfig();
        config[HanaConnectorConfig.WriteMode] = "merge";

        using var task = StartWithoutDriver(config, []);

        var sql = task.BuildMergeCommand(
            "\"ORDERS\"",
            ["ID", "NAME"],
            "\"ID\", \"NAME\"",
            ":p0, :p1");

        // Without keys there is nothing to match on, so no merge can be built.
        Assert.Equal("""INSERT INTO "ORDERS" ("ID", "NAME") VALUES (:p0, :p1)""", sql);
    }

    private static HanaSinkTask StartWithoutDriver(IDictionary<string, string> config, List<Exception> errors)
    {
        var task = new HanaSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });

        try
        {
            task.Start(config);
        }
        catch (NotSupportedException)
        {
            // This build ships without the SAP driver. Start parses the whole
            // configuration before it reports the missing driver, so write mode, key
            // columns and batch size are in place here.
        }

        return task;
    }

    private static SinkRecord Record(string value)
    {
        return new SinkRecord
        {
            Topic = "orders",
            Partition = 0,
            Offset = 0,
            Value = Encoding.UTF8.GetBytes(value),
            Timestamp = DateTimeOffset.UnixEpoch
        };
    }

    private static Dictionary<string, string> SinkConfig()
    {
        return new Dictionary<string, string>
        {
            [HanaConnectorConfig.Topics] = "orders",
            [HanaConnectorConfig.TargetTable] = "ORDERS",
            [HanaConnectorConfig.Host] = "hana.invalid",
            [HanaConnectorConfig.Username] = "SAPUSER",
            [HanaConnectorConfig.Password] = "secret"
        };
    }
}
