using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sap.Hana.Tests;

public class HanaSourceTaskTests
{
    [Fact]
    public void Start_WithoutTheDriver_NamesThePackageAndTheDefine()
    {
        using var task = new HanaSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        // The SAP driver is not redistributable, so this build cannot talk to HANA at
        // all. The failure has to say exactly what is missing instead of pretending
        // the task started.
        var ex = Assert.Throws<NotSupportedException>(() => task.Start(TableConfig()));

        Assert.Contains("Sap.Data.Hana.Core.v2.1", ex.Message, StringComparison.Ordinal);
        Assert.Contains("SAP_HANA_AVAILABLE", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_RestoresTheStoredCursor_SoARestartDoesNotReEmitTheTable()
    {
        var offsets = new StubOffsetReader(new Dictionary<string, object>
        {
            ["message_id"] = 17L,
            ["incremental_value"] = "2026-01-31T12:00:00"
        });

        using var task = StartWithoutDriver(TableConfig(), offsets);

        // The partition the task asks for must be the one its records carry, otherwise
        // the runtime hands back nothing and the whole table is re-read on every restart.
        var partition = Assert.Single(offsets.RequestedPartitions);
        Assert.Equal("hana", Assert.IsType<string>(partition["source"]));
        Assert.Equal("ORDERS", Assert.IsType<string>(partition["table"]));

        // ... and the restored cursor has to reach the query, not just a field.
        Assert.Equal(
            """SELECT * FROM "SAPABAP1"."ORDERS" WHERE "CHANGED_AT" > :lastValue ORDER BY "CHANGED_AT" LIMIT 500""",
            task.BuildQuery());
    }

    [Fact]
    public void Start_WithoutAStoredOffset_ReadsFromTheBeginning()
    {
        var offsets = new StubOffsetReader(null);

        using var task = StartWithoutDriver(TableConfig(), offsets);

        Assert.Equal(
            """SELECT * FROM "SAPABAP1"."ORDERS" ORDER BY "CHANGED_AT" LIMIT 500""",
            task.BuildQuery());
    }

    [Fact]
    public void Start_WithAnEmptyStoredCursor_ReadsFromTheBeginning()
    {
        // An offset written before the first row was ever read carries an empty cursor;
        // restoring it as a value would filter every row away.
        var offsets = new StubOffsetReader(new Dictionary<string, object>
        {
            ["incremental_value"] = ""
        });

        using var task = StartWithoutDriver(TableConfig(), offsets);

        Assert.DoesNotContain("WHERE", task.BuildQuery(), StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_WithSelectedColumns_QuotesEveryColumnAndDropsTheOrderBy()
    {
        var config = TableConfig();
        config.Remove(HanaConnectorConfig.Schema);
        config.Remove(HanaConnectorConfig.IncrementalColumn);
        config[HanaConnectorConfig.Columns] = "ID, NAME";

        using var task = StartWithoutDriver(config, new StubOffsetReader(null));

        Assert.Equal(
            """SELECT "ID", "NAME" FROM "ORDERS" LIMIT 500""",
            task.BuildQuery());
    }

    [Fact]
    public void BuildQuery_ForAQueryThatAlreadyFilters_AppendsTheCursorWithAnd()
    {
        var config = TableConfig();
        config.Remove(HanaConnectorConfig.Table);
        config.Remove(HanaConnectorConfig.Schema);
        config[HanaConnectorConfig.Query] = "SELECT * FROM ORDERS WHERE MANDT = '100'";

        var offsets = new StubOffsetReader(new Dictionary<string, object>
        {
            ["incremental_value"] = "2026-01-31T12:00:00"
        });

        using var task = StartWithoutDriver(config, offsets);

        var partition = Assert.Single(offsets.RequestedPartitions);
        Assert.Equal("query", Assert.IsType<string>(partition["table"]));

        Assert.Equal(
            """SELECT * FROM ORDERS WHERE MANDT = '100' AND "CHANGED_AT" > :lastValue LIMIT 500""",
            task.BuildQuery());
    }

    [Fact]
    public void BuildQuery_WithoutAnIncrementalColumn_ReadsThePlainTable()
    {
        var config = TableConfig();
        config.Remove(HanaConnectorConfig.IncrementalColumn);

        using var task = StartWithoutDriver(config, new StubOffsetReader(new Dictionary<string, object>
        {
            ["incremental_value"] = "2026-01-31T12:00:00"
        }));

        Assert.Equal(
            """SELECT * FROM "SAPABAP1"."ORDERS" LIMIT 500""",
            task.BuildQuery());
    }

    private static HanaSourceTask StartWithoutDriver(
        IDictionary<string, string> config,
        IOffsetStorageReader offsets)
    {
        var task = new HanaSourceTask();
        task.Initialize(new TaskContext { OffsetStorageReader = offsets, RaiseError = _ => { } });

        try
        {
            task.Start(config);
        }
        catch (NotSupportedException)
        {
            // This build ships without the SAP driver. Start parses the configuration
            // and restores the cursor before it reports the missing driver, so the
            // query builder is fully configured here.
        }

        return task;
    }

    private static Dictionary<string, string> TableConfig()
    {
        return new Dictionary<string, string>
        {
            [HanaConnectorConfig.Topic] = "hana-orders",
            [HanaConnectorConfig.Table] = "ORDERS",
            [HanaConnectorConfig.Schema] = "SAPABAP1",
            [HanaConnectorConfig.IncrementalColumn] = "CHANGED_AT",
            [HanaConnectorConfig.RowLimit] = "500",
            [HanaConnectorConfig.Host] = "hana.invalid",
            [HanaConnectorConfig.Username] = "SAPUSER",
            [HanaConnectorConfig.Password] = "secret"
        };
    }

    private sealed class StubOffsetReader : IOffsetStorageReader
    {
        private readonly IDictionary<string, object>? _storedOffset;

        public StubOffsetReader(IDictionary<string, object>? storedOffset)
        {
            _storedOffset = storedOffset;
        }

        public List<IDictionary<string, object>> RequestedPartitions { get; } = [];

        public IDictionary<string, object>? Offset(IDictionary<string, object> partition)
        {
            RequestedPartitions.Add(partition);
            return _storedOffset;
        }

        public IDictionary<IDictionary<string, object>, IDictionary<string, object>> Offsets(
            IReadOnlyCollection<IDictionary<string, object>> partitions)
        {
            return new Dictionary<IDictionary<string, object>, IDictionary<string, object>>();
        }
    }
}
