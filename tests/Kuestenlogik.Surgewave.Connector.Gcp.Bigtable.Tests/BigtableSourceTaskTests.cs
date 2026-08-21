using System.Text;
using Google.Cloud.Bigtable.V2;
using Google.Protobuf;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Bigtable.Tests;

/// <summary>
/// Exercises the read side without a Bigtable connection: the row set a poll asks for, the
/// filter it applies and the record it turns a row into.
/// </summary>
public class BigtableSourceTaskTests
{
    [Fact]
    public void BuildRowSet_WithAPrefixAndNoProgressYet_StartsAtThePrefixAndEndsAtItsSuccessor()
    {
        using var task = StartedTask(c => c[BigtableConnectorConfig.RowKeyPrefix] = "user#");

        var range = Assert.Single(task.BuildRowSet(null).RowRanges);

        Assert.Equal("user#", range.StartKeyClosed.ToStringUtf8());
        Assert.Equal("user$", range.EndKeyOpen.ToStringUtf8());
    }

    [Fact]
    public void BuildRowSet_WithAPrefixAfterProgress_ResumesAfterTheLastRowKey()
    {
        // The prefix branch used to ignore the last row key, so every poll re-read the same
        // first page of the prefix range forever.
        using var task = StartedTask(c => c[BigtableConnectorConfig.RowKeyPrefix] = "user#");

        var range = Assert.Single(task.BuildRowSet("user#0042").RowRanges);

        Assert.Equal("user#0042", range.StartKeyOpen.ToStringUtf8());
        Assert.False(range.HasStartKeyClosed);
        Assert.Equal("user$", range.EndKeyOpen.ToStringUtf8());
    }

    [Fact]
    public void BuildRowSet_WithAnExplicitRangeAfterProgress_ResumesAfterTheLastRowKeyAndKeepsTheEnd()
    {
        using var task = StartedTask(c =>
        {
            c[BigtableConnectorConfig.RowKeyStart] = "a";
            c[BigtableConnectorConfig.RowKeyEnd] = "m";
        });

        var range = Assert.Single(task.BuildRowSet("f").RowRanges);

        Assert.Equal("f", range.StartKeyOpen.ToStringUtf8());
        Assert.False(range.HasStartKeyClosed);
        Assert.Equal("m", range.EndKeyOpen.ToStringUtf8());
    }

    [Fact]
    public void BuildRowSet_WithAnExplicitRangeAndNoProgressYet_StartsAtTheConfiguredStart()
    {
        using var task = StartedTask(c =>
        {
            c[BigtableConnectorConfig.RowKeyStart] = "a";
            c[BigtableConnectorConfig.RowKeyEnd] = "m";
        });

        var range = Assert.Single(task.BuildRowSet(null).RowRanges);

        Assert.Equal("a", range.StartKeyClosed.ToStringUtf8());
        Assert.False(range.HasStartKeyOpen);
        Assert.Equal("m", range.EndKeyOpen.ToStringUtf8());
    }

    [Fact]
    public void BuildRowSet_WithNoBoundsAtAll_ScansEverythingUntilTheFirstRowWasRead()
    {
        using var task = StartedTask(_ => { });

        Assert.Empty(task.BuildRowSet(null).RowRanges);

        var range = Assert.Single(task.BuildRowSet("z").RowRanges);
        Assert.Equal("z", range.StartKeyOpen.ToStringUtf8());
        Assert.False(range.HasEndKeyOpen);
    }

    [Fact]
    public void BuildFilter_WithFamilyAndColumns_ChainsFamilyColumnsAndTheLatestCellLimit()
    {
        using var task = StartedTask(c =>
        {
            c[BigtableConnectorConfig.ColumnFamily] = "cf";
            c[BigtableConnectorConfig.Columns] = "name, email";
        });

        var filter = task.BuildFilter();

        Assert.NotNull(filter);
        Assert.Equal(3, filter!.Chain.Filters.Count);
        Assert.Contains("cf", filter.Chain.Filters[0].FamilyNameRegexFilter, StringComparison.Ordinal);
        var interleaved = filter.Chain.Filters[1].Interleave.Filters;
        Assert.Equal(2, interleaved.Count);
        Assert.Contains("name", interleaved[0].ColumnQualifierRegexFilter.ToStringUtf8(), StringComparison.Ordinal);
        Assert.Contains("email", interleaved[1].ColumnQualifierRegexFilter.ToStringUtf8(), StringComparison.Ordinal);
        Assert.Equal(1, filter.Chain.Filters[2].CellsPerColumnLimitFilter);
    }

    [Fact]
    public void BuildFilter_WithNothingConfigured_StillLimitsToTheLatestCell()
    {
        using var task = StartedTask(_ => { });

        var filter = task.BuildFilter();

        Assert.NotNull(filter);
        Assert.True(filter!.HasCellsPerColumnLimitFilter);
        Assert.Equal(1, filter.CellsPerColumnLimitFilter);
    }

    [Fact]
    public void CreateRecord_KeysTheRecordByRowKeyAndCarriesItInHeadersAndOffset()
    {
        using var task = StartedTask(c => c[BigtableConnectorConfig.IncludeTimestamp] = "false");

        var record = task.CreateRecord(RowWith("user#1", "cf", "name", "Ada"));

        Assert.Equal("bigtable-rows", record.Topic);
        Assert.Equal("user#1", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("events", Encoding.UTF8.GetString(record.Headers!["bigtable.table"]));
        Assert.Equal("user#1", Encoding.UTF8.GetString(record.Headers!["bigtable.rowkey"]));
        Assert.Equal("bigtable", record.SourcePartition["source"]);
        Assert.Equal("events", record.SourcePartition["table"]);
        Assert.Equal(1L, record.SourceOffset["message_id"]);
        Assert.Equal("user#1", record.SourceOffset["row_key"]);

        var payload = Encoding.UTF8.GetString(record.Value);
        Assert.Contains("\"rowKey\":\"user#1\"", payload, StringComparison.Ordinal);
        Assert.Contains("\"cf\":{\"name\":\"Ada\"}", payload, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRecord_WithTimestampsIncluded_CarriesTheCellValueBase64EncodedNextToItsVersion()
    {
        using var task = StartedTask(_ => { });

        var record = task.CreateRecord(RowWith("user#1", "cf", "name", "Ada", timestampMicros: 1_700_000_000_000_000));

        var payload = Encoding.UTF8.GetString(record.Value);
        Assert.Contains("\"value\":\"QWRh\"", payload, StringComparison.Ordinal);
        Assert.Contains("\"timestamp\":1700000000000000", payload, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRecord_AssignsAscendingMessageIds()
    {
        using var task = StartedTask(_ => { });

        var first = task.CreateRecord(RowWith("a", "cf", "name", "Ada"));
        var second = task.CreateRecord(RowWith("b", "cf", "name", "Grace"));

        Assert.Equal(1L, first.SourceOffset["message_id"]);
        Assert.Equal(2L, second.SourceOffset["message_id"]);
        Assert.Equal("a", first.SourceOffset["row_key"]);
        Assert.Equal("b", second.SourceOffset["row_key"]);
    }

    [Fact]
    public async Task PollAsync_WhenTheReadFails_SurfacesTheErrorInsteadOfSpinningSilently()
    {
        // The empty catch turned a permanently failing source into an endless stream of
        // empty batches with nothing logged anywhere.
        var errors = new List<Exception>();
        using var task = new BigtableSourceTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfig(SourceConfig());

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Empty(records);
        Assert.Single(errors);
    }

    private static Row RowWith(string rowKey, string family, string qualifier, string value, long timestampMicros = 0) =>
        new()
        {
            Key = ByteString.CopyFromUtf8(rowKey),
            Families =
            {
                new Family
                {
                    Name = family,
                    Columns =
                    {
                        new Column
                        {
                            Qualifier = ByteString.CopyFromUtf8(qualifier),
                            Cells =
                            {
                                new Cell
                                {
                                    Value = ByteString.CopyFromUtf8(value),
                                    TimestampMicros = timestampMicros
                                }
                            }
                        }
                    }
                }
            }
        };

    private static BigtableSourceTask StartedTask(Action<Dictionary<string, string>> configure)
    {
        var config = SourceConfig();
        configure(config);

        var task = new BigtableSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.ApplyConfig(config);
        return task;
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [BigtableConnectorConfig.ProjectId] = "demo-project",
        [BigtableConnectorConfig.InstanceId] = "demo-instance",
        [BigtableConnectorConfig.TableId] = "events",
        [BigtableConnectorConfig.Topic] = "bigtable-rows",
        [BigtableConnectorConfig.PollIntervalMs] = "0"
    };
}
