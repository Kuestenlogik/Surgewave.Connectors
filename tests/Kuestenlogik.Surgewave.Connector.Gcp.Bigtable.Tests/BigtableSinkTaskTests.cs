using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Bigtable.Tests;

/// <summary>
/// Exercises the write side without a Bigtable connection: how a record becomes mutations,
/// how cell values are decoded, and what happens when a write batch fails.
/// </summary>
public class BigtableSinkTaskTests
{
    [Fact]
    public void CreateMutationEntry_FlatPayload_WritesEveryFieldToTheDefaultFamilyAndSkipsTheRowKeyField()
    {
        using var task = StartedTask(_ => { });

        var entry = task.CreateMutationEntry(Record("""{"rowKey":"user#1","name":"Ada","age":36}"""));

        Assert.NotNull(entry);
        Assert.Equal("user#1", entry!.RowKey.ToStringUtf8());
        Assert.Equal(2, entry.Mutations.Count);
        Assert.Equal("cf", entry.Mutations[0].SetCell.FamilyName);
        Assert.Equal("name", entry.Mutations[0].SetCell.ColumnQualifier.ToStringUtf8());
        Assert.Equal("Ada", entry.Mutations[0].SetCell.Value.ToStringUtf8());
        Assert.Equal("age", entry.Mutations[1].SetCell.ColumnQualifier.ToStringUtf8());
        Assert.Equal("36", entry.Mutations[1].SetCell.Value.ToStringUtf8());
    }

    [Fact]
    public void CreateMutationEntry_FamiliesPayload_KeepsTheDeclaredColumnFamilies()
    {
        using var task = StartedTask(_ => { });

        var entry = task.CreateMutationEntry(
            Record("""{"rowKey":"user#1","families":{"profile":{"name":"Ada"},"stats":{"logins":"3"}}}"""));

        Assert.NotNull(entry);
        Assert.Equal(2, entry!.Mutations.Count);
        Assert.Equal("profile", entry.Mutations[0].SetCell.FamilyName);
        Assert.Equal("name", entry.Mutations[0].SetCell.ColumnQualifier.ToStringUtf8());
        Assert.Equal("stats", entry.Mutations[1].SetCell.FamilyName);
        Assert.Equal("logins", entry.Mutations[1].SetCell.ColumnQualifier.ToStringUtf8());
    }

    [Fact]
    public void CreateMutationEntry_WithoutTheRowKeyField_FallsBackToTheRecordKey()
    {
        using var task = StartedTask(_ => { });

        var entry = task.CreateMutationEntry(
            Record("""{"name":"Ada"}""", Encoding.UTF8.GetBytes("user#7")));

        Assert.NotNull(entry);
        Assert.Equal("user#7", entry!.RowKey.ToStringUtf8());
        Assert.Equal("name", Assert.Single(entry.Mutations).SetCell.ColumnQualifier.ToStringUtf8());
    }

    [Fact]
    public void CreateMutationEntry_WithNoRowKeyAnywhere_ProducesNoEntry()
    {
        using var task = StartedTask(_ => { });

        Assert.Null(task.CreateMutationEntry(Record("""{"name":"Ada"}""")));
    }

    [Fact]
    public void CreateMutationEntry_WithNothingButTheRowKey_ProducesNoEntry()
    {
        using var task = StartedTask(_ => { });

        Assert.Null(task.CreateMutationEntry(Record("""{"rowKey":"user#1"}""")));
    }

    [Fact]
    public void CreateMutationEntry_HonoursACustomRowKeyFieldAndDefaultFamily()
    {
        using var task = StartedTask(c =>
        {
            c[BigtableConnectorConfig.RowKeyField] = "id";
            c[BigtableConnectorConfig.DefaultColumnFamily] = "data";
        });

        var entry = task.CreateMutationEntry(Record("""{"id":"user#1","name":"Ada"}"""));

        Assert.NotNull(entry);
        Assert.Equal("user#1", entry!.RowKey.ToStringUtf8());
        Assert.Equal("data", Assert.Single(entry.Mutations).SetCell.FamilyName);
    }

    [Fact]
    public void DecodeCellValue_Base64WrappedValue_IsDecodedToItsBytes()
    {
        var bytes = BigtableSinkTask.DecodeCellValue(Element("""{"value":"QWRh","timestamp":1}"""));

        Assert.Equal("Ada", Encoding.UTF8.GetString(bytes));
    }

    [Fact]
    public void DecodeCellValue_WrappedValueThatIsNotBase64_FallsBackToItsUtf8Bytes()
    {
        var bytes = BigtableSinkTask.DecodeCellValue(Element("""{"value":"Ada!"}"""));

        Assert.Equal("Ada!", Encoding.UTF8.GetString(bytes));
    }

    [Theory]
    [InlineData("\"Ada\"", "Ada")]
    [InlineData("36", "36")]
    public void DecodeCellValue_ScalarValue_IsTakenAsUtf8(string json, string expected)
    {
        var bytes = BigtableSinkTask.DecodeCellValue(Element(json));

        Assert.Equal(expected, Encoding.UTF8.GetString(bytes));
    }

    [Fact]
    public void CreateReadModifyWriteRule_InIncrementMode_ProducesAnIncrementNotASetCell()
    {
        // "increment" used to be advertised in the ConfigDef and then quietly written as
        // a SetCell, overwriting the counter instead of advancing it.
        using var task = StartedTask(c => c[BigtableConnectorConfig.WriteMode] = "increment");

        var rule = task.CreateReadModifyWriteRule("cf", "hits", Element("5"));

        Assert.True(rule.HasIncrementAmount);
        Assert.Equal(5L, rule.IncrementAmount);
        Assert.Equal("cf", rule.FamilyName);
        Assert.Equal("hits", rule.ColumnQualifier.ToStringUtf8());
    }

    [Fact]
    public void CreateReadModifyWriteRule_InIncrementMode_AcceptsAnIntegerCarriedAsAString()
    {
        using var task = StartedTask(c => c[BigtableConnectorConfig.WriteMode] = "increment");

        var rule = task.CreateReadModifyWriteRule("cf", "hits", Element("\"7\""));

        Assert.Equal(7L, rule.IncrementAmount);
    }

    [Fact]
    public void CreateReadModifyWriteRule_InIncrementMode_RejectsANonIntegerValue()
    {
        using var task = StartedTask(c => c[BigtableConnectorConfig.WriteMode] = "increment");

        var ex = Assert.Throws<InvalidOperationException>(
            () => task.CreateReadModifyWriteRule("cf", "hits", Element("\"not-a-number\"")));

        Assert.Contains("cf:hits", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateReadModifyWriteRule_InAppendMode_ProducesAnAppend()
    {
        using var task = StartedTask(c => c[BigtableConnectorConfig.WriteMode] = "append");

        var rule = task.CreateReadModifyWriteRule("cf", "log", Element("\"line\""));

        Assert.True(rule.HasAppendValue);
        Assert.Equal("line", rule.AppendValue.ToStringUtf8());
    }

    [Fact]
    public async Task PutAsync_WhenTheMutationCallFails_RaisesAndRethrowsInsteadOfDroppingTheBatch()
    {
        // Start was deliberately not called, so flushing the batch fails on the missing client.
        // What is pinned is where that failure goes: it used to be swallowed by an empty catch
        // while the worker committed the offsets of records Bigtable never stored.
        var errors = new List<Exception>();
        using var task = new BigtableSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfig(SinkConfig());

        var thrown = await Assert.ThrowsAnyAsync<Exception>(() =>
            task.PutAsync([Record("""{"rowKey":"user#1","name":"Ada"}""")],
                TestContext.Current.CancellationToken));

        Assert.Same(thrown, Assert.Single(errors));
    }

    [Fact]
    public async Task PutAsync_WithoutRecordValues_WritesNothing()
    {
        var errors = new List<Exception>();
        using var task = new BigtableSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfig(SinkConfig());

        await task.PutAsync([RecordWithoutValue()], TestContext.Current.CancellationToken);

        Assert.Empty(errors);
    }

    private static JsonElement Element(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }

    private static SinkRecord Record(string json, byte[]? key = null) => new()
    {
        Topic = "bigtable-writes",
        Partition = 0,
        Offset = 0,
        Key = key,
        Value = Encoding.UTF8.GetBytes(json)
    };

    private static SinkRecord RecordWithoutValue() => new()
    {
        Topic = "bigtable-writes",
        Partition = 0,
        Offset = 0,
        Value = null!
    };

    private static BigtableSinkTask StartedTask(Action<Dictionary<string, string>> configure)
    {
        var config = SinkConfig();
        configure(config);

        var task = new BigtableSinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.ApplyConfig(config);
        return task;
    }

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [BigtableConnectorConfig.ProjectId] = "demo-project",
        [BigtableConnectorConfig.InstanceId] = "demo-instance",
        [BigtableConnectorConfig.TableId] = "events",
        [BigtableConnectorConfig.Topics] = "bigtable-writes",
        [BigtableConnectorConfig.BatchSize] = "1"
    };
}
