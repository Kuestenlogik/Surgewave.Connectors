using System.Reflection;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Cassandra;

namespace Kuestenlogik.Surgewave.Connector.Cassandra.Tests;

public class CassandraSinkTaskTests
{
    [Fact]
    public void Start_WithUnsupportedWriteMode_ThrowsNamingTheSupportedModes()
    {
        using var task = new CassandraSinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var config = SinkConfig();
        config[CassandraConnectorConfig.WriteModeConfig] = "replace";

        var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
        Assert.Contains(CassandraConnectorConfig.WriteModeUpsert, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_WithUpsertModeButNoPartitionKeys_ThrowsInsteadOfSilentlyInserting()
    {
        using var task = new CassandraSinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var config = SinkConfig();
        config[CassandraConnectorConfig.WriteModeConfig] = CassandraConnectorConfig.WriteModeUpsert;

        var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
        Assert.Contains(CassandraConnectorConfig.PartitionKeyColumnsConfig, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildWriteCql_InsertMode_WritesAllColumns()
    {
        using var task = new CassandraSinkTask();
        SetField(task, "_table", "events");

        var cql = BuildWriteCql(task, ["id", "payload"], 0);

        Assert.Equal("INSERT INTO events (id, payload) VALUES (?, ?)", cql);
    }

    [Fact]
    public void BuildWriteCql_UpsertMode_UpdatesTheRowAddressedByItsKey()
    {
        using var task = new CassandraSinkTask();
        SetField(task, "_table", "events");

        var cql = BuildWriteCql(task, ["payload", "id"], 1);

        Assert.Equal("UPDATE events SET payload = ? WHERE id = ?", cql);
    }

    [Fact]
    public void ParseRecordValue_WithUnparseableValue_RaisesErrorInsteadOfDroppingSilently()
    {
        var errors = new List<Exception>();

        using var task = new CassandraSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });

        var record = new SinkRecord
        {
            Topic = "events",
            Partition = 0,
            Offset = 42,
            Value = Encoding.UTF8.GetBytes("this is not json")
        };

        var parsed = typeof(CassandraSinkTask)
            .GetMethod("ParseRecordValue", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(task, [record]);

        Assert.Null(parsed);
        var error = Assert.Single(errors);
        Assert.Contains("events[0]@42", error.Message, StringComparison.Ordinal);
    }

    private static string BuildWriteCql(CassandraSinkTask task, string[] bindOrder, int keyCount)
        => (string)typeof(CassandraSinkTask)
            .GetMethod("BuildWriteCql", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(task, [bindOrder, keyCount])!;

    private static void SetField(object target, string field, object? value)
        => target.GetType()
            .GetField(field, BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(target, value);

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [CassandraConnectorConfig.ContactPointsConfig] = "127.0.0.1",
        [CassandraConnectorConfig.KeyspaceConfig] = "test_keyspace",
        [CassandraConnectorConfig.TableConfig] = "events"
    };
}
