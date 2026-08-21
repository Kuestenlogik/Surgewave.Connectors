using System.Globalization;
using System.Reflection;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Cassandra;

namespace Kuestenlogik.Surgewave.Connector.Cassandra.Tests;

public class CassandraSourceTaskTests
{
    private static readonly DateTimeOffset Cursor = new(2026, 8, 20, 10, 30, 15, 250, TimeSpan.Zero);

    [Fact]
    public void RestoreOffsets_RestoresTimestampCursor_SoRestartsDoNotReIngestTheTable()
    {
        var reader = new RecordingOffsetStorageReader(new Dictionary<string, object>
        {
            [CassandraConnectorConfig.OffsetTimestamp] = Cursor.ToString("O", CultureInfo.InvariantCulture)
        });

        using var task = new CassandraSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { }, OffsetStorageReader = reader });
        SetField(task, "_sourcePartition", new Dictionary<string, object>
        {
            [CassandraConnectorConfig.OffsetTable] = "events"
        });

        Invoke(task, "RestoreOffsets");

        Assert.Single(reader.RequestedPartitions);
        Assert.Equal(Cursor, (DateTimeOffset?)GetField(task, "_lastTimestamp"));
    }

    [Fact]
    public void RestoreOffsets_RestoresPagingState_SoTheScanResumesAtTheStoredPage()
    {
        var pagingState = new byte[] { 1, 2, 3, 4 };
        var reader = new RecordingOffsetStorageReader(new Dictionary<string, object>
        {
            [CassandraConnectorConfig.OffsetPagingState] = Convert.ToBase64String(pagingState)
        });

        using var task = new CassandraSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { }, OffsetStorageReader = reader });
        SetField(task, "_sourcePartition", new Dictionary<string, object>
        {
            [CassandraConnectorConfig.OffsetTable] = "events"
        });

        Invoke(task, "RestoreOffsets");

        Assert.Equal(pagingState, (byte[]?)GetField(task, "_pagingState"));
    }

    [Fact]
    public void RestoreOffsets_WithoutStoredOffset_LeavesCursorEmpty()
    {
        var reader = new RecordingOffsetStorageReader(null);

        using var task = new CassandraSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { }, OffsetStorageReader = reader });
        SetField(task, "_sourcePartition", new Dictionary<string, object>
        {
            [CassandraConnectorConfig.OffsetTable] = "events"
        });

        Invoke(task, "RestoreOffsets");

        Assert.Null((DateTimeOffset?)GetField(task, "_lastTimestamp"));
        Assert.Null((byte[]?)GetField(task, "_pagingState"));
    }

    [Fact]
    public void BuildQuery_TableModeWithoutTimestampColumn_OmitsLimitSoPagingAdvancesTheScan()
    {
        using var task = new CassandraSourceTask();
        SetField(task, "_table", "events");
        SetField(task, "_mode", "table");
        SetField(task, "_maxRowsPerPoll", 100);

        Assert.Equal("SELECT * FROM events", BuildQuery(task));
    }

    [Fact]
    public void BuildQuery_TableModeWithTimestampCursor_FiltersInInvariantUtcFormat()
    {
        using var task = new CassandraSourceTask();
        SetField(task, "_table", "events");
        SetField(task, "_mode", "table");
        SetField(task, "_maxRowsPerPoll", 100);
        SetField(task, "_timestampColumn", "updated_at");
        SetField(task, "_lastTimestamp", new DateTimeOffset(2026, 8, 20, 12, 30, 15, 250, TimeSpan.FromHours(2)));

        var cql = BuildQuery(task);

        Assert.Contains("updated_at > '2026-08-20 10:30:15.250+0000'", cql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 100", cql, StringComparison.Ordinal);
    }

    private static string BuildQuery(CassandraSourceTask task)
        => (string)typeof(CassandraSourceTask)
            .GetMethod("BuildQuery", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(task, null)!;

    private static void Invoke(object target, string method)
        => target.GetType()
            .GetMethod(method, BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(target, null);

    private static void SetField(object target, string field, object? value)
        => target.GetType()
            .GetField(field, BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(target, value);

    private static object? GetField(object target, string field)
        => target.GetType()
            .GetField(field, BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(target);

    private sealed class RecordingOffsetStorageReader(IDictionary<string, object>? storedOffset) : IOffsetStorageReader
    {
        public List<IDictionary<string, object>> RequestedPartitions { get; } = [];

        public IDictionary<string, object>? Offset(IDictionary<string, object> partition)
        {
            RequestedPartitions.Add(partition);
            return storedOffset;
        }

        public IDictionary<IDictionary<string, object>, IDictionary<string, object>> Offsets(
            IReadOnlyCollection<IDictionary<string, object>> partitions)
        {
            var result = new Dictionary<IDictionary<string, object>, IDictionary<string, object>>();

            foreach (var partition in partitions)
            {
                var offset = Offset(partition);
                if (offset != null)
                    result[partition] = offset;
            }

            return result;
        }
    }
}
