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

    [Fact]
    public void TeamsSourceTask_Start_RestoresCursorFromOffsetStorage()
    {
        var reader = new RecordingOffsetStorageReader(new Dictionary<string, object>
        {
            [TeamsConnectorConfig.OffsetCursor] = "2026-01-02T03:04:05.0000000+00:00",
            [TeamsConnectorConfig.OffsetMessageId] = "message-1"
        });

        using var task = new TeamsSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { }, OffsetStorageReader = reader });
        task.Start(SourceConfig());

        // The task must ask offset storage for its own team/channel partition instead of
        // starting from "now" and dropping everything posted while it was stopped.
        var partition = Assert.Single(reader.RequestedPartitions);
        Assert.Equal("team-1", partition[TeamsConnectorConfig.PartitionTeamId]);
        Assert.Equal("channel-1", partition[TeamsConnectorConfig.PartitionChannelId]);
    }

    [Fact]
    public void TeamsSourceTask_Start_WithoutStoredOffset_StillQueriesOffsetStorage()
    {
        var reader = new RecordingOffsetStorageReader(null);

        using var task = new TeamsSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { }, OffsetStorageReader = reader });
        task.Start(SourceConfig());

        Assert.Single(reader.RequestedPartitions);
        // Should not throw when nothing was stored yet
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [TeamsConnectorConfig.Topic] = "teams-messages",
        [TeamsConnectorConfig.TenantId] = "00000000-0000-0000-0000-000000000000",
        [TeamsConnectorConfig.ClientId] = "11111111-1111-1111-1111-111111111111",
        [TeamsConnectorConfig.ClientSecret] = "test-secret",
        [TeamsConnectorConfig.TeamId] = "team-1",
        [TeamsConnectorConfig.ChannelId] = "channel-1"
    };

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
