using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using NATS.Client.JetStream;
using NATS.Client.ObjectStore;
using NATS.Client.ObjectStore.Models;

namespace Kuestenlogik.Surgewave.Connector.Nats.ObjectStore.Tests;

/// <summary>
/// The source used to fire-and-forget its initialisation and swallow every connect, bucket and
/// watch failure, so a broken bucket was indistinguishable from an idle one. These tests drive the
/// task against a fake object store and pin the record mapping, the filters and the failure path.
/// </summary>
public class NatsObjectStoreSourceTaskTests
{
    [Fact]
    public async Task PollAsync_TurnsObjectChangesIntoRecords()
    {
        var store = new FakeObjectStore();
        store.Publish(Info("report.pdf", 5), "hello");

        using var task = new NatsObjectStoreSourceTask(store);
        task.Initialize(new TaskContext());
        task.Configure(SourceConfig());

        var record = Assert.Single(await DrainAsync(task, 1));
        task.Stop();

        Assert.Equal("objectstore-events", record.Topic);
        Assert.Equal("report.pdf", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("nats-objectstore", (string)record.SourcePartition["source"]);
        Assert.Equal("assets", (string)record.SourcePartition["bucket"]);
        Assert.Equal(1L, (long)record.SourceOffset["message_id"]);
        Assert.Equal("report.pdf", (string)record.SourceOffset["name"]);
        Assert.Equal("nuid-report.pdf", (string)record.SourceOffset["nuid"]);
        Assert.Equal("assets", Encoding.UTF8.GetString(record.Headers!["nats.objectstore.bucket"]));
        Assert.Equal("put", Encoding.UTF8.GetString(record.Headers!["nats.objectstore.operation"]));
        Assert.Equal("5", Encoding.UTF8.GetString(record.Headers!["nats.objectstore.size"]));

        var payload = Payload(record);
        Assert.Equal("put", payload.GetProperty("type").GetString());
        Assert.Equal("report.pdf", payload.GetProperty("name").GetString());
        Assert.Equal("base64", payload.GetProperty("contentEncoding").GetString());
        Assert.Equal(
            "hello",
            Encoding.UTF8.GetString(Convert.FromBase64String(payload.GetProperty("content").GetString()!)));
    }

    [Fact]
    public async Task PollAsync_SkipsObjectsOutsideTheWatchPrefix()
    {
        var store = new FakeObjectStore();
        store.Publish(Info("other/note.txt", 2), "no");
        store.Publish(Info("reports/report.pdf", 5), "hello");

        using var task = new NatsObjectStoreSourceTask(store);
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config[NatsObjectStoreConnectorConfig.WatchPrefix] = "reports/";
        task.Configure(config);

        var record = Assert.Single(await DrainAsync(task, 1));
        task.Stop();

        Assert.Equal("reports/report.pdf", Encoding.UTF8.GetString(record.Key!));

        // A filtered-out object must not even be downloaded.
        Assert.Equal(new[] { "reports/report.pdf" }, store.Fetched);
    }

    [Fact]
    public async Task PollAsync_MarksDeletesAndDoesNotFetchTheirContent()
    {
        var store = new FakeObjectStore();
        store.Publish(Info("gone.txt", 12, deleted: true), "irrelevant");

        using var task = new NatsObjectStoreSourceTask(store);
        task.Initialize(new TaskContext());
        task.Configure(SourceConfig());

        var record = Assert.Single(await DrainAsync(task, 1));
        task.Stop();

        Assert.Equal("delete", Encoding.UTF8.GetString(record.Headers!["nats.objectstore.operation"]));
        Assert.Equal(JsonValueKind.Null, Payload(record).GetProperty("content").ValueKind);
        Assert.Empty(store.Fetched);
    }

    [Fact]
    public async Task PollAsync_LeavesOutContentAboveTheConfiguredMaximum()
    {
        var store = new FakeObjectStore();
        store.Publish(Info("huge.bin", 5), "hello");

        using var task = new NatsObjectStoreSourceTask(store);
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config[NatsObjectStoreConnectorConfig.MaxContentSize] = "4";
        task.Configure(config);

        var record = Assert.Single(await DrainAsync(task, 1));
        task.Stop();

        // The change event still flows, only the payload is left behind.
        Assert.Equal("huge.bin", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal(JsonValueKind.Null, Payload(record).GetProperty("content").ValueKind);
        Assert.Empty(store.Fetched);
    }

    [Fact]
    public async Task PollAsync_PassesTheWatchOptionsTheConfigurationAsksFor()
    {
        var store = new FakeObjectStore();
        store.Publish(Info("a.txt", 1), "a");

        using var task = new NatsObjectStoreSourceTask(store);
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config[NatsObjectStoreConnectorConfig.IncludeDeletes] = "false";
        config[NatsObjectStoreConnectorConfig.IncludeHistory] = "true";
        task.Configure(config);

        Assert.Single(await DrainAsync(task, 1));
        task.Stop();

        var opts = Assert.IsType<NatsObjWatchOpts>(store.LastWatchOpts);
        Assert.True(opts.IgnoreDeletes);
        Assert.True(opts.IncludeHistory);
    }

    [Fact]
    public async Task PollAsync_SurfacesAWatchThatDied()
    {
        var failure = new InvalidOperationException("watch subscription dropped");
        var store = new FakeObjectStore { WatchFailure = failure };

        using var task = new NatsObjectStoreSourceTask(store);

        Exception? raised = null;
        task.Initialize(new TaskContext { RaiseError = ex => raised = ex });
        task.Configure(SourceConfig());

        await task.PollAsync(CancellationToken.None);
        await WaitUntilAsync(() => raised != null);
        task.Stop();

        // A dead watch must not degenerate into an endless run of empty polls.
        Assert.Same(failure, raised);
    }

    private static async Task<IReadOnlyList<SourceRecord>> DrainAsync(NatsObjectStoreSourceTask task, int expected)
    {
        var records = new List<SourceRecord>();
        // Generous: the deadline only breaks a runaway loop, it is not the assertion -
        // a loaded CI runner can starve these polls for far longer than the happy path needs.
        var deadline = DateTime.UtcNow.AddSeconds(30);

        while (records.Count < expected && DateTime.UtcNow < deadline)
        {
            records.AddRange(await task.PollAsync(CancellationToken.None));

            if (records.Count < expected)
            {
                await Task.Delay(10);
            }
        }

        return records;
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);

        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }
    }

    private static JsonElement Payload(SourceRecord record)
    {
        using var document = JsonDocument.Parse(Encoding.UTF8.GetString(record.Value));
        return document.RootElement.Clone();
    }

    private static ObjectMetadata Info(string name, ulong size, bool deleted = false) => new()
    {
        Name = name,
        Bucket = "assets",
        Nuid = "nuid-" + name,
        Size = size,
        Chunks = 1,
        Digest = "SHA-256=abc",
        Deleted = deleted,
        MTime = new DateTimeOffset(2026, 8, 20, 12, 0, 0, TimeSpan.Zero)
    };

    private static Dictionary<string, string> SourceConfig() => new(StringComparer.Ordinal)
    {
        [NatsObjectStoreConnectorConfig.Topic] = "objectstore-events",
        [NatsObjectStoreConnectorConfig.BucketName] = "assets",
        [NatsObjectStoreConnectorConfig.Servers] = "nats://localhost:4222"
    };

    private sealed class FakeObjectStore : INatsObjStore
    {
        private readonly Queue<ObjectMetadata> _pending = new();
        private readonly Dictionary<string, byte[]> _contents = new(StringComparer.Ordinal);

        public Exception? WatchFailure { get; init; }

        public NatsObjWatchOpts? LastWatchOpts { get; private set; }

        public List<string> Fetched { get; } = [];

        public string Bucket => "assets";

        public INatsJSContext JetStreamContext => throw new NotSupportedException();

        public void Publish(ObjectMetadata info, string content)
        {
            _pending.Enqueue(info);
            _contents[info.Name] = Encoding.UTF8.GetBytes(content);
        }

        public async IAsyncEnumerable<ObjectMetadata> WatchAsync(
            NatsObjWatchOpts? opts = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            LastWatchOpts = opts;
            await Task.Yield();

            while (_pending.TryDequeue(out var info))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return info;
            }

            var failure = WatchFailure;
            if (failure != null)
            {
                throw failure;
            }
        }

        public ValueTask<ObjectMetadata> GetAsync(string key, Stream stream, bool leaveOpen = false, CancellationToken cancellationToken = default)
        {
            Fetched.Add(key);

            var content = _contents[key];
            stream.Write(content, 0, content.Length);

            return ValueTask.FromResult(new ObjectMetadata { Name = key, Size = (ulong)content.Length });
        }

        public ValueTask<byte[]> GetBytesAsync(string key, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<ObjectMetadata> PutAsync(string key, byte[] value, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<ObjectMetadata> PutAsync(string key, Stream stream, bool leaveOpen = false, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<ObjectMetadata> PutAsync(ObjectMetadata meta, Stream stream, bool leaveOpen = false, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<ObjectMetadata> UpdateMetaAsync(string key, ObjectMetadata meta, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<ObjectMetadata> AddLinkAsync(string link, string target, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<ObjectMetadata> AddLinkAsync(string link, ObjectMetadata target, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<ObjectMetadata> AddBucketLinkAsync(string link, INatsObjStore target, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask SealAsync(CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<ObjectMetadata> GetInfoAsync(string key, bool showDeleted = false, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public IAsyncEnumerable<ObjectMetadata> ListAsync(NatsObjListOpts? opts = null, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<NatsObjStatus> GetStatusAsync(CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask DeleteAsync(string key, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
    }
}
