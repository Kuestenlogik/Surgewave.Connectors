using System.Text;
using Kuestenlogik.Surgewave.Connect;
using NATS.Client.JetStream;
using NATS.Client.ObjectStore;
using NATS.Client.ObjectStore.Models;

namespace Kuestenlogik.Surgewave.Connector.Nats.ObjectStore.Tests;

/// <summary>
/// The sink used to wait five seconds for a fire-and-forget initialisation and then return
/// quietly, and to wrap every object write in an empty catch, so a whole batch could be discarded
/// while the worker advanced the consumer offsets past it. These tests drive the task against a
/// fake object store and pin both the object mapping and the failure path.
/// </summary>
public class NatsObjectStoreSinkTaskTests
{
    [Fact]
    public async Task PutAsync_WritesTheObjectNamedByTheConfiguredField()
    {
        var store = new FakeObjectStore();
        using var task = new NatsObjectStoreSinkTask(store);
        task.Initialize(new TaskContext());
        task.Configure(SinkConfig());

        await task.PutAsync(
            [Record("""{"name":"report.pdf","content":"aGVsbG8=","contentEncoding":"base64","description":"nightly"}""")],
            CancellationToken.None);

        var put = Assert.Single(store.Puts);
        Assert.Equal("report.pdf", put.Meta.Name);
        Assert.Equal("nightly", put.Meta.Description);
        Assert.Equal("hello", Encoding.UTF8.GetString(put.Content));
        Assert.Empty(store.Deletes);
    }

    [Fact]
    public async Task PutAsync_FallsBackToTheRecordKeyAndTheWholePayload()
    {
        var store = new FakeObjectStore();
        using var task = new NatsObjectStoreSinkTask(store);
        task.Initialize(new TaskContext());
        task.Configure(SinkConfig());

        const string value = """{"payload":"no name and no content field"}""";
        await task.PutAsync([Record(value, key: "fallback.json")], CancellationToken.None);

        var put = Assert.Single(store.Puts);
        Assert.Equal("fallback.json", put.Meta.Name);
        Assert.Equal(value, Encoding.UTF8.GetString(put.Content));
    }

    [Fact]
    public async Task PutAsync_AppliesThePrefixContentTypeAndChunkSize()
    {
        var store = new FakeObjectStore();
        using var task = new NatsObjectStoreSinkTask(store);
        task.Initialize(new TaskContext());

        var config = SinkConfig();
        config[NatsObjectStoreConnectorConfig.ObjectNamePrefix] = "reports/";
        config[NatsObjectStoreConnectorConfig.ContentType] = "application/pdf";
        config[NatsObjectStoreConnectorConfig.ChunkSize] = "4096";
        task.Configure(config);

        await task.PutAsync([Record("""{"name":"report.pdf","content":"aGVsbG8=","contentEncoding":"base64"}""")], CancellationToken.None);

        var put = Assert.Single(store.Puts);
        Assert.Equal("reports/report.pdf", put.Meta.Name);
        Assert.Equal(new[] { "application/pdf" }, put.Meta.Headers!["Content-Type"]);

        // 'nats.objectstore.chunk.size' is declared by the connector, so the task has to honour it.
        Assert.Equal(4096, put.Meta.Options!.MaxChunkSize);
    }

    [Fact]
    public async Task PutAsync_DeletesWhenTheOperationHeaderSaysSo()
    {
        var store = new FakeObjectStore();
        using var task = new NatsObjectStoreSinkTask(store);
        task.Initialize(new TaskContext());
        task.Configure(SinkConfig());

        var record = Record("""{"name":"report.pdf"}""") with
        {
            Headers = new Dictionary<string, byte[]>(StringComparer.Ordinal)
            {
                ["nats.objectstore.operation"] = Encoding.UTF8.GetBytes("delete")
            }
        };

        await task.PutAsync([record], CancellationToken.None);

        Assert.Equal("report.pdf", Assert.Single(store.Deletes));
        Assert.Empty(store.Puts);
    }

    [Fact]
    public async Task PutAsync_WritesEveryRecordInTheBatch()
    {
        var store = new FakeObjectStore();
        using var task = new NatsObjectStoreSinkTask(store);
        task.Initialize(new TaskContext());
        task.Configure(SinkConfig());

        await task.PutAsync(
            [
                Record("""{"name":"a.txt","content":"a"}"""),
                Record("""{"name":"b.txt","content":"b"}"""),
                Record("""{"name":"c.txt","content":"c"}""")
            ],
            CancellationToken.None);

        Assert.Equal(new[] { "a.txt", "b.txt", "c.txt" }, store.Puts.Select(p => p.Meta.Name).ToList());
    }

    [Fact]
    public async Task PutAsync_RaisesAndRethrows_WhenTheObjectWriteFails()
    {
        var failure = new InvalidOperationException("bucket is sealed");
        var store = new FakeObjectStore { PutFailure = failure };
        using var task = new NatsObjectStoreSinkTask(store);

        Exception? raised = null;
        task.Initialize(new TaskContext { RaiseError = ex => raised = ex });
        task.Configure(SinkConfig());

        // A dropped object write must fail the batch. Swallowing it would let the worker commit an
        // offset for a record that never reached the bucket.
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => task.PutAsync([Record("""{"name":"a.txt","content":"a"}""")], CancellationToken.None));

        Assert.Same(failure, thrown);
        Assert.Same(failure, raised);
    }

    private static Dictionary<string, string> SinkConfig() => new(StringComparer.Ordinal)
    {
        [NatsObjectStoreConnectorConfig.BucketName] = "assets",
        [NatsObjectStoreConnectorConfig.Servers] = "nats://localhost:4222",
        [NatsObjectStoreConnectorConfig.ObjectNameField] = "name"
    };

    private static SinkRecord Record(string value, string? key = null) => new()
    {
        Topic = "objectstore-out",
        Partition = 0,
        Offset = 1,
        Key = key is null ? null : Encoding.UTF8.GetBytes(key),
        Value = Encoding.UTF8.GetBytes(value),
        Timestamp = DateTimeOffset.UnixEpoch
    };

    private sealed record PutCall(ObjectMetadata Meta, byte[] Content);

    private sealed class FakeObjectStore : INatsObjStore
    {
        public List<PutCall> Puts { get; } = [];

        public List<string> Deletes { get; } = [];

        public Exception? PutFailure { get; init; }

        public string Bucket => "assets";

        public INatsJSContext JetStreamContext => throw new NotSupportedException();

        public ValueTask<ObjectMetadata> PutAsync(ObjectMetadata meta, Stream stream, bool leaveOpen = false, CancellationToken cancellationToken = default)
        {
            var failure = PutFailure;
            if (failure != null)
            {
                throw failure;
            }

            using var buffer = new MemoryStream();
            stream.CopyTo(buffer);
            Puts.Add(new PutCall(meta, buffer.ToArray()));

            return ValueTask.FromResult(meta);
        }

        public ValueTask DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            Deletes.Add(key);
            return ValueTask.CompletedTask;
        }

        public ValueTask<byte[]> GetBytesAsync(string key, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<ObjectMetadata> GetAsync(string key, Stream stream, bool leaveOpen = false, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<ObjectMetadata> PutAsync(string key, byte[] value, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<ObjectMetadata> PutAsync(string key, Stream stream, bool leaveOpen = false, CancellationToken cancellationToken = default)
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

        public IAsyncEnumerable<ObjectMetadata> WatchAsync(NatsObjWatchOpts? opts = null, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
    }
}
