using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Hosting;
using Orleans.Configuration;
using Orleans.Streams;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Orleans;

/// <summary>
/// Task that subscribes to an Orleans Grain Stream and buffers events
/// for pull-based consumption by the Surgewave Connect framework.
/// </summary>
public sealed class OrleansSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _topic = "";
    private string _clusterUrl = OrleansConnectorConfig.DefaultClusterUrl;
    private string _clusterId = OrleansConnectorConfig.DefaultClusterId;
    private string _serviceId = OrleansConnectorConfig.DefaultServiceId;
    private string _streamProvider = OrleansConnectorConfig.DefaultStreamProvider;
    private string _streamNamespace = "";
    private string _streamId = "";
    private int _batchSize = OrleansConnectorConfig.DefaultBatchSize;

    private IHost? _host;
    private IClusterClient? _client;
    private StreamSubscriptionHandle<byte[]>? _subscription;
    private readonly ConcurrentQueue<(byte[] Data, StreamSequenceToken? Token)> _buffer = new();

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[OrleansConnectorConfig.Topic];

        if (config.TryGetValue(OrleansConnectorConfig.ClusterUrl, out var url))
            _clusterUrl = url;
        if (config.TryGetValue(OrleansConnectorConfig.ClusterId, out var clusterId))
            _clusterId = clusterId;
        if (config.TryGetValue(OrleansConnectorConfig.ServiceId, out var serviceId))
            _serviceId = serviceId;
        if (config.TryGetValue(OrleansConnectorConfig.StreamProvider, out var provider))
            _streamProvider = provider;
        _streamNamespace = config[OrleansConnectorConfig.StreamNamespace];
        if (config.TryGetValue(OrleansConnectorConfig.StreamId, out var streamId))
            _streamId = streamId;
        if (config.TryGetValue(OrleansConnectorConfig.BatchSize, out var batchSize))
            _batchSize = int.Parse(batchSize);

        ConnectAsync().GetAwaiter().GetResult();
    }

    private async Task ConnectAsync()
    {
        var endpoint = ParseEndpoint(_clusterUrl);

        _host = Host.CreateDefaultBuilder()
            .UseOrleansClient(clientBuilder =>
            {
                clientBuilder
                    .Configure<ClusterOptions>(options =>
                    {
                        options.ClusterId = _clusterId;
                        options.ServiceId = _serviceId;
                    })
                    .UseStaticClustering(endpoint)
                    .AddMemoryStreams(_streamProvider);
            })
            .Build();

        await _host.StartAsync();
        _client = _host.Services.GetService(typeof(IClusterClient)) as IClusterClient;

        if (_client == null)
            throw new InvalidOperationException("Failed to resolve IClusterClient from host services.");

        var streamGuid = ResolveStreamId();
        var streamProviderInstance = _client.GetStreamProvider(_streamProvider);
        var stream = streamProviderInstance.GetStream<byte[]>(StreamId.Create(_streamNamespace, streamGuid.ToString()));

        _subscription = await stream.SubscribeAsync(OnNextAsync, OnErrorAsync, OnCompletedAsync);
    }

    private Task OnNextAsync(byte[] data, StreamSequenceToken? token)
    {
        _buffer.Enqueue((data, token));
        return Task.CompletedTask;
    }

    private Task OnErrorAsync(Exception ex)
    {
        // Log and continue — errors don't stop the subscription
        return Task.CompletedTask;
    }

    private Task OnCompletedAsync()
    {
        return Task.CompletedTask;
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();
        var count = 0;

        while (count < _batchSize && _buffer.TryDequeue(out var item))
        {
            var sourcePartition = new Dictionary<string, object>
            {
                ["stream_namespace"] = _streamNamespace,
                ["stream_provider"] = _streamProvider
            };

            var sourceOffset = new Dictionary<string, object>
            {
                [OrleansConnectorConfig.OffsetSequenceToken] = item.Token?.ToString() ?? count.ToString()
            };

            var record = new SourceRecord
            {
                SourcePartition = sourcePartition,
                SourceOffset = sourceOffset,
                Topic = _topic,
                Key = Encoding.UTF8.GetBytes(_streamNamespace),
                Value = item.Data,
                Timestamp = DateTimeOffset.UtcNow
            };

            records.Add(record);
            count++;
        }

        return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Orleans handles delivery — no explicit commit needed
        return Task.CompletedTask;
    }

    public override void Stop()
    {
        if (_subscription != null)
        {
            try { _subscription.UnsubscribeAsync().GetAwaiter().GetResult(); }
            catch { /* ignore */ }
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            try { _host?.StopAsync().GetAwaiter().GetResult(); }
            catch { /* ignore */ }
            _host?.Dispose();
        }
        base.Dispose(disposing);
    }

    private Guid ResolveStreamId()
    {
        if (!string.IsNullOrEmpty(_streamId) && Guid.TryParse(_streamId, out var parsed))
            return parsed;

        // Derive a deterministic GUID from the topic name
        var hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(_topic));
        var guidBytes = new byte[16];
        Array.Copy(hashBytes, guidBytes, 16);
        return new Guid(guidBytes);
    }

    private static IPEndPoint ParseEndpoint(string address)
    {
        var parts = address.Split(':');
        var host = parts[0];
        var port = parts.Length > 1 ? int.Parse(parts[1]) : 30000;

        if (IPAddress.TryParse(host, out var ip))
            return new IPEndPoint(ip, port);

        // Resolve hostname
        var addresses = Dns.GetHostAddresses(host);
        return new IPEndPoint(addresses[0], port);
    }
}
