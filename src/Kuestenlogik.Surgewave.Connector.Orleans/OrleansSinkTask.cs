using System.Net;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Hosting;
using Orleans.Configuration;
using Orleans.Streams;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Orleans;

/// <summary>
/// Task that publishes Surgewave records to an Orleans Grain Stream.
/// </summary>
public sealed class OrleansSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _clusterUrl = OrleansConnectorConfig.DefaultClusterUrl;
    private string _clusterId = OrleansConnectorConfig.DefaultClusterId;
    private string _serviceId = OrleansConnectorConfig.DefaultServiceId;
    private string _streamProvider = OrleansConnectorConfig.DefaultStreamProvider;
    private string _streamNamespace = "";
    private string _streamId = "";
    private int _publishTimeoutMs = OrleansConnectorConfig.DefaultPublishTimeoutMs;
    private int _retries = OrleansConnectorConfig.DefaultRetries;

    private IHost? _host;
    private IClusterClient? _client;
    private IAsyncStream<byte[]>? _stream;

    public override void Start(IDictionary<string, string> config)
    {
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
        if (config.TryGetValue(OrleansConnectorConfig.PublishTimeoutMs, out var publishTimeout))
            _publishTimeoutMs = int.Parse(publishTimeout);
        if (config.TryGetValue(OrleansConnectorConfig.Retries, out var retries))
            _retries = int.Parse(retries);

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
        _stream = streamProviderInstance.GetStream<byte[]>(StreamId.Create(_streamNamespace, streamGuid.ToString()));
    }

    public override void Stop()
    {
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

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0 || _stream == null)
            return;

        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0)
                continue;

            var lastException = default(Exception);
            for (var attempt = 0; attempt <= _retries; attempt++)
            {
                try
                {
                    using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    timeoutCts.CancelAfter(_publishTimeoutMs);

                    await _stream.OnNextAsync(record.Value);
                    lastException = null;
                    break;
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested && attempt < _retries)
                {
                    lastException = ex;
                    await Task.Delay(100 * (attempt + 1), cancellationToken);
                }
            }

            if (lastException != null)
            {
                throw new InvalidOperationException(
                    $"Failed to publish message to Orleans stream after {_retries + 1} attempts",
                    lastException);
            }
        }
    }

    private Guid ResolveStreamId()
    {
        if (!string.IsNullOrEmpty(_streamId) && Guid.TryParse(_streamId, out var parsed))
            return parsed;

        // Derive a deterministic GUID from the stream namespace
        var hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(_streamNamespace));
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
