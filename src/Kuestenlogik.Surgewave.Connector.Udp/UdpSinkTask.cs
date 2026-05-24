using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Udp;

/// <summary>
/// Task that sends records as UDP datagrams.
/// </summary>
public sealed class UdpSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private UdpClient? _udpClient;
    private IPEndPoint? _remoteEndpoint;
    private bool _multicastEnabled;

    public override void Start(IDictionary<string, string> config)
    {
        var host = config[UdpConnectorConfig.Host];
        var port = int.Parse(config[UdpConnectorConfig.Port]);
        _multicastEnabled = bool.Parse(GetConfigOrDefault(config, UdpConnectorConfig.MulticastEnabled, UdpConnectorConfig.DefaultMulticastEnabled.ToString()));
        var multicastTtl = int.Parse(GetConfigOrDefault(config, UdpConnectorConfig.MulticastTtl, UdpConnectorConfig.DefaultMulticastTtl.ToString()));
        var multicastLoopback = bool.Parse(GetConfigOrDefault(config, UdpConnectorConfig.MulticastLoopback, UdpConnectorConfig.DefaultMulticastLoopback.ToString()));
        var sendBufferSize = int.Parse(GetConfigOrDefault(config, UdpConnectorConfig.SendBufferSize, UdpConnectorConfig.DefaultSendBufferSize.ToString()));

        // Parse destination
        var hostAddress = Dns.GetHostAddresses(host).FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork)
                          ?? IPAddress.Parse(host);
        _remoteEndpoint = new IPEndPoint(hostAddress, port);

        // Create UDP client
        _udpClient = new UdpClient();
        _udpClient.Client.SendBufferSize = sendBufferSize;

        // Configure multicast if enabled
        if (_multicastEnabled)
        {
            _udpClient.Ttl = (short)multicastTtl;
            _udpClient.MulticastLoopback = multicastLoopback;
        }

        // Connect to remote endpoint for efficiency
        _udpClient.Connect(_remoteEndpoint);
    }

    public override void Stop()
    {
        try { _udpClient?.Close(); } catch { /* ignore */ }
        try { _udpClient?.Dispose(); } catch { /* ignore */ }
        _udpClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
            try { _udpClient?.Dispose(); } catch { /* ignore */ }
            _udpClient = null;
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0 || _udpClient == null)
            return;

        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0)
                continue;

            try
            {
                await _udpClient.SendAsync(record.Value, cancellationToken);
            }
            catch (SocketException ex)
            {
                Context.RaiseError?.Invoke(ex);
                throw;
            }
        }
    }

    private static string GetConfigOrDefault(IDictionary<string, string> config, string key, string defaultValue)
    {
        return config.TryGetValue(key, out var value) ? value : defaultValue;
    }
}
