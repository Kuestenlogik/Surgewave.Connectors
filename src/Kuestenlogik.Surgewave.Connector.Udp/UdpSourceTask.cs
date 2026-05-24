using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Udp;

/// <summary>
/// Task that receives UDP datagrams and produces them as source records.
/// </summary>
public sealed class UdpSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _topic = "";
    private UdpClient? _udpClient;
    private int _maxMessageSize;
    private bool _includeSourceInfo;
    private bool _multicastEnabled;
    private string? _multicastGroup;
    private CancellationTokenSource? _cts;
    private Task? _receiveTask;
    private readonly Channel<(byte[] Data, IPEndPoint Source)> _messageChannel =
        Channel.CreateBounded<(byte[], IPEndPoint)>(new BoundedChannelOptions(1000)
        {
            FullMode = BoundedChannelFullMode.DropOldest
        });
    private long _offset;

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[UdpConnectorConfig.Topic];

        var listenAddress = GetConfigOrDefault(config, UdpConnectorConfig.ListenAddress, UdpConnectorConfig.DefaultListenAddress);
        var listenPort = int.Parse(GetConfigOrDefault(config, UdpConnectorConfig.ListenPort, UdpConnectorConfig.DefaultListenPort.ToString()));
        _maxMessageSize = int.Parse(GetConfigOrDefault(config, UdpConnectorConfig.MaxMessageSize, UdpConnectorConfig.DefaultMaxMessageSize.ToString()));
        _includeSourceInfo = bool.Parse(GetConfigOrDefault(config, UdpConnectorConfig.IncludeSourceInfo, UdpConnectorConfig.DefaultIncludeSourceInfo.ToString()));
        _multicastEnabled = bool.Parse(GetConfigOrDefault(config, UdpConnectorConfig.MulticastEnabled, UdpConnectorConfig.DefaultMulticastEnabled.ToString()));
        _multicastGroup = config.TryGetValue(UdpConnectorConfig.MulticastGroup, out var mg) ? mg : null;
        var receiveBufferSize = int.Parse(GetConfigOrDefault(config, UdpConnectorConfig.ReceiveBufferSize, UdpConnectorConfig.DefaultReceiveBufferSize.ToString()));

        // Create and configure UDP client
        var localEndpoint = new IPEndPoint(IPAddress.Parse(listenAddress), listenPort);
        _udpClient = new UdpClient();
        _udpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        _udpClient.Client.ReceiveBufferSize = receiveBufferSize;
        _udpClient.Client.Bind(localEndpoint);

        // Join multicast group if enabled
        if (_multicastEnabled && !string.IsNullOrEmpty(_multicastGroup))
        {
            var multicastAddress = IPAddress.Parse(_multicastGroup);
            _udpClient.JoinMulticastGroup(multicastAddress);
        }

        _cts = new CancellationTokenSource();
        _receiveTask = ReceiveLoopAsync(_cts.Token);
    }

    public override void Stop()
    {
        _cts?.Cancel();

        if (_multicastEnabled && !string.IsNullOrEmpty(_multicastGroup) && _udpClient != null)
        {
            try
            {
                _udpClient.DropMulticastGroup(IPAddress.Parse(_multicastGroup));
            }
            catch
            {
                // Ignore errors on shutdown
            }
        }

        try { _udpClient?.Close(); } catch { /* ignore */ }
        try { _udpClient?.Dispose(); } catch { /* ignore */ }
        _udpClient = null;

        try
        {
            _receiveTask?.Wait(TimeSpan.FromSeconds(5));
        }
        catch
        {
            // Ignore timeout
        }

        try { _cts?.Dispose(); } catch { /* ignore */ }
        _cts = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
            _cts?.Dispose();
            _cts = null;
            try { _udpClient?.Dispose(); } catch { /* ignore */ }
            _udpClient = null;
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        // Collect available messages
        while (_messageChannel.Reader.TryRead(out var message))
        {
            var headers = new Dictionary<string, byte[]>();

            if (_includeSourceInfo)
            {
                headers["udp_source_ip"] = Encoding.UTF8.GetBytes(message.Source.Address.ToString());
                headers["udp_source_port"] = Encoding.UTF8.GetBytes(message.Source.Port.ToString());
            }

            var offset = Interlocked.Increment(ref _offset);
            records.Add(new SourceRecord
            {
                Topic = _topic,
                Partition = 0,
                SourcePartition = new Dictionary<string, object> { ["endpoint"] = $"{message.Source}" },
                SourceOffset = new Dictionary<string, object> { ["offset"] = offset },
                Key = null,
                Value = message.Data,
                Headers = headers.Count > 0 ? headers : null
            });

            if (records.Count >= 1000)
                break;
        }

        // If no messages, wait briefly for one
        if (records.Count == 0)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(100);

                if (await _messageChannel.Reader.WaitToReadAsync(cts.Token))
                {
                    if (_messageChannel.Reader.TryRead(out var message))
                    {
                        var headers = new Dictionary<string, byte[]>();

                        if (_includeSourceInfo)
                        {
                            headers["udp_source_ip"] = Encoding.UTF8.GetBytes(message.Source.Address.ToString());
                            headers["udp_source_port"] = Encoding.UTF8.GetBytes(message.Source.Port.ToString());
                        }

                        var offset = Interlocked.Increment(ref _offset);
                        records.Add(new SourceRecord
                        {
                            Topic = _topic,
                            Partition = 0,
                            SourcePartition = new Dictionary<string, object> { ["endpoint"] = $"{message.Source}" },
                            SourceOffset = new Dictionary<string, object> { ["offset"] = offset },
                            Key = null,
                            Value = message.Data,
                            Headers = headers.Count > 0 ? headers : null
                        });
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Normal timeout
            }
        }

        return records;
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && _udpClient != null)
        {
            try
            {
                var result = await _udpClient.ReceiveAsync(cancellationToken);

                if (result.Buffer.Length > 0)
                {
                    // Make a copy of the data
                    var data = new byte[result.Buffer.Length];
                    Buffer.BlockCopy(result.Buffer, 0, data, 0, result.Buffer.Length);

                    await _messageChannel.Writer.WriteAsync((data, result.RemoteEndPoint), cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.Interrupted)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(ex);
            }
        }
    }

    private static string GetConfigOrDefault(IDictionary<string, string> config, string key, string defaultValue)
    {
        return config.TryGetValue(key, out var value) ? value : defaultValue;
    }
}
