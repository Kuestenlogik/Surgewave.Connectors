using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.SocketServer;

/// <summary>
/// Source task that listens on TCP, UDP, or Unix sockets and receives messages from multiple clients.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
[SuppressMessage("Security", "CA5359:Do Not Disable Certificate Validation", Justification = "TLS configuration is intentionally configurable")]
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "NetworkStream ownership managed by connection lifecycle")]
public sealed class SocketServerSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _protocol = SocketServerConnectorConfig.DefaultProtocol;
    private string _listenAddress = SocketServerConnectorConfig.DefaultListenAddress;
    private int _listenPort = SocketServerConnectorConfig.DefaultListenPort;
    private string _unixSocketPath = "";
    private string _topic = "";
    private int _maxConnections = SocketServerConnectorConfig.DefaultMaxConnections;
    private int _idleTimeoutMs = SocketServerConnectorConfig.DefaultIdleTimeoutMs;
    private string _framing = SocketServerConnectorConfig.DefaultFraming;
    private byte[] _delimiter = [(byte)'\n'];
    private int _lengthPrefixBytes = SocketServerConnectorConfig.DefaultLengthPrefixBytes;
    private bool _lengthPrefixBigEndian = SocketServerConnectorConfig.DefaultLengthPrefixBigEndian;
    private int _maxMessageSize = SocketServerConnectorConfig.DefaultMaxMessageSize;
    private int _receiveBufferSize = SocketServerConnectorConfig.DefaultReceiveBufferSize;
    private bool _tlsEnabled;
    private X509Certificate2? _tlsCertificate;
    private bool _tlsRequireClientCert;
    private bool _udpMulticastEnabled;
    private string _udpMulticastGroup = "";
    private bool _includeClientInfo = SocketServerConnectorConfig.DefaultIncludeClientInfo;
    private bool _udpIncludeSourceInfo = SocketServerConnectorConfig.DefaultUdpIncludeSourceInfo;

    private Socket? _listener;
    private UdpClient? _udpClient;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;
    private readonly ConcurrentDictionary<string, ClientConnection> _connections = new();
    private readonly ConcurrentQueue<SourceRecord> _pendingRecords = new();
    private long _messageCounter;
    private long _connectionCounter;

    public override void Start(IDictionary<string, string> config)
    {
        ParseConfig(config);

        _cts = new CancellationTokenSource();

        switch (_protocol)
        {
            case SocketServerConnectorConfig.ProtocolTcp:
                StartTcpServer();
                break;
            case SocketServerConnectorConfig.ProtocolUdp:
                StartUdpServer();
                break;
            case SocketServerConnectorConfig.ProtocolUnix:
                StartUnixServer();
                break;
        }
    }

    private void ParseConfig(IDictionary<string, string> config)
    {
        _protocol = config.TryGetValue(SocketServerConnectorConfig.Protocol, out var p)
            ? p : SocketServerConnectorConfig.DefaultProtocol;
        _listenAddress = config.TryGetValue(SocketServerConnectorConfig.ListenAddress, out var la)
            ? la : SocketServerConnectorConfig.DefaultListenAddress;
        _listenPort = config.TryGetValue(SocketServerConnectorConfig.ListenPort, out var lp) && int.TryParse(lp, out var port)
            ? port : SocketServerConnectorConfig.DefaultListenPort;
        _unixSocketPath = config.TryGetValue(SocketServerConnectorConfig.UnixSocketPath, out var usp)
            ? usp : "";
        _topic = config[SocketServerConnectorConfig.Topic];

        _maxConnections = config.TryGetValue(SocketServerConnectorConfig.MaxConnections, out var mc) && int.TryParse(mc, out var maxConn)
            ? maxConn : SocketServerConnectorConfig.DefaultMaxConnections;
        _idleTimeoutMs = config.TryGetValue(SocketServerConnectorConfig.IdleTimeoutMs, out var itm) && int.TryParse(itm, out var idleTimeout)
            ? idleTimeout : SocketServerConnectorConfig.DefaultIdleTimeoutMs;

        _framing = config.TryGetValue(SocketServerConnectorConfig.Framing, out var f)
            ? f : SocketServerConnectorConfig.DefaultFraming;
        var delimiterStr = config.TryGetValue(SocketServerConnectorConfig.Delimiter, out var d)
            ? d : SocketServerConnectorConfig.DefaultDelimiter;
        _delimiter = ParseDelimiter(delimiterStr);
        _lengthPrefixBytes = config.TryGetValue(SocketServerConnectorConfig.LengthPrefixBytes, out var lpb) && int.TryParse(lpb, out var bytes)
            ? bytes : SocketServerConnectorConfig.DefaultLengthPrefixBytes;
        _lengthPrefixBigEndian = !config.TryGetValue(SocketServerConnectorConfig.LengthPrefixBigEndian, out var lbe) ||
            !bool.TryParse(lbe, out var bigEndian) || bigEndian;
        _maxMessageSize = config.TryGetValue(SocketServerConnectorConfig.MaxMessageSize, out var mms) && int.TryParse(mms, out var maxSize)
            ? maxSize : SocketServerConnectorConfig.DefaultMaxMessageSize;
        _receiveBufferSize = config.TryGetValue(SocketServerConnectorConfig.ReceiveBufferSize, out var rbs) && int.TryParse(rbs, out var receiveBuffer)
            ? receiveBuffer : SocketServerConnectorConfig.DefaultReceiveBufferSize;

        _tlsEnabled = config.TryGetValue(SocketServerConnectorConfig.TlsEnabled, out var te) && bool.TryParse(te, out var tlsEnabled) && tlsEnabled;
        if (_tlsEnabled && config.TryGetValue(SocketServerConnectorConfig.TlsCertificatePath, out var certPath) && !string.IsNullOrEmpty(certPath))
        {
            var certPassword = config.TryGetValue(SocketServerConnectorConfig.TlsCertificatePassword, out var cp) ? cp : "";
            _tlsCertificate = X509CertificateLoader.LoadPkcs12FromFile(certPath, certPassword);
        }
        _tlsRequireClientCert = config.TryGetValue(SocketServerConnectorConfig.TlsRequireClientCert, out var trc) &&
            bool.TryParse(trc, out var requireClient) && requireClient;

        _udpMulticastEnabled = config.TryGetValue(SocketServerConnectorConfig.UdpMulticastEnabled, out var ume) &&
            bool.TryParse(ume, out var multicast) && multicast;
        _udpMulticastGroup = config.TryGetValue(SocketServerConnectorConfig.UdpMulticastGroup, out var umg) ? umg : "";
        _includeClientInfo = !config.TryGetValue(SocketServerConnectorConfig.IncludeClientInfo, out var ici) ||
            !bool.TryParse(ici, out var includeInfo) || includeInfo;
        _udpIncludeSourceInfo = !config.TryGetValue(SocketServerConnectorConfig.UdpIncludeSourceInfo, out var usi) ||
            !bool.TryParse(usi, out var includeSource) || includeSource;
    }

    private static byte[] ParseDelimiter(string delimiter)
    {
        var result = new List<byte>();
        for (int i = 0; i < delimiter.Length; i++)
        {
            if (delimiter[i] == '\\' && i + 1 < delimiter.Length)
            {
                result.Add(delimiter[i + 1] switch
                {
                    'n' => (byte)'\n',
                    'r' => (byte)'\r',
                    't' => (byte)'\t',
                    '0' => 0,
                    '\\' => (byte)'\\',
                    _ => (byte)delimiter[i + 1]
                });
                i++;
            }
            else
            {
                result.Add((byte)delimiter[i]);
            }
        }
        return result.Count > 0 ? [.. result] : [(byte)'\n'];
    }

    private void StartTcpServer()
    {
        var endpoint = _listenAddress == "0.0.0.0"
            ? new IPEndPoint(IPAddress.Any, _listenPort)
            : new IPEndPoint(IPAddress.Parse(_listenAddress), _listenPort);

        _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
        {
            ReceiveBufferSize = _receiveBufferSize
        };
        _listener.Bind(endpoint);
        _listener.Listen(_maxConnections);

        _acceptTask = Task.Run(() => AcceptTcpConnectionsAsync(_cts!.Token));
    }

    private void StartUdpServer()
    {
        var endpoint = _listenAddress == "0.0.0.0"
            ? new IPEndPoint(IPAddress.Any, _listenPort)
            : new IPEndPoint(IPAddress.Parse(_listenAddress), _listenPort);

        _udpClient = new UdpClient
        {
            Client = { ReceiveBufferSize = _receiveBufferSize }
        };
        _udpClient.Client.Bind(endpoint);

        if (_udpMulticastEnabled && !string.IsNullOrEmpty(_udpMulticastGroup))
        {
            _udpClient.JoinMulticastGroup(IPAddress.Parse(_udpMulticastGroup));
        }

        _acceptTask = Task.Run(() => ReceiveUdpDatagramsAsync(_cts!.Token));
    }

    private void StartUnixServer()
    {
        // Remove existing socket file if present
        if (File.Exists(_unixSocketPath))
        {
            File.Delete(_unixSocketPath);
        }

        var endpoint = new UnixDomainSocketEndPoint(_unixSocketPath);

        _listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        _listener.Bind(endpoint);
        _listener.Listen(_maxConnections);

        _acceptTask = Task.Run(() => AcceptUnixConnectionsAsync(_cts!.Token));
    }

    private async Task AcceptTcpConnectionsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && _listener != null)
        {
            try
            {
                var clientSocket = await _listener.AcceptAsync(cancellationToken);

                if (_connections.Count >= _maxConnections)
                {
                    clientSocket.Dispose();
                    continue;
                }

                var connectionId = $"tcp-{Interlocked.Increment(ref _connectionCounter)}";
                var remoteEndpoint = clientSocket.RemoteEndPoint?.ToString() ?? "unknown";

                var connection = new ClientConnection(connectionId, remoteEndpoint, clientSocket);
                _connections[connectionId] = connection;

                // CA2025: connection disposal is handled in HandleTcpClientAsync's finally block
#pragma warning disable CA2025
                _ = Task.Run(() => HandleTcpClientAsync(connection, cancellationToken), cancellationToken);
#pragma warning restore CA2025
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch
            {
                // Continue accepting
            }
        }
    }

    private async Task HandleTcpClientAsync(ClientConnection connection, CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_receiveBufferSize);
        var messageBuffer = new MemoryStream();
        var pendingLength = -1;

        try
        {
            Stream stream = new NetworkStream(connection.Socket, ownsSocket: false);

            if (_tlsEnabled && _tlsCertificate != null)
            {
                var sslStream = new SslStream(stream, leaveInnerStreamOpen: false);
                await sslStream.AuthenticateAsServerAsync(_tlsCertificate, _tlsRequireClientCert, false);
                stream = sslStream;
            }

            if (_idleTimeoutMs > 0)
            {
                stream.ReadTimeout = _idleTimeoutMs;
            }

            while (!cancellationToken.IsCancellationRequested && connection.Socket.Connected)
            {
                var bytesRead = await stream.ReadAsync(buffer.AsMemory(), cancellationToken);
                if (bytesRead == 0)
                    break;

                ProcessBytes(buffer.AsSpan(0, bytesRead), messageBuffer, ref pendingLength, connection);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch
        {
            // Connection error
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            messageBuffer.Dispose();
            _connections.TryRemove(connection.ConnectionId, out _);
            connection.Dispose();
        }
    }

    private async Task AcceptUnixConnectionsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && _listener != null)
        {
            try
            {
                var clientSocket = await _listener.AcceptAsync(cancellationToken);

                if (_connections.Count >= _maxConnections)
                {
                    clientSocket.Dispose();
                    continue;
                }

                var connectionId = $"unix-{Interlocked.Increment(ref _connectionCounter)}";

                var connection = new ClientConnection(connectionId, _unixSocketPath, clientSocket);
                _connections[connectionId] = connection;

                // CA2025: connection disposal is handled in HandleUnixClientAsync's finally block
#pragma warning disable CA2025
                _ = Task.Run(() => HandleUnixClientAsync(connection, cancellationToken), cancellationToken);
#pragma warning restore CA2025
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch
            {
                // Continue accepting
            }
        }
    }

    private async Task HandleUnixClientAsync(ClientConnection connection, CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_receiveBufferSize);
        var messageBuffer = new MemoryStream();
        var pendingLength = -1;

        try
        {
            var stream = new NetworkStream(connection.Socket, ownsSocket: false);

            if (_idleTimeoutMs > 0)
            {
                stream.ReadTimeout = _idleTimeoutMs;
            }

            while (!cancellationToken.IsCancellationRequested && connection.Socket.Connected)
            {
                var bytesRead = await stream.ReadAsync(buffer.AsMemory(), cancellationToken);
                if (bytesRead == 0)
                    break;

                ProcessBytes(buffer.AsSpan(0, bytesRead), messageBuffer, ref pendingLength, connection);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch
        {
            // Connection error
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            messageBuffer.Dispose();
            _connections.TryRemove(connection.ConnectionId, out _);
            connection.Dispose();
        }
    }

    private async Task ReceiveUdpDatagramsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && _udpClient != null)
        {
            try
            {
                var result = await _udpClient.ReceiveAsync(cancellationToken);

                var messageId = Interlocked.Increment(ref _messageCounter);
                var sourceEndpoint = result.RemoteEndPoint.ToString();

                var headers = new Dictionary<string, byte[]>
                {
                    ["protocol"] = Encoding.UTF8.GetBytes("udp")
                };

                if (_udpIncludeSourceInfo)
                {
                    headers["source_ip"] = Encoding.UTF8.GetBytes(result.RemoteEndPoint.Address.ToString());
                    headers["source_port"] = Encoding.UTF8.GetBytes(result.RemoteEndPoint.Port.ToString());
                }

                var record = new SourceRecord
                {
                    SourcePartition = new Dictionary<string, object> { ["endpoint"] = $"{_listenAddress}:{_listenPort}" },
                    SourceOffset = new Dictionary<string, object> { ["message_id"] = messageId },
                    Topic = _topic,
                    Value = result.Buffer,
                    Timestamp = DateTimeOffset.UtcNow,
                    Headers = headers
                };

                _pendingRecords.Enqueue(record);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch
            {
                // Continue receiving
            }
        }
    }

    private void ProcessBytes(ReadOnlySpan<byte> data, MemoryStream messageBuffer, ref int pendingLength, ClientConnection connection)
    {
        switch (_framing)
        {
            case SocketServerConnectorConfig.FramingLine:
                ProcessLineFraming(data, messageBuffer, connection);
                break;
            case SocketServerConnectorConfig.FramingDelimiter:
                ProcessDelimiterFraming(data, messageBuffer, connection);
                break;
            case SocketServerConnectorConfig.FramingLengthPrefix:
                ProcessLengthPrefixFraming(data, messageBuffer, ref pendingLength, connection);
                break;
            case SocketServerConnectorConfig.FramingRaw:
                EmitRecord(data.ToArray(), connection);
                break;
        }
    }

    private void ProcessLineFraming(ReadOnlySpan<byte> data, MemoryStream messageBuffer, ClientConnection connection)
    {
        foreach (var b in data)
        {
            if (b == '\n')
            {
                if (messageBuffer.Length > 0)
                {
                    var message = messageBuffer.ToArray();
                    if (message.Length > 0 && message[^1] == '\r')
                    {
                        message = message[..^1];
                    }
                    EmitRecord(message, connection);
                    messageBuffer.SetLength(0);
                }
            }
            else
            {
                if (messageBuffer.Length < _maxMessageSize)
                {
                    messageBuffer.WriteByte(b);
                }
            }
        }
    }

    private void ProcessDelimiterFraming(ReadOnlySpan<byte> data, MemoryStream messageBuffer, ClientConnection connection)
    {
        var delimiterIndex = 0;

        foreach (var b in data)
        {
            messageBuffer.WriteByte(b);

            if (b == _delimiter[delimiterIndex])
            {
                delimiterIndex++;
                if (delimiterIndex == _delimiter.Length)
                {
                    var fullMessage = messageBuffer.ToArray();
                    var message = fullMessage[..^_delimiter.Length];
                    if (message.Length > 0)
                    {
                        EmitRecord(message, connection);
                    }
                    messageBuffer.SetLength(0);
                    delimiterIndex = 0;
                }
            }
            else
            {
                delimiterIndex = 0;
            }

            if (messageBuffer.Length > _maxMessageSize)
            {
                messageBuffer.SetLength(0);
                delimiterIndex = 0;
            }
        }
    }

    private void ProcessLengthPrefixFraming(ReadOnlySpan<byte> data, MemoryStream messageBuffer, ref int pendingLength, ClientConnection connection)
    {
        var offset = 0;

        while (offset < data.Length)
        {
            if (pendingLength < 0)
            {
                var bytesNeeded = _lengthPrefixBytes - (int)messageBuffer.Length;
                var bytesAvailable = Math.Min(bytesNeeded, data.Length - offset);

                messageBuffer.Write(data.Slice(offset, bytesAvailable));
                offset += bytesAvailable;

                if (messageBuffer.Length == _lengthPrefixBytes)
                {
                    var lengthBytes = messageBuffer.ToArray();
                    pendingLength = ReadLength(lengthBytes);
                    messageBuffer.SetLength(0);

                    if (pendingLength > _maxMessageSize || pendingLength < 0)
                    {
                        throw new InvalidDataException($"Message length {pendingLength} exceeds maximum {_maxMessageSize}");
                    }
                }
            }
            else
            {
                var bytesNeeded = pendingLength - (int)messageBuffer.Length;
                var bytesAvailable = Math.Min(bytesNeeded, data.Length - offset);

                messageBuffer.Write(data.Slice(offset, bytesAvailable));
                offset += bytesAvailable;

                if (messageBuffer.Length == pendingLength)
                {
                    EmitRecord(messageBuffer.ToArray(), connection);
                    messageBuffer.SetLength(0);
                    pendingLength = -1;
                }
            }
        }
    }

    private int ReadLength(byte[] bytes)
    {
        return _lengthPrefixBytes switch
        {
            1 => bytes[0],
            2 => _lengthPrefixBigEndian
                ? (bytes[0] << 8) | bytes[1]
                : bytes[0] | (bytes[1] << 8),
            4 => _lengthPrefixBigEndian
                ? (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3]
                : bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24),
            _ => throw new InvalidOperationException($"Invalid length prefix bytes: {_lengthPrefixBytes}")
        };
    }

    private void EmitRecord(byte[] data, ClientConnection connection)
    {
        var messageId = Interlocked.Increment(ref _messageCounter);

        var headers = new Dictionary<string, byte[]>
        {
            ["protocol"] = Encoding.UTF8.GetBytes(_protocol),
            ["framing"] = Encoding.UTF8.GetBytes(_framing)
        };

        if (_includeClientInfo)
        {
            headers["connection_id"] = Encoding.UTF8.GetBytes(connection.ConnectionId);
            headers["remote_endpoint"] = Encoding.UTF8.GetBytes(connection.RemoteEndpoint);
        }

        var endpoint = _protocol == SocketServerConnectorConfig.ProtocolUnix
            ? _unixSocketPath
            : $"{_listenAddress}:{_listenPort}";

        var record = new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["endpoint"] = endpoint,
                ["connection_id"] = connection.ConnectionId
            },
            SourceOffset = new Dictionary<string, object> { ["message_id"] = messageId },
            Topic = _topic,
            Value = data,
            Timestamp = DateTimeOffset.UtcNow,
            Headers = headers
        };

        _pendingRecords.Enqueue(record);
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        while (_pendingRecords.TryDequeue(out var record))
        {
            records.Add(record);
        }

        return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
    }

    public override void Stop()
    {
        _cts?.Cancel();

        try
        {
            _acceptTask?.Wait(TimeSpan.FromSeconds(5));
        }
        catch { }

        // Close all connections
        foreach (var (_, connection) in _connections)
        {
            connection.Dispose();
        }
        _connections.Clear();

        // Cleanup server
        _listener?.Dispose();
        _listener = null;

        if (_udpMulticastEnabled && !string.IsNullOrEmpty(_udpMulticastGroup) && _udpClient != null)
        {
            try
            {
                _udpClient.DropMulticastGroup(IPAddress.Parse(_udpMulticastGroup));
            }
            catch { }
        }
        _udpClient?.Dispose();
        _udpClient = null;

        // Cleanup Unix socket file
        if (_protocol == SocketServerConnectorConfig.ProtocolUnix && File.Exists(_unixSocketPath))
        {
            try
            {
                File.Delete(_unixSocketPath);
            }
            catch { }
        }

        _tlsCertificate?.Dispose();
        _tlsCertificate = null;

        _cts?.Dispose();
        _cts = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }

    private sealed class ClientConnection : IDisposable
    {
        public string ConnectionId { get; }
        public string RemoteEndpoint { get; }
        public Socket Socket { get; }

        public ClientConnection(string connectionId, string remoteEndpoint, Socket socket)
        {
            ConnectionId = connectionId;
            RemoteEndpoint = remoteEndpoint;
            Socket = socket;
        }

        public void Dispose()
        {
            try
            {
                Socket.Shutdown(SocketShutdown.Both);
            }
            catch { }
            Socket.Dispose();
        }
    }
}
