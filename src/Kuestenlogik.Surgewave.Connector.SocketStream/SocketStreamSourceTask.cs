using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.SocketStream;

/// <summary>
/// Source task that connects to a TCP or Unix socket server and consumes a continuous message stream.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
[SuppressMessage("Security", "CA5359:Do Not Disable Certificate Validation", Justification = "TLS validation is intentionally configurable")]
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "NetworkStream ownership managed by connection lifecycle")]
public sealed class SocketStreamSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _socketType = SocketStreamConnectorConfig.DefaultSocketType;
    private string _host = SocketStreamConnectorConfig.DefaultHost;
    private int _port = SocketStreamConnectorConfig.DefaultPort;
    private string _unixSocketPath = "";
    private string _topic = "";
    private string _framing = SocketStreamConnectorConfig.DefaultFraming;
    private byte[] _delimiter = [(byte)'\n'];
    private int _lengthPrefixBytes = SocketStreamConnectorConfig.DefaultLengthPrefixBytes;
    private bool _lengthPrefixBigEndian = SocketStreamConnectorConfig.DefaultLengthPrefixBigEndian;
    private int _maxMessageSize = SocketStreamConnectorConfig.DefaultMaxMessageSize;
    private int _connectTimeoutMs = SocketStreamConnectorConfig.DefaultConnectTimeoutMs;
    private int _readTimeoutMs = SocketStreamConnectorConfig.DefaultReadTimeoutMs;
    private bool _reconnectEnabled = SocketStreamConnectorConfig.DefaultReconnectEnabled;
    private int _reconnectDelayMs = SocketStreamConnectorConfig.DefaultReconnectDelayMs;
    private int _reconnectMaxDelayMs = SocketStreamConnectorConfig.DefaultReconnectMaxDelayMs;
    private int _reconnectMaxAttempts = SocketStreamConnectorConfig.DefaultReconnectMaxAttempts;
    private int _receiveBufferSize = SocketStreamConnectorConfig.DefaultReceiveBufferSize;
    private bool _tlsEnabled;
    private string _tlsServerName = "";
    private bool _tlsValidateCertificate = true;
    private string _tlsClientCertPath = "";
    private string _tlsClientCertPassword = "";

    private Socket? _socket;
    private Stream? _stream;
    private CancellationTokenSource? _cts;
    private Task? _readTask;
    private readonly ConcurrentQueue<SourceRecord> _pendingRecords = new();
    private long _messageCounter;
    private int _reconnectAttempts;

    public override void Start(IDictionary<string, string> config)
    {
        ParseConfig(config);

        _cts = new CancellationTokenSource();
        _readTask = Task.Run(() => ReadLoopAsync(_cts.Token));
    }

    private void ParseConfig(IDictionary<string, string> config)
    {
        _socketType = config.TryGetValue(SocketStreamConnectorConfig.SocketType, out var st)
            ? st : SocketStreamConnectorConfig.DefaultSocketType;
        _host = config.TryGetValue(SocketStreamConnectorConfig.Host, out var h)
            ? h : SocketStreamConnectorConfig.DefaultHost;
        _port = config.TryGetValue(SocketStreamConnectorConfig.Port, out var p) && int.TryParse(p, out var port)
            ? port : SocketStreamConnectorConfig.DefaultPort;
        _unixSocketPath = config.TryGetValue(SocketStreamConnectorConfig.UnixSocketPath, out var usp)
            ? usp : "";
        _topic = config[SocketStreamConnectorConfig.Topic];

        _framing = config.TryGetValue(SocketStreamConnectorConfig.Framing, out var f)
            ? f : SocketStreamConnectorConfig.DefaultFraming;
        var delimiterStr = config.TryGetValue(SocketStreamConnectorConfig.Delimiter, out var d)
            ? d : SocketStreamConnectorConfig.DefaultDelimiter;
        _delimiter = ParseDelimiter(delimiterStr);
        _lengthPrefixBytes = config.TryGetValue(SocketStreamConnectorConfig.LengthPrefixBytes, out var lpb) && int.TryParse(lpb, out var bytes)
            ? bytes : SocketStreamConnectorConfig.DefaultLengthPrefixBytes;
        _lengthPrefixBigEndian = !config.TryGetValue(SocketStreamConnectorConfig.LengthPrefixBigEndian, out var lbe) ||
            !bool.TryParse(lbe, out var bigEndian) || bigEndian;
        _maxMessageSize = config.TryGetValue(SocketStreamConnectorConfig.MaxMessageSize, out var mms) && int.TryParse(mms, out var maxSize)
            ? maxSize : SocketStreamConnectorConfig.DefaultMaxMessageSize;

        _connectTimeoutMs = config.TryGetValue(SocketStreamConnectorConfig.ConnectTimeoutMs, out var ctm) && int.TryParse(ctm, out var connectTimeout)
            ? connectTimeout : SocketStreamConnectorConfig.DefaultConnectTimeoutMs;
        _readTimeoutMs = config.TryGetValue(SocketStreamConnectorConfig.ReadTimeoutMs, out var rtm) && int.TryParse(rtm, out var readTimeout)
            ? readTimeout : SocketStreamConnectorConfig.DefaultReadTimeoutMs;
        _reconnectEnabled = !config.TryGetValue(SocketStreamConnectorConfig.ReconnectEnabled, out var re) ||
            !bool.TryParse(re, out var reconnect) || reconnect;
        _reconnectDelayMs = config.TryGetValue(SocketStreamConnectorConfig.ReconnectDelayMs, out var rdm) && int.TryParse(rdm, out var reconnectDelay)
            ? reconnectDelay : SocketStreamConnectorConfig.DefaultReconnectDelayMs;
        _reconnectMaxDelayMs = config.TryGetValue(SocketStreamConnectorConfig.ReconnectMaxDelayMs, out var rmdm) && int.TryParse(rmdm, out var reconnectMaxDelay)
            ? reconnectMaxDelay : SocketStreamConnectorConfig.DefaultReconnectMaxDelayMs;
        _reconnectMaxAttempts = config.TryGetValue(SocketStreamConnectorConfig.ReconnectMaxAttempts, out var rma) && int.TryParse(rma, out var reconnectMaxAttempts)
            ? reconnectMaxAttempts : SocketStreamConnectorConfig.DefaultReconnectMaxAttempts;
        _receiveBufferSize = config.TryGetValue(SocketStreamConnectorConfig.ReceiveBufferSize, out var rbs) && int.TryParse(rbs, out var receiveBuffer)
            ? receiveBuffer : SocketStreamConnectorConfig.DefaultReceiveBufferSize;

        _tlsEnabled = config.TryGetValue(SocketStreamConnectorConfig.TlsEnabled, out var te) && bool.TryParse(te, out var tlsEnabled) && tlsEnabled;
        _tlsServerName = config.TryGetValue(SocketStreamConnectorConfig.TlsServerName, out var tsn) ? tsn : "";
        _tlsValidateCertificate = !config.TryGetValue(SocketStreamConnectorConfig.TlsValidateCertificate, out var tvc) ||
            !bool.TryParse(tvc, out var validateCert) || validateCert;
        _tlsClientCertPath = config.TryGetValue(SocketStreamConnectorConfig.TlsClientCertPath, out var tccp) ? tccp : "";
        _tlsClientCertPassword = config.TryGetValue(SocketStreamConnectorConfig.TlsClientCertPassword, out var tcpw) ? tcpw : "";
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

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await ConnectAsync(cancellationToken);
                _reconnectAttempts = 0;

                await ReadMessagesAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception)
            {
                CloseConnection();

                if (!_reconnectEnabled || cancellationToken.IsCancellationRequested)
                    break;

                if (_reconnectMaxAttempts >= 0 && _reconnectAttempts >= _reconnectMaxAttempts)
                    break;

                var delay = Math.Min(_reconnectDelayMs * (1 << _reconnectAttempts), _reconnectMaxDelayMs);
                _reconnectAttempts++;

                try
                {
                    await Task.Delay(delay, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    private async Task ConnectAsync(CancellationToken cancellationToken)
    {
        if (_socketType == SocketStreamConnectorConfig.SocketTypeUnix)
        {
            _socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            var endpoint = new UnixDomainSocketEndPoint(_unixSocketPath);

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_connectTimeoutMs);
            await _socket.ConnectAsync(endpoint, cts.Token);

            _stream = new NetworkStream(_socket, ownsSocket: false);
        }
        else
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true,
                ReceiveBufferSize = _receiveBufferSize
            };

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_connectTimeoutMs);
            await _socket.ConnectAsync(_host, _port, cts.Token);

            Stream networkStream = new NetworkStream(_socket, ownsSocket: false);

            if (_tlsEnabled)
            {
                var sslStream = new SslStream(networkStream, leaveInnerStreamOpen: false,
                    _tlsValidateCertificate ? null : (_, _, _, _) => true);

                var sslOptions = new SslClientAuthenticationOptions
                {
                    TargetHost = string.IsNullOrEmpty(_tlsServerName) ? _host : _tlsServerName
                };

                if (!string.IsNullOrEmpty(_tlsClientCertPath))
                {
                    var cert = X509CertificateLoader.LoadPkcs12FromFile(_tlsClientCertPath, _tlsClientCertPassword);
                    sslOptions.ClientCertificates = [cert];
                }

                await sslStream.AuthenticateAsClientAsync(sslOptions, cancellationToken);
                _stream = sslStream;
            }
            else
            {
                _stream = networkStream;
            }
        }

        if (_readTimeoutMs > 0)
        {
            _stream.ReadTimeout = _readTimeoutMs;
        }
    }

    private async Task ReadMessagesAsync(CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_receiveBufferSize);
        var messageBuffer = new MemoryStream();

        try
        {
            while (!cancellationToken.IsCancellationRequested && _stream != null)
            {
                var bytesRead = await _stream.ReadAsync(buffer.AsMemory(), cancellationToken);
                if (bytesRead == 0)
                {
                    // Connection closed
                    throw new IOException("Connection closed by remote host");
                }

                ProcessBytes(buffer.AsSpan(0, bytesRead), messageBuffer);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            messageBuffer.Dispose();
        }
    }

    private void ProcessBytes(ReadOnlySpan<byte> data, MemoryStream messageBuffer)
    {
        switch (_framing)
        {
            case SocketStreamConnectorConfig.FramingLine:
                ProcessLineFraming(data, messageBuffer);
                break;
            case SocketStreamConnectorConfig.FramingDelimiter:
                ProcessDelimiterFraming(data, messageBuffer);
                break;
            case SocketStreamConnectorConfig.FramingLengthPrefix:
                ProcessLengthPrefixFraming(data, messageBuffer);
                break;
            case SocketStreamConnectorConfig.FramingRaw:
                EmitRecord(data.ToArray());
                break;
        }
    }

    private void ProcessLineFraming(ReadOnlySpan<byte> data, MemoryStream messageBuffer)
    {
        foreach (var b in data)
        {
            if (b == '\n')
            {
                if (messageBuffer.Length > 0)
                {
                    var message = messageBuffer.ToArray();
                    // Trim trailing \r if present
                    if (message.Length > 0 && message[^1] == '\r')
                    {
                        message = message[..^1];
                    }
                    EmitRecord(message);
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

    private void ProcessDelimiterFraming(ReadOnlySpan<byte> data, MemoryStream messageBuffer)
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
                    // Found complete delimiter
                    var fullMessage = messageBuffer.ToArray();
                    var message = fullMessage[..^_delimiter.Length];
                    if (message.Length > 0)
                    {
                        EmitRecord(message);
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
                // Message too large, discard and reset
                messageBuffer.SetLength(0);
                delimiterIndex = 0;
            }
        }
    }

    private int _pendingLength = -1;
    private void ProcessLengthPrefixFraming(ReadOnlySpan<byte> data, MemoryStream messageBuffer)
    {
        var offset = 0;

        while (offset < data.Length)
        {
            if (_pendingLength < 0)
            {
                // Need to read length prefix
                var bytesNeeded = _lengthPrefixBytes - (int)messageBuffer.Length;
                var bytesAvailable = Math.Min(bytesNeeded, data.Length - offset);

                messageBuffer.Write(data.Slice(offset, bytesAvailable));
                offset += bytesAvailable;

                if (messageBuffer.Length == _lengthPrefixBytes)
                {
                    var lengthBytes = messageBuffer.ToArray();
                    _pendingLength = ReadLength(lengthBytes);
                    messageBuffer.SetLength(0);

                    if (_pendingLength > _maxMessageSize || _pendingLength < 0)
                    {
                        throw new InvalidDataException($"Message length {_pendingLength} exceeds maximum {_maxMessageSize}");
                    }
                }
            }
            else
            {
                // Reading message body
                var bytesNeeded = _pendingLength - (int)messageBuffer.Length;
                var bytesAvailable = Math.Min(bytesNeeded, data.Length - offset);

                messageBuffer.Write(data.Slice(offset, bytesAvailable));
                offset += bytesAvailable;

                if (messageBuffer.Length == _pendingLength)
                {
                    EmitRecord(messageBuffer.ToArray());
                    messageBuffer.SetLength(0);
                    _pendingLength = -1;
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

    private void EmitRecord(byte[] data)
    {
        var messageId = Interlocked.Increment(ref _messageCounter);
        var endpoint = _socketType == SocketStreamConnectorConfig.SocketTypeUnix
            ? _unixSocketPath
            : $"{_host}:{_port}";

        var record = new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["endpoint"] = endpoint },
            SourceOffset = new Dictionary<string, object> { ["message_id"] = messageId },
            Topic = _topic,
            Value = data,
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["socket_type"] = Encoding.UTF8.GetBytes(_socketType),
                ["framing"] = Encoding.UTF8.GetBytes(_framing)
            }
        };

        _pendingRecords.Enqueue(record);
    }

    private void CloseConnection()
    {
        try
        {
            _stream?.Dispose();
        }
        catch { }
        _stream = null;

        try
        {
            _socket?.Dispose();
        }
        catch { }
        _socket = null;
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
            _readTask?.Wait(TimeSpan.FromSeconds(5));
        }
        catch { }

        CloseConnection();
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
}
