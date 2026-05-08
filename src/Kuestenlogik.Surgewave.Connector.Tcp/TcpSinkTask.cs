using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Tcp;

/// <summary>
/// Task that connects to a TCP endpoint and sends records.
/// Supports automatic reconnection and configurable framing.
/// </summary>
public sealed class TcpSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _host = "";
    private int _port;
    private string _framing = TcpConnectorConfig.DefaultFraming;
    private byte[] _delimiter = Encoding.UTF8.GetBytes(TcpConnectorConfig.DefaultDelimiter);
    private int _lengthPrefixBytes = TcpConnectorConfig.DefaultLengthPrefixBytes;
    private bool _lengthPrefixBigEndian = TcpConnectorConfig.DefaultLengthPrefixBigEndian;
    private int _maxMessageSize = TcpConnectorConfig.DefaultMaxMessageSize;
    private bool _useTls;
    private bool _validateCertificate = TcpConnectorConfig.DefaultTlsValidateCertificate;
    private bool _reconnect = TcpConnectorConfig.DefaultReconnect;
    private int _reconnectDelayMs = TcpConnectorConfig.DefaultReconnectDelayMs;
    private int _reconnectMaxDelayMs = TcpConnectorConfig.DefaultReconnectMaxDelayMs;
    private int _sendBufferSize = TcpConnectorConfig.DefaultSendBufferSize;
    private int _connectionTimeoutMs = TcpConnectorConfig.DefaultConnectionTimeout;

    private TcpClient? _client;
    private Stream? _stream;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private int _currentReconnectDelay;
    private bool _isConnecting;

    public override void Start(IDictionary<string, string> config)
    {
        _host = config[TcpConnectorConfig.Host];
        _port = int.Parse(config[TcpConnectorConfig.Port]);

        if (config.TryGetValue(TcpConnectorConfig.Framing, out var framing))
            _framing = framing;
        if (config.TryGetValue(TcpConnectorConfig.Delimiter, out var delim))
            _delimiter = Encoding.UTF8.GetBytes(delim.Replace("\\n", "\n").Replace("\\r", "\r").Replace("\\t", "\t"));
        if (config.TryGetValue(TcpConnectorConfig.LengthPrefixBytes, out var lpb))
            _lengthPrefixBytes = int.Parse(lpb);
        if (config.TryGetValue(TcpConnectorConfig.LengthPrefixBigEndian, out var lpbe))
            _lengthPrefixBigEndian = bool.Parse(lpbe);
        if (config.TryGetValue(TcpConnectorConfig.MaxMessageSize, out var maxMsg))
            _maxMessageSize = int.Parse(maxMsg);
        if (config.TryGetValue(TcpConnectorConfig.UseTls, out var useTls))
            _useTls = bool.Parse(useTls);
        if (config.TryGetValue(TcpConnectorConfig.TlsValidateCertificate, out var validateCert))
            _validateCertificate = bool.Parse(validateCert);
        if (config.TryGetValue(TcpConnectorConfig.Reconnect, out var reconnect))
            _reconnect = bool.Parse(reconnect);
        if (config.TryGetValue(TcpConnectorConfig.ReconnectDelayMs, out var reconnectDelay))
            _reconnectDelayMs = int.Parse(reconnectDelay);
        if (config.TryGetValue(TcpConnectorConfig.ReconnectMaxDelayMs, out var reconnectMaxDelay))
            _reconnectMaxDelayMs = int.Parse(reconnectMaxDelay);
        if (config.TryGetValue(TcpConnectorConfig.SendBufferSize, out var sendBuffer))
            _sendBufferSize = int.Parse(sendBuffer);
        if (config.TryGetValue(TcpConnectorConfig.ConnectionTimeout, out var timeout))
            _connectionTimeoutMs = int.Parse(timeout);

        _currentReconnectDelay = _reconnectDelayMs;
    }

    public override void Stop()
    {
        try { _stream?.Dispose(); } catch { /* ignore */ }
        try { _client?.Dispose(); } catch { /* ignore */ }
        _stream = null;
        _client = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            try { _stream?.Dispose(); } catch { /* ignore */ }
            try { _client?.Dispose(); } catch { /* ignore */ }
            _connectionLock.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0)
            return;

        var stream = await EnsureConnectedAsync(cancellationToken);
        if (stream == null)
        {
            if (!_reconnect)
                throw new InvalidOperationException("Not connected to TCP endpoint and reconnection is disabled");
            return; // Will retry on next call
        }

        try
        {
            foreach (var record in records)
            {
                if (record.Value == null || record.Value.Length == 0)
                    continue;

                if (record.Value.Length > _maxMessageSize)
                    continue; // Skip oversized messages

                await WriteFramedMessageAsync(stream, record.Value, cancellationToken);
            }

            await stream.FlushAsync(cancellationToken);
            _currentReconnectDelay = _reconnectDelayMs; // Reset on success
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            Disconnect();
            throw;
        }
    }

    private async Task WriteFramedMessageAsync(Stream stream, byte[] data, CancellationToken ct)
    {
        switch (_framing)
        {
            case TcpConnectorConfig.FramingLine:
                await stream.WriteAsync(data, ct);
                await stream.WriteAsync("\n"u8.ToArray(), ct);
                break;

            case TcpConnectorConfig.FramingLengthPrefix:
                var lengthBytes = WriteLengthPrefix(data.Length);
                await stream.WriteAsync(lengthBytes, ct);
                await stream.WriteAsync(data, ct);
                break;

            case TcpConnectorConfig.FramingDelimiter:
                await stream.WriteAsync(data, ct);
                await stream.WriteAsync(_delimiter, ct);
                break;

            default:
                // Default to line framing
                await stream.WriteAsync(data, ct);
                await stream.WriteAsync("\n"u8.ToArray(), ct);
                break;
        }
    }

    private byte[] WriteLengthPrefix(int length)
    {
        return _lengthPrefixBytes switch
        {
            1 => [(byte)length],
            2 => _lengthPrefixBigEndian
                ? [(byte)(length >> 8), (byte)length]
                : [(byte)length, (byte)(length >> 8)],
            4 => _lengthPrefixBigEndian
                ? [(byte)(length >> 24), (byte)(length >> 16), (byte)(length >> 8), (byte)length]
                : [(byte)length, (byte)(length >> 8), (byte)(length >> 16), (byte)(length >> 24)],
            _ => [(byte)(length >> 24), (byte)(length >> 16), (byte)(length >> 8), (byte)length]
        };
    }

    private async Task<Stream?> EnsureConnectedAsync(CancellationToken ct)
    {
        if (_stream != null && _client?.Connected == true)
            return _stream;

        await _connectionLock.WaitAsync(ct);
        try
        {
            // Double-check after acquiring lock
            if (_stream != null && _client?.Connected == true)
                return _stream;

            if (_isConnecting)
                return null;

            _isConnecting = true;
            Disconnect();

            try
            {
                _client = new TcpClient
                {
                    SendBufferSize = _sendBufferSize,
                    NoDelay = true
                };

                using var timeoutCts = new CancellationTokenSource(_connectionTimeoutMs);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

                await _client.ConnectAsync(_host, _port, linkedCts.Token);

                Stream stream = _client.GetStream();

                if (_useTls)
                {
#pragma warning disable CA5359 // Certificate validation intentionally disabled when configured
                    var sslStream = new SslStream(
                        stream,
                        false,
                        _validateCertificate ? null : (sender, cert, chain, errors) => true);
#pragma warning restore CA5359

                    await sslStream.AuthenticateAsClientAsync(_host);
                    stream = sslStream;
                }

                _stream = stream;
                _currentReconnectDelay = _reconnectDelayMs;
                return _stream;
            }
            catch (Exception)
            {
                Disconnect();

                if (_reconnect)
                {
                    await Task.Delay(_currentReconnectDelay, ct);
                    _currentReconnectDelay = Math.Min(_currentReconnectDelay * 2, _reconnectMaxDelayMs);
                }

                throw;
            }
            finally
            {
                _isConnecting = false;
            }
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private void Disconnect()
    {
        try { _stream?.Dispose(); } catch { /* ignore */ }
        try { _client?.Dispose(); } catch { /* ignore */ }
        _stream = null;
        _client = null;
    }
}
