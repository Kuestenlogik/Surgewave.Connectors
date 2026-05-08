using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Tcp;

/// <summary>
/// Task that listens for TCP connections and produces received messages as records.
/// Supports multiple concurrent connections and configurable framing.
/// </summary>
public sealed class TcpSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _topic = "";
    private string _listenAddress = TcpConnectorConfig.DefaultListenAddress;
    private int _listenPort = TcpConnectorConfig.DefaultListenPort;
    private int _maxConnections = TcpConnectorConfig.DefaultMaxConnections;
    private string _framing = TcpConnectorConfig.DefaultFraming;
    private byte[] _delimiter = Encoding.UTF8.GetBytes(TcpConnectorConfig.DefaultDelimiter);
    private int _lengthPrefixBytes = TcpConnectorConfig.DefaultLengthPrefixBytes;
    private bool _lengthPrefixBigEndian = TcpConnectorConfig.DefaultLengthPrefixBigEndian;
    private int _maxMessageSize = TcpConnectorConfig.DefaultMaxMessageSize;
    private bool _useTls;
    private X509Certificate2? _certificate;

    private TcpListener? _listener;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;
    private readonly ConcurrentDictionary<string, TcpClientHandler> _clients = new();
    private readonly ConcurrentQueue<SourceRecord> _pendingRecords = new();
    private long _connectionCounter;

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[TcpConnectorConfig.Topic];

        if (config.TryGetValue(TcpConnectorConfig.ListenAddress, out var addr))
            _listenAddress = addr;
        if (config.TryGetValue(TcpConnectorConfig.ListenPort, out var port))
            _listenPort = int.Parse(port);
        if (config.TryGetValue(TcpConnectorConfig.MaxConnections, out var maxConn))
            _maxConnections = int.Parse(maxConn);
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

        if (_useTls && config.TryGetValue(TcpConnectorConfig.TlsCertificatePath, out var certPath) && !string.IsNullOrEmpty(certPath))
        {
            config.TryGetValue(TcpConnectorConfig.TlsCertificatePassword, out var password);
            _certificate = string.IsNullOrEmpty(password)
                ? X509CertificateLoader.LoadPkcs12FromFile(certPath, null)
                : X509CertificateLoader.LoadPkcs12FromFile(certPath, password);
        }

        _cts = new CancellationTokenSource();

        var endpoint = IPAddress.TryParse(_listenAddress, out var ip)
            ? new IPEndPoint(ip, _listenPort)
            : new IPEndPoint(IPAddress.Any, _listenPort);

        _listener = new TcpListener(endpoint);
        _listener.Start();

        _acceptTask = AcceptConnectionsAsync(_cts.Token);
    }

    public override void Stop()
    {
        _cts?.Cancel();

        foreach (var client in _clients.Values)
        {
            client.Dispose();
        }
        _clients.Clear();

        try { _listener?.Stop(); } catch { /* ignore */ }

        try { _acceptTask?.Wait(1000); } catch { /* ignore */ }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _cts?.Dispose();
            _certificate?.Dispose();
            try { _listener?.Dispose(); } catch { /* ignore */ }
        }
        base.Dispose(disposing);
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();
        var maxBatch = 1000;

        while (records.Count < maxBatch && _pendingRecords.TryDequeue(out var record))
        {
            records.Add(record);
        }

        return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
    }

    private async Task AcceptConnectionsAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && _listener != null)
        {
            try
            {
                var client = await _listener.AcceptTcpClientAsync(ct);

                if (_clients.Count >= _maxConnections)
                {
                    client.Dispose();
                    continue;
                }

                var connectionId = $"conn-{Interlocked.Increment(ref _connectionCounter)}";
                var handler = new TcpClientHandler(connectionId, client, this, ct);
                _clients[connectionId] = handler;

                _ = handler.StartAsync();
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SocketException)
            {
                // Listener stopped
                break;
            }
        }
    }

    internal void EnqueueRecord(string connectionId, byte[] data, long bytesReceived)
    {
        var sourcePartition = new Dictionary<string, object> { ["connection"] = connectionId };
        var sourceOffset = new Dictionary<string, object>
        {
            [TcpConnectorConfig.OffsetConnectionId] = connectionId,
            [TcpConnectorConfig.OffsetBytesReceived] = bytesReceived
        };

        var record = new SourceRecord
        {
            SourcePartition = sourcePartition,
            SourceOffset = sourceOffset,
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(connectionId),
            Value = data,
            Timestamp = DateTimeOffset.UtcNow
        };

        _pendingRecords.Enqueue(record);
    }

    internal void RemoveClient(string connectionId)
    {
        if (_clients.TryRemove(connectionId, out var handler))
        {
            handler.Dispose();
        }
    }

    internal async Task<Stream> WrapWithTlsAsync(NetworkStream stream, CancellationToken ct)
    {
        if (!_useTls || _certificate == null)
            return stream;

        var sslStream = new SslStream(stream, false);
        await sslStream.AuthenticateAsServerAsync(_certificate, false, false);
        return sslStream;
    }

    internal string Framing => _framing;
    internal byte[] Delimiter => _delimiter;
    internal int LengthPrefixBytes => _lengthPrefixBytes;
    internal bool LengthPrefixBigEndian => _lengthPrefixBigEndian;
    internal int MaxMessageSize => _maxMessageSize;

    private sealed class TcpClientHandler : IDisposable
    {
        private readonly string _connectionId;
        private readonly TcpClient _client;
        private readonly TcpSourceTask _task;
        private readonly CancellationToken _ct;
        private long _bytesReceived;
        private bool _disposed;

        public TcpClientHandler(string connectionId, TcpClient client, TcpSourceTask task, CancellationToken ct)
        {
            _connectionId = connectionId;
            _client = client;
            _task = task;
            _ct = ct;
        }

        public async Task StartAsync()
        {
            try
            {
                var networkStream = _client.GetStream();
                var stream = await _task.WrapWithTlsAsync(networkStream, _ct);

                await ProcessStreamAsync(stream);
            }
            catch (Exception) when (_ct.IsCancellationRequested)
            {
                // Normal shutdown
            }
            catch (Exception)
            {
                // Connection error
            }
            finally
            {
                _task.RemoveClient(_connectionId);
            }
        }

        private async Task ProcessStreamAsync(Stream stream)
        {
            switch (_task.Framing)
            {
                case TcpConnectorConfig.FramingLine:
                    await ProcessLineFramedAsync(stream);
                    break;
                case TcpConnectorConfig.FramingLengthPrefix:
                    await ProcessLengthPrefixedAsync(stream);
                    break;
                case TcpConnectorConfig.FramingDelimiter:
                    await ProcessDelimiterFramedAsync(stream);
                    break;
                default:
                    await ProcessLineFramedAsync(stream);
                    break;
            }
        }

        private async Task ProcessLineFramedAsync(Stream stream)
        {
            using var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true);

            while (!_ct.IsCancellationRequested)
            {
                var line = await reader.ReadLineAsync(_ct);
                if (line == null) break;

                var data = Encoding.UTF8.GetBytes(line);
                _bytesReceived += data.Length + 1; // +1 for newline
                _task.EnqueueRecord(_connectionId, data, _bytesReceived);
            }
        }

        private async Task ProcessLengthPrefixedAsync(Stream stream)
        {
            var lengthBuffer = new byte[_task.LengthPrefixBytes];

            while (!_ct.IsCancellationRequested)
            {
                var read = await ReadExactAsync(stream, lengthBuffer, _ct);
                if (read < lengthBuffer.Length) break;

                var length = ReadLength(lengthBuffer, _task.LengthPrefixBytes, _task.LengthPrefixBigEndian);
                if (length <= 0 || length > _task.MaxMessageSize)
                {
                    // Invalid length, close connection
                    break;
                }

                var data = new byte[length];
                read = await ReadExactAsync(stream, data, _ct);
                if (read < length) break;

                _bytesReceived += lengthBuffer.Length + length;
                _task.EnqueueRecord(_connectionId, data, _bytesReceived);
            }
        }

        private async Task ProcessDelimiterFramedAsync(Stream stream)
        {
            var buffer = new List<byte>();
            var singleByte = new byte[1];
            var delimiter = _task.Delimiter;

            while (!_ct.IsCancellationRequested)
            {
                var read = await stream.ReadAsync(singleByte, _ct);
                if (read == 0) break;

                buffer.Add(singleByte[0]);
                _bytesReceived++;

                if (buffer.Count > _task.MaxMessageSize)
                {
                    // Message too large, skip until delimiter
                    buffer.Clear();
                    continue;
                }

                if (EndsWithDelimiter(buffer, delimiter))
                {
                    var data = buffer.Take(buffer.Count - delimiter.Length).ToArray();
                    _task.EnqueueRecord(_connectionId, data, _bytesReceived);
                    buffer.Clear();
                }
            }
        }

        private static async Task<int> ReadExactAsync(Stream stream, byte[] buffer, CancellationToken ct)
        {
            var totalRead = 0;
            while (totalRead < buffer.Length)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(totalRead), ct);
                if (read == 0) break;
                totalRead += read;
            }
            return totalRead;
        }

        private static int ReadLength(byte[] buffer, int bytes, bool bigEndian)
        {
            return bytes switch
            {
                1 => buffer[0],
                2 => bigEndian
                    ? (buffer[0] << 8) | buffer[1]
                    : buffer[0] | (buffer[1] << 8),
                4 => bigEndian
                    ? (buffer[0] << 24) | (buffer[1] << 16) | (buffer[2] << 8) | buffer[3]
                    : buffer[0] | (buffer[1] << 8) | (buffer[2] << 16) | (buffer[3] << 24),
                _ => 0
            };
        }

        private static bool EndsWithDelimiter(List<byte> buffer, byte[] delimiter)
        {
            if (buffer.Count < delimiter.Length) return false;
            var start = buffer.Count - delimiter.Length;
            for (var i = 0; i < delimiter.Length; i++)
            {
                if (buffer[start + i] != delimiter[i]) return false;
            }
            return true;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            try { _client.Close(); } catch { /* ignore */ }
            try { _client.Dispose(); } catch { /* ignore */ }
        }
    }
}
