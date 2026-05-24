using System.IO.MemoryMappedFiles;
using System.IO.Pipes;
using System.Runtime.Versioning;
using System.Text;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.InProc;

/// <summary>
/// Task that receives messages from in-process channels, named pipes, or shared memory.
/// </summary>
public sealed class InProcSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _topic = "";
    private string _mode = InProcConnectorConfig.DefaultMode;
    private Channel<InProcMessage>? _channel;
    private NamedPipeClientStream? _pipeClient;
    private MemoryMappedFile? _mmf;
    private MemoryMappedViewAccessor? _accessor;
    private int _sharedMemorySize;
    private long _offset;
    private CancellationTokenSource? _cts;
    private Task? _pipeTask;
    private readonly Channel<InProcMessage> _pipeBuffer = System.Threading.Channels.Channel.CreateBounded<InProcMessage>(
        new BoundedChannelOptions(1000) { FullMode = BoundedChannelFullMode.Wait });

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[InProcConnectorConfig.Topic];
        _mode = config.TryGetValue(InProcConnectorConfig.Mode, out var m) ? m : InProcConnectorConfig.DefaultMode;

        switch (_mode)
        {
            case InProcConnectorConfig.ModeChannel:
                var channelName = config[InProcConnectorConfig.ChannelName];
                var bufferSize = int.Parse(GetConfigOrDefault(config, InProcConnectorConfig.BufferSize, InProcConnectorConfig.DefaultBufferSize.ToString()));
                _channel = InProcChannel.GetOrCreate(channelName, bufferSize);
                break;

            case InProcConnectorConfig.ModeNamedPipe:
                var pipeName = config[InProcConnectorConfig.PipeName];
                var pipeServerName = GetConfigOrDefault(config, InProcConnectorConfig.PipeServerName, InProcConnectorConfig.DefaultPipeServerName);
                _pipeClient = new NamedPipeClientStream(pipeServerName, pipeName, PipeDirection.In);
                var timeout = int.Parse(GetConfigOrDefault(config, InProcConnectorConfig.PipeTimeout, InProcConnectorConfig.DefaultPipeTimeout.ToString()));
                _pipeClient.Connect(timeout);
                _cts = new CancellationTokenSource();
                _pipeTask = ReadPipeLoopAsync(_cts.Token);
                break;

            case InProcConnectorConfig.ModeSharedMemory:
                if (!OperatingSystem.IsWindows())
                    throw new PlatformNotSupportedException("Shared memory mode is only supported on Windows.");
                InitializeSharedMemory(config);
                break;
        }
    }

    public override void Stop()
    {
        _cts?.Cancel();

        try { _pipeTask?.Wait(TimeSpan.FromSeconds(5)); } catch { /* ignore */ }
        try { _pipeClient?.Dispose(); } catch { /* ignore */ }
        _pipeClient = null;

        try { _accessor?.Dispose(); } catch { /* ignore */ }
        _accessor = null;
        try { _mmf?.Dispose(); } catch { /* ignore */ }
        _mmf = null;

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
            _pipeClient?.Dispose();
            _pipeClient = null;
            _accessor?.Dispose();
            _accessor = null;
            _mmf?.Dispose();
            _mmf = null;
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        switch (_mode)
        {
            case InProcConnectorConfig.ModeChannel:
                await PollChannelAsync(records, cancellationToken);
                break;
            case InProcConnectorConfig.ModeNamedPipe:
                await PollPipeAsync(records, cancellationToken);
                break;
            case InProcConnectorConfig.ModeSharedMemory:
                PollSharedMemory(records);
                break;
        }

        return records;
    }

    private async Task PollChannelAsync(List<SourceRecord> records, CancellationToken cancellationToken)
    {
        if (_channel == null) return;

        // Collect available messages
        while (_channel.Reader.TryRead(out var message))
        {
            records.Add(CreateRecord(message));
            if (records.Count >= 1000) break;
        }

        // Wait briefly for one if none available
        if (records.Count == 0)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(100);

                if (await _channel.Reader.WaitToReadAsync(cts.Token))
                {
                    if (_channel.Reader.TryRead(out var message))
                    {
                        records.Add(CreateRecord(message));
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Normal timeout
            }
        }
    }

    private async Task PollPipeAsync(List<SourceRecord> records, CancellationToken cancellationToken)
    {
        while (_pipeBuffer.Reader.TryRead(out var message))
        {
            records.Add(CreateRecord(message));
            if (records.Count >= 1000) break;
        }

        if (records.Count == 0)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(100);

                if (await _pipeBuffer.Reader.WaitToReadAsync(cts.Token))
                {
                    if (_pipeBuffer.Reader.TryRead(out var message))
                    {
                        records.Add(CreateRecord(message));
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Normal timeout
            }
        }
    }

    private void PollSharedMemory(List<SourceRecord> records)
    {
        if (_accessor == null) return;

        // Read from shared memory using a simple protocol:
        // [4 bytes: length][length bytes: data]
        // Length of 0 means no data
        var position = 0;
        while (position < _sharedMemorySize - 4)
        {
            var length = _accessor.ReadInt32(position);
            if (length <= 0) break;

            position += 4;
            if (position + length > _sharedMemorySize) break;

            var data = new byte[length];
            _accessor.ReadArray(position, data, 0, length);
            position += length;

            records.Add(CreateRecord(new InProcMessage { Value = data }));

            if (records.Count >= 1000) break;
        }
    }

    private async Task ReadPipeLoopAsync(CancellationToken cancellationToken)
    {
        if (_pipeClient == null) return;

        var buffer = new byte[8192];
        var messageBuffer = new List<byte>();

        try
        {
            while (!cancellationToken.IsCancellationRequested && _pipeClient.IsConnected)
            {
                var bytesRead = await _pipeClient.ReadAsync(buffer, cancellationToken);
                if (bytesRead == 0) break;

                // Simple line-based protocol for named pipes
                for (var i = 0; i < bytesRead; i++)
                {
                    if (buffer[i] == '\n')
                    {
                        if (messageBuffer.Count > 0)
                        {
                            var message = new InProcMessage { Value = messageBuffer.ToArray() };
                            await _pipeBuffer.Writer.WriteAsync(message, cancellationToken);
                            messageBuffer.Clear();
                        }
                    }
                    else if (buffer[i] != '\r')
                    {
                        messageBuffer.Add(buffer[i]);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation
        }
        catch (IOException)
        {
            // Pipe closed
        }
    }

    private SourceRecord CreateRecord(InProcMessage message)
    {
        var offset = Interlocked.Increment(ref _offset);
        return new SourceRecord
        {
            Topic = _topic,
            Partition = 0,
            SourcePartition = new Dictionary<string, object> { ["mode"] = _mode },
            SourceOffset = new Dictionary<string, object> { ["offset"] = offset },
            Key = message.Key,
            Value = message.Value,
            Headers = message.Headers != null ? new Dictionary<string, byte[]>(message.Headers) : null,
            Timestamp = message.Timestamp
        };
    }

    [SupportedOSPlatform("windows")]
    private void InitializeSharedMemory(IDictionary<string, string> config)
    {
        var sharedMemoryName = config[InProcConnectorConfig.SharedMemoryName];
        _sharedMemorySize = int.Parse(GetConfigOrDefault(config, InProcConnectorConfig.SharedMemorySize, InProcConnectorConfig.DefaultSharedMemorySize.ToString()));
        _mmf = MemoryMappedFile.OpenExisting(sharedMemoryName);
        _accessor = _mmf.CreateViewAccessor();
    }

    private static string GetConfigOrDefault(IDictionary<string, string> config, string key, string defaultValue)
    {
        return config.TryGetValue(key, out var value) ? value : defaultValue;
    }
}
