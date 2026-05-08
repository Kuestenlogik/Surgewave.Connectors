using System.IO.MemoryMappedFiles;
using System.IO.Pipes;
using System.Runtime.Versioning;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.InProc;

/// <summary>
/// Task that sends messages to in-process channels, named pipes, or shared memory.
/// </summary>
public sealed class InProcSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _mode = InProcConnectorConfig.DefaultMode;
    private Channel<InProcMessage>? _channel;
    private NamedPipeServerStream? _pipeServer;
    private MemoryMappedFile? _mmf;
    private MemoryMappedViewAccessor? _accessor;
    private int _sharedMemorySize;
    private int _sharedMemoryPosition;
    private bool _pipeConnected;

    public override void Start(IDictionary<string, string> config)
    {
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
                _pipeServer = new NamedPipeServerStream(pipeName, PipeDirection.Out, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
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
        try { _pipeServer?.Dispose(); } catch { /* ignore */ }
        _pipeServer = null;

        try { _accessor?.Dispose(); } catch { /* ignore */ }
        _accessor = null;
        try { _mmf?.Dispose(); } catch { /* ignore */ }
        _mmf = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
            _pipeServer?.Dispose();
            _pipeServer = null;
            _accessor?.Dispose();
            _accessor = null;
            _mmf?.Dispose();
            _mmf = null;
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0) return;

        switch (_mode)
        {
            case InProcConnectorConfig.ModeChannel:
                await PutChannelAsync(records, cancellationToken);
                break;
            case InProcConnectorConfig.ModeNamedPipe:
                await PutPipeAsync(records, cancellationToken);
                break;
            case InProcConnectorConfig.ModeSharedMemory:
                PutSharedMemory(records);
                break;
        }
    }

    private async Task PutChannelAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_channel == null) return;

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            var message = new InProcMessage
            {
                Key = record.Key,
                Value = record.Value,
                Headers = record.Headers,
                Timestamp = record.Timestamp
            };

            await _channel.Writer.WriteAsync(message, cancellationToken);
        }
    }

    private async Task PutPipeAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_pipeServer == null) return;

        // Wait for client connection if not already connected
        if (!_pipeConnected)
        {
            await _pipeServer.WaitForConnectionAsync(cancellationToken);
            _pipeConnected = true;
        }

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            // Write with newline delimiter
            await _pipeServer.WriteAsync(record.Value, cancellationToken);
            await _pipeServer.WriteAsync(new byte[] { (byte)'\n' }, cancellationToken);
        }

        await _pipeServer.FlushAsync(cancellationToken);
    }

    private void PutSharedMemory(IReadOnlyList<SinkRecord> records)
    {
        if (_accessor == null) return;

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            // Check if there's space
            if (_sharedMemoryPosition + 4 + record.Value.Length > _sharedMemorySize)
            {
                // Wrap around to start
                _sharedMemoryPosition = 0;
            }

            // Write [4 bytes: length][length bytes: data]
            _accessor.Write(_sharedMemoryPosition, record.Value.Length);
            _sharedMemoryPosition += 4;
            _accessor.WriteArray(_sharedMemoryPosition, record.Value, 0, record.Value.Length);
            _sharedMemoryPosition += record.Value.Length;
        }

        // Mark end with 0 length
        if (_sharedMemoryPosition + 4 <= _sharedMemorySize)
        {
            _accessor.Write(_sharedMemoryPosition, 0);
        }

        _accessor.Flush();
    }

    [SupportedOSPlatform("windows")]
    private void InitializeSharedMemory(IDictionary<string, string> config)
    {
        var sharedMemoryName = config[InProcConnectorConfig.SharedMemoryName];
        _sharedMemorySize = int.Parse(GetConfigOrDefault(config, InProcConnectorConfig.SharedMemorySize, InProcConnectorConfig.DefaultSharedMemorySize.ToString()));
        _mmf = MemoryMappedFile.CreateOrOpen(sharedMemoryName, _sharedMemorySize);
        _accessor = _mmf.CreateViewAccessor();
        _sharedMemoryPosition = 0;
    }

    private static string GetConfigOrDefault(IDictionary<string, string> config, string key, string defaultValue)
    {
        return config.TryGetValue(key, out var value) ? value : defaultValue;
    }
}
