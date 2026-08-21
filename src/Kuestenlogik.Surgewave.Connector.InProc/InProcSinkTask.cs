using System.Buffers.Binary;
using System.Globalization;
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
    private string _sharedMemoryName = "";
    private int _sharedMemorySize;
    private int _ringCapacity;
    private readonly byte[] _lengthBuffer = new byte[InProcSharedMemoryRing.LengthPrefixSize];
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
        _pipeConnected = false;

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

        // A client that dropped leaves the server half connected - release it so a new
        // client can be accepted instead of failing on the broken pipe forever.
        if (_pipeConnected && !_pipeServer.IsConnected)
            DisconnectPipeClient();

        // Wait for client connection if not already connected
        if (!_pipeConnected)
        {
            await _pipeServer.WaitForConnectionAsync(cancellationToken);
            _pipeConnected = true;
        }

        try
        {
            foreach (var record in records)
            {
                if (record.Value == null) continue;

                // Write with newline delimiter
                await _pipeServer.WriteAsync(record.Value, cancellationToken);
                await _pipeServer.WriteAsync(new byte[] { (byte)'\n' }, cancellationToken);
            }

            await _pipeServer.FlushAsync(cancellationToken);
        }
        catch (IOException ex)
        {
            // The client vanished mid-batch: accept a new one on the next put and let the
            // worker retry this batch.
            DisconnectPipeClient();
            Context?.RaiseError?.Invoke(ex);
            throw;
        }
    }

    private void DisconnectPipeClient()
    {
        try
        {
            if (_pipeServer is { IsConnected: true })
                _pipeServer.Disconnect();
        }
        catch (InvalidOperationException)
        {
            // Already disconnected
        }

        _pipeConnected = false;
    }

    private void PutSharedMemory(IReadOnlyList<SinkRecord> records)
    {
        if (_accessor == null) return;

        var writePosition = _accessor.ReadInt64(InProcSharedMemoryRing.WritePositionOffset);

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            var frameSize = InProcSharedMemoryRing.LengthPrefixSize + record.Value.Length;

            if (frameSize > _ringCapacity)
            {
                // Poison record: it can never fit, skip it but keep it visible.
                Context?.RaiseError?.Invoke(new InvalidOperationException(
                    $"Record of {record.Value.Length} bytes does not fit into the {_ringCapacity} byte " +
                    $"shared memory '{_sharedMemoryName}'; raise {InProcConnectorConfig.SharedMemorySize}."));
                continue;
            }

            var readPosition = _accessor.ReadInt64(InProcSharedMemoryRing.ReadPositionOffset);

            if (writePosition - readPosition + frameSize > _ringCapacity)
            {
                // Never overwrite messages the reader has not consumed: fail the batch so
                // the worker retries it once the reader caught up.
                var error = new InvalidOperationException(
                    $"Shared memory '{_sharedMemoryName}' is full ({_ringCapacity} bytes); " +
                    $"the reader still owes {writePosition - readPosition} bytes.");
                Context?.RaiseError?.Invoke(error);
                throw error;
            }

            // Write [4 bytes: length][length bytes: data]
            BinaryPrimitives.WriteInt32LittleEndian(_lengthBuffer, record.Value.Length);
            InProcSharedMemoryRing.Write(_accessor, _ringCapacity, writePosition,
                _lengthBuffer, 0, InProcSharedMemoryRing.LengthPrefixSize);
            InProcSharedMemoryRing.Write(_accessor, _ringCapacity, writePosition + InProcSharedMemoryRing.LengthPrefixSize,
                record.Value, 0, record.Value.Length);

            writePosition += frameSize;

            // Publish the frame only once its bytes are in place.
            _accessor.Write(InProcSharedMemoryRing.WritePositionOffset, writePosition);
        }

        _accessor.Flush();
    }

    [SupportedOSPlatform("windows")]
    private void InitializeSharedMemory(IDictionary<string, string> config)
    {
        _sharedMemoryName = config[InProcConnectorConfig.SharedMemoryName];
        _sharedMemorySize = int.Parse(
            GetConfigOrDefault(config, InProcConnectorConfig.SharedMemorySize,
                InProcConnectorConfig.DefaultSharedMemorySize.ToString(CultureInfo.InvariantCulture)),
            CultureInfo.InvariantCulture);
        _mmf = MemoryMappedFile.CreateOrOpen(_sharedMemoryName, _sharedMemorySize);
        _accessor = _mmf.CreateViewAccessor();
        _ringCapacity = InProcSharedMemoryRing.CapacityOf(_accessor);

        if (_ringCapacity <= InProcSharedMemoryRing.LengthPrefixSize)
        {
            throw new ArgumentException(
                $"{InProcConnectorConfig.SharedMemorySize} must leave room for the " +
                $"{InProcSharedMemoryRing.HeaderSize} byte header and at least one message.",
                nameof(config));
        }
    }

    private static string GetConfigOrDefault(IDictionary<string, string> config, string key, string defaultValue)
    {
        return config.TryGetValue(key, out var value) ? value : defaultValue;
    }
}
