using System.Buffers.Binary;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.IO.Pipes;
using System.Runtime.Versioning;
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
    private int _ringCapacity;
    private long _pendingReadPosition;
    private long _committedReadPosition;
    private readonly byte[] _lengthBuffer = new byte[InProcSharedMemoryRing.LengthPrefixSize];
    private long _offset;

    private const int MaxRecordsPerPoll = 1000;
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

        // Read length-prefixed frames from the ring, resuming at the cursor instead of
        // restarting at 0 (which re-emitted every message on every poll).
        var writePosition = _accessor.ReadInt64(InProcSharedMemoryRing.WritePositionOffset);

        while (_pendingReadPosition < writePosition && records.Count < MaxRecordsPerPoll)
        {
            if (writePosition - _pendingReadPosition < InProcSharedMemoryRing.LengthPrefixSize)
                break;

            InProcSharedMemoryRing.Read(_accessor, _ringCapacity, _pendingReadPosition,
                _lengthBuffer, 0, InProcSharedMemoryRing.LengthPrefixSize);
            var length = BinaryPrimitives.ReadInt32LittleEndian(_lengthBuffer);

            if (length <= 0 || length > _ringCapacity - InProcSharedMemoryRing.LengthPrefixSize)
            {
                // Corrupt frame: resynchronize on the writer instead of looping forever.
                Context?.RaiseError?.Invoke(new InvalidDataException(
                    $"Shared memory frame at {_pendingReadPosition} declares {length} bytes; " +
                    $"skipping ahead to the writer position {writePosition}."));
                _pendingReadPosition = writePosition;
                break;
            }

            if (writePosition - _pendingReadPosition < InProcSharedMemoryRing.LengthPrefixSize + length)
                break; // frame not fully published yet

            var data = new byte[length];
            InProcSharedMemoryRing.Read(_accessor, _ringCapacity,
                _pendingReadPosition + InProcSharedMemoryRing.LengthPrefixSize, data, 0, length);
            _pendingReadPosition += InProcSharedMemoryRing.LengthPrefixSize + length;

            records.Add(CreateRecord(new InProcMessage { Value = data }));
        }
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Publishing the read cursor releases the space for the writer, so it may only
        // happen after the polled records have been produced.
        if (_accessor != null && _pendingReadPosition > _committedReadPosition)
        {
            _committedReadPosition = _pendingReadPosition;
            _accessor.Write(InProcSharedMemoryRing.ReadPositionOffset, _committedReadPosition);
            _accessor.Flush();
        }

        return Task.CompletedTask;
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
        var configuredSize = int.Parse(
            GetConfigOrDefault(config, InProcConnectorConfig.SharedMemorySize,
                InProcConnectorConfig.DefaultSharedMemorySize.ToString(CultureInfo.InvariantCulture)),
            CultureInfo.InvariantCulture);

        _mmf = MemoryMappedFile.OpenExisting(sharedMemoryName);
        _accessor = _mmf.CreateViewAccessor();
        _ringCapacity = InProcSharedMemoryRing.CapacityOf(_accessor);

        if (configuredSize > _accessor.Capacity)
        {
            throw new ArgumentException(
                $"{InProcConnectorConfig.SharedMemorySize} is {configuredSize} bytes but the writer mapped " +
                $"'{sharedMemoryName}' with only {_accessor.Capacity} bytes.",
                nameof(config));
        }

        // Resume where the last committed poll left off instead of re-reading the ring.
        _committedReadPosition = _accessor.ReadInt64(InProcSharedMemoryRing.ReadPositionOffset);
        _pendingReadPosition = _committedReadPosition;
    }

    private static string GetConfigOrDefault(IDictionary<string, string> config, string key, string defaultValue)
    {
        return config.TryGetValue(key, out var value) ? value : defaultValue;
    }
}
