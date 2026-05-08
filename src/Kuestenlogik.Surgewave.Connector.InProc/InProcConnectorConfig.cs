namespace Kuestenlogik.Surgewave.Connector.InProc;

/// <summary>
/// Configuration constants for the InProc connector.
/// </summary>
public static class InProcConnectorConfig
{
    // Common config
    public const string Topic = "topic";
    public const string Topics = "topics";
    public const string ChannelName = "channel.name";
    public const string Mode = "mode";
    public const string BufferSize = "buffer.size";

    // Named pipe config
    public const string PipeName = "pipe.name";
    public const string PipeServerName = "pipe.server.name";
    public const string PipeDirection = "pipe.direction";
    public const string PipeTimeout = "pipe.timeout.ms";

    // Shared memory config
    public const string SharedMemoryName = "sharedmemory.name";
    public const string SharedMemorySize = "sharedmemory.size";

    // Mode values
    public const string ModeChannel = "channel";
    public const string ModeNamedPipe = "namedpipe";
    public const string ModeSharedMemory = "sharedmemory";

    // Pipe direction values
    public const string PipeDirectionIn = "in";
    public const string PipeDirectionOut = "out";
    public const string PipeDirectionInOut = "inout";

    // Defaults
    public const string DefaultMode = ModeChannel;
    public const int DefaultBufferSize = 1000;
    public const string DefaultPipeServerName = ".";
    public const string DefaultPipeDirection = PipeDirectionInOut;
    public const int DefaultPipeTimeout = 5000;
    public const int DefaultSharedMemorySize = 1024 * 1024; // 1 MB
}
