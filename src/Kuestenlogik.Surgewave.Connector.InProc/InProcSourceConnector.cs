using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.InProc;

/// <summary>
/// Source connector that receives messages from in-process channels, named pipes, or shared memory.
/// </summary>
public sealed class InProcSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(InProcSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(InProcConnectorConfig.Topic, ConfigType.String, Importance.High, "Target topic for produced records", EditorHint.Topic)
        .Define(InProcConnectorConfig.Mode, ConfigType.String, InProcConnectorConfig.DefaultMode, Importance.Medium, "Communication mode: channel, namedpipe, sharedmemory")
        .Define(InProcConnectorConfig.ChannelName, ConfigType.String, Importance.High, "Name of the in-process channel (required for channel mode)")
        .Define(InProcConnectorConfig.BufferSize, ConfigType.Int, InProcConnectorConfig.DefaultBufferSize, Importance.Low, "Channel buffer size")
        .Define(InProcConnectorConfig.PipeName, ConfigType.String, Importance.High, "Named pipe name (required for namedpipe mode)")
        .Define(InProcConnectorConfig.PipeServerName, ConfigType.String, InProcConnectorConfig.DefaultPipeServerName, Importance.Low, "Named pipe server name")
        .Define(InProcConnectorConfig.PipeTimeout, ConfigType.Int, InProcConnectorConfig.DefaultPipeTimeout, Importance.Low, "Named pipe connection timeout in ms")
        .Define(InProcConnectorConfig.SharedMemoryName, ConfigType.String, Importance.High, "Shared memory name (required for sharedmemory mode)")
        .Define(InProcConnectorConfig.SharedMemorySize, ConfigType.Int, InProcConnectorConfig.DefaultSharedMemorySize, Importance.Low, "Shared memory size in bytes");

    private string _topic = "";
    private string _channelName = "";
    private string _mode = InProcConnectorConfig.DefaultMode;
    private int _bufferSize = InProcConnectorConfig.DefaultBufferSize;
    private string _pipeName = "";
    private string _pipeServerName = InProcConnectorConfig.DefaultPipeServerName;
    private int _pipeTimeout = InProcConnectorConfig.DefaultPipeTimeout;
    private string _sharedMemoryName = "";
    private int _sharedMemorySize = InProcConnectorConfig.DefaultSharedMemorySize;

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(InProcConnectorConfig.Topic, out _topic!) || string.IsNullOrEmpty(_topic))
            throw new ArgumentException($"Missing required config: {InProcConnectorConfig.Topic}");

        _mode = config.TryGetValue(InProcConnectorConfig.Mode, out var m) ? m : InProcConnectorConfig.DefaultMode;

        switch (_mode)
        {
            case InProcConnectorConfig.ModeChannel:
                if (!config.TryGetValue(InProcConnectorConfig.ChannelName, out _channelName!) || string.IsNullOrEmpty(_channelName))
                    throw new ArgumentException($"Missing required config for channel mode: {InProcConnectorConfig.ChannelName}");
                if (config.TryGetValue(InProcConnectorConfig.BufferSize, out var bs))
                    _bufferSize = int.Parse(bs);
                break;

            case InProcConnectorConfig.ModeNamedPipe:
                if (!config.TryGetValue(InProcConnectorConfig.PipeName, out _pipeName!) || string.IsNullOrEmpty(_pipeName))
                    throw new ArgumentException($"Missing required config for named pipe mode: {InProcConnectorConfig.PipeName}");
                if (config.TryGetValue(InProcConnectorConfig.PipeServerName, out var psn))
                    _pipeServerName = psn;
                if (config.TryGetValue(InProcConnectorConfig.PipeTimeout, out var pt))
                    _pipeTimeout = int.Parse(pt);
                break;

            case InProcConnectorConfig.ModeSharedMemory:
                if (!config.TryGetValue(InProcConnectorConfig.SharedMemoryName, out _sharedMemoryName!) || string.IsNullOrEmpty(_sharedMemoryName))
                    throw new ArgumentException($"Missing required config for shared memory mode: {InProcConnectorConfig.SharedMemoryName}");
                if (config.TryGetValue(InProcConnectorConfig.SharedMemorySize, out var sms))
                    _sharedMemorySize = int.Parse(sms);
                break;

            default:
                throw new ArgumentException($"Invalid mode: {_mode}. Valid values: {InProcConnectorConfig.ModeChannel}, {InProcConnectorConfig.ModeNamedPipe}, {InProcConnectorConfig.ModeSharedMemory}");
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        var config = new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = _topic,
            [InProcConnectorConfig.Mode] = _mode,
            [InProcConnectorConfig.BufferSize] = _bufferSize.ToString()
        };

        switch (_mode)
        {
            case InProcConnectorConfig.ModeChannel:
                config[InProcConnectorConfig.ChannelName] = _channelName;
                break;
            case InProcConnectorConfig.ModeNamedPipe:
                config[InProcConnectorConfig.PipeName] = _pipeName;
                config[InProcConnectorConfig.PipeServerName] = _pipeServerName;
                config[InProcConnectorConfig.PipeTimeout] = _pipeTimeout.ToString();
                break;
            case InProcConnectorConfig.ModeSharedMemory:
                config[InProcConnectorConfig.SharedMemoryName] = _sharedMemoryName;
                config[InProcConnectorConfig.SharedMemorySize] = _sharedMemorySize.ToString();
                break;
        }

        return [config];
    }
}
