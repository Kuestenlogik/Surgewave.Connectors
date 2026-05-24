using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.FileStream;

/// <summary>
/// A simple sink connector that writes records to a file.
/// This is equivalent to Kafka's FileStreamSinkConnector.
/// </summary>
[ConnectorMetadata(
    Name = "File Stream Sink",
    Description = "Writes records to a file. Equivalent to Kafka's FileStreamSinkConnector.",
    Author = "KL Surgewave",
    Tags = "file,stream,sink,text",
    Icon = "FileExportOutline")]
public sealed class FileStreamSinkConnector : SinkConnector
{
    private const string FileConfig = "file";
    private const string TopicsConfig = "topics";

    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(FileStreamSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(FileConfig, ConfigType.String, Importance.High, "Destination file to write to")
        .Define(TopicsConfig, ConfigType.String, Importance.High, "Topics to consume from");

    private string _filename = "";
    private string _topics = "";

    public override void Start(IDictionary<string, string> config)
    {
        _filename = config.TryGetValue(FileConfig, out var file)
            ? file
            : throw new ArgumentException($"Missing required config: {FileConfig}");

        _topics = config.TryGetValue(TopicsConfig, out var topics)
            ? topics
            : throw new ArgumentException($"Missing required config: {TopicsConfig}");
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // FileStream only supports a single task
        return
        [
            new Dictionary<string, string>
            {
                [FileConfig] = _filename,
                [TopicsConfig] = _topics
            }
        ];
    }
}

/// <summary>
/// Task that writes records to a file.
/// </summary>
public sealed class FileStreamSinkTask : SinkTask
{
    private const string FileConfig = "file";

    public override string Version => "1.0.0";

    private string _filename = "";
    private StreamWriter? _writer;

    public override void Start(IDictionary<string, string> config)
    {
        _filename = config[FileConfig];
        _writer = new StreamWriter(_filename, append: true);
    }

    public override void Stop()
    {
        _writer?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _writer?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_writer == null)
        {
            return;
        }

        foreach (var record in records)
        {
            var value = Encoding.UTF8.GetString(record.Value);
            await _writer.WriteLineAsync(value.AsMemory(), cancellationToken);
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        if (_writer != null)
        {
            await _writer.FlushAsync(cancellationToken);
        }
    }
}
