using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.FileStream;

/// <summary>
/// A simple source connector that reads lines from a file and produces them to a topic.
/// This is equivalent to Kafka's FileStreamSourceConnector.
/// </summary>
[ConnectorMetadata(
    Name = "File Stream Source",
    Description = "Reads lines from a file and produces them to a topic. Equivalent to Kafka's FileStreamSourceConnector.",
    Author = "KL Surgewave",
    Tags = "file,stream,source,text",
    Icon = "FileDocumentOutline")]
public sealed class FileStreamSourceConnector : SourceConnector
{
    private const string FileConfig = "file";
    private const string TopicConfig = "topic";

    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(FileStreamSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(FileConfig, ConfigType.String, Importance.High, "Source file to read from")
        .Define(TopicConfig, ConfigType.String, Importance.High, "Topic to write to");

    private string _filename = "";
    private string _topic = "";

    public override void Start(IDictionary<string, string> config)
    {
        _filename = config.TryGetValue(FileConfig, out var file)
            ? file
            : throw new ArgumentException($"Missing required config: {FileConfig}");

        _topic = config.TryGetValue(TopicConfig, out var topic)
            ? topic
            : throw new ArgumentException($"Missing required config: {TopicConfig}");
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
                [TopicConfig] = _topic
            }
        ];
    }
}

/// <summary>
/// Task that reads lines from a file and produces them as records.
/// </summary>
public sealed class FileStreamSourceTask : SourceTask
{
    private const string FileConfig = "file";
    private const string TopicConfig = "topic";
    private const string PositionField = "position";

    public override string Version => "1.0.0";

    private string _filename = "";
    private string _topic = "";
    private StreamReader? _reader;
    private System.IO.FileStream? _stream;
    private long _streamOffset;
    private readonly Dictionary<string, object> _sourcePartition = new();

    public override void Start(IDictionary<string, string> config)
    {
        _filename = config[FileConfig];
        _topic = config[TopicConfig];

        _sourcePartition["filename"] = _filename;

        // Try to get stored offset
        var storedOffset = Context.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null && storedOffset.TryGetValue(PositionField, out var position))
        {
            _streamOffset = Convert.ToInt64(position);
        }

        OpenFile();
    }

    public override void Stop()
    {
        _reader?.Dispose();
        _stream?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _reader?.Dispose();
            _stream?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_reader == null || !File.Exists(_filename))
        {
            await Task.Delay(1000, cancellationToken);
            return [];
        }

        var records = new List<SourceRecord>();
        var batchSize = 100;
        var count = 0;

        while (count < batchSize)
        {
            var line = await _reader.ReadLineAsync(cancellationToken);
            if (line == null)
            {
                // End of file, wait for more data
                break;
            }

            _streamOffset = _stream!.Position;

            var sourceOffset = new Dictionary<string, object>
            {
                [PositionField] = _streamOffset
            };

            records.Add(new SourceRecord
            {
                SourcePartition = _sourcePartition,
                SourceOffset = sourceOffset,
                Topic = _topic,
                Value = Encoding.UTF8.GetBytes(line)
            });

            count++;
        }

        if (records.Count == 0)
        {
            // No data available, wait a bit
            await Task.Delay(1000, cancellationToken);
        }

        return records;
    }

    private void OpenFile()
    {
        if (File.Exists(_filename))
        {
            _stream = new System.IO.FileStream(_filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            if (_streamOffset > 0)
            {
                _stream.Seek(_streamOffset, SeekOrigin.Begin);
            }
            _reader = new StreamReader(_stream);
        }
    }
}
