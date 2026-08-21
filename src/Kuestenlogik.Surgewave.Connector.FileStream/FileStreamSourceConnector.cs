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
    private System.IO.FileStream? _stream;
    private long _streamOffset;
    private readonly MemoryStream _pending = new();
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
        _stream?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _stream?.Dispose();
            _pending.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_stream == null)
        {
            // The file may be created after the task started - keep retrying
            OpenFile();
        }

        if (_stream == null)
        {
            await Task.Delay(1000, cancellationToken);
            return [];
        }

        var records = new List<SourceRecord>();
        const int batchSize = 100;
        var buffer = new byte[4096];

        while (records.Count < batchSize)
        {
            ExtractLines(records, batchSize);
            if (records.Count >= batchSize)
            {
                break;
            }

            var read = await _stream.ReadAsync(buffer.AsMemory(), cancellationToken);
            if (read == 0)
            {
                // End of file, wait for more data
                break;
            }

            _pending.Seek(0, SeekOrigin.End);
            _pending.Write(buffer, 0, read);
        }

        if (records.Count == 0)
        {
            // No data available, wait a bit
            await Task.Delay(1000, cancellationToken);
        }

        return records;
    }

    /// <summary>
    /// Extracts complete lines from the pending byte buffer. The stored offset counts
    /// consumed bytes (line plus terminator), never the file stream's read position:
    /// buffered read-ahead would otherwise skip unconsumed lines on restart.
    /// </summary>
    private void ExtractLines(List<SourceRecord> records, int batchSize)
    {
        var data = _pending.GetBuffer();
        var length = (int)_pending.Length;
        var lineStart = 0;

        for (var i = 0; i < length && records.Count < batchSize; i++)
        {
            if (data[i] != (byte)'\n')
            {
                continue;
            }

            var lineEnd = i > lineStart && data[i - 1] == (byte)'\r' ? i - 1 : i;
            var line = Encoding.UTF8.GetString(data, lineStart, lineEnd - lineStart);

            _streamOffset += i - lineStart + 1;
            lineStart = i + 1;

            records.Add(new SourceRecord
            {
                SourcePartition = _sourcePartition,
                SourceOffset = new Dictionary<string, object>
                {
                    [PositionField] = _streamOffset
                },
                Topic = _topic,
                Value = Encoding.UTF8.GetBytes(line)
            });
        }

        if (lineStart > 0)
        {
            var remaining = length - lineStart;
            Buffer.BlockCopy(data, lineStart, data, 0, remaining);
            _pending.SetLength(remaining);
        }
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
        }
    }
}
