namespace Kuestenlogik.Surgewave.Connector.Sequence;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A source task that reads from multiple child sources in sequence.
/// When one source completes (returns empty multiple times), it advances to the next.
/// </summary>
public sealed class SequenceSourceTask : SourceTask
{
    private string _topic = "";
    private List<ChildSourceConfig> _sourceConfigs = [];
    private bool _continueOnError = SequenceConnectorConfig.DefaultContinueOnError;
    private int _emptyPollsBeforeAdvance = SequenceConnectorConfig.DefaultEmptyPollsBeforeAdvance;
    private int _emptyPollDelayMs = SequenceConnectorConfig.DefaultEmptyPollDelayMs;
    private bool _includeSourceIndex = SequenceConnectorConfig.DefaultIncludeSourceIndex;
    private string _sourceIndexHeader = SequenceConnectorConfig.DefaultSourceIndexHeader;
    private string _completionBehavior = SequenceConnectorConfig.DefaultCompletionBehavior;

    private int _currentSourceIndex;
    private SourceTask? _currentTask;
    private int _consecutiveEmptyPolls;
    private bool _allSourcesCompleted;
    private bool _disposed;

    public override string Version => "1.0.0";

    /// <summary>
    /// Gets the current source index (0-based).
    /// </summary>
    public int CurrentSourceIndex => _currentSourceIndex;

    /// <summary>
    /// Gets whether all sources have completed.
    /// </summary>
    public bool AllSourcesCompleted => _allSourcesCompleted;

    /// <summary>
    /// Gets the total number of configured sources.
    /// </summary>
    public int SourceCount => _sourceConfigs.Count;

    /// <summary>
    /// Event raised when advancing to the next source.
    /// </summary>
#pragma warning disable CA1003 // Use generic event handler instances
    public event Action<int, int>? OnSourceAdvance; // (fromIndex, toIndex)
#pragma warning restore CA1003

    /// <summary>
    /// Event raised when all sources complete.
    /// </summary>
#pragma warning disable CA1003 // Use generic event handler instances
    public event Action? OnAllSourcesCompleted;
#pragma warning restore CA1003

    public override void Start(IDictionary<string, string> config)
    {
        if (config.TryGetValue(SequenceConnectorConfig.TopicConfig, out var topic))
            _topic = topic;

        if (config.TryGetValue(SequenceConnectorConfig.SourcesConfig, out var sources))
            _sourceConfigs = ParseSourceConfigs(sources);

        if (config.TryGetValue(SequenceConnectorConfig.ContinueOnErrorConfig, out var continueOnError))
            _continueOnError = bool.Parse(continueOnError);

        if (config.TryGetValue(SequenceConnectorConfig.EmptyPollsBeforeAdvanceConfig, out var emptyPolls))
            _emptyPollsBeforeAdvance = int.Parse(emptyPolls);

        if (config.TryGetValue(SequenceConnectorConfig.EmptyPollDelayMsConfig, out var emptyDelay))
            _emptyPollDelayMs = int.Parse(emptyDelay);

        if (config.TryGetValue(SequenceConnectorConfig.IncludeSourceIndexConfig, out var includeIndex))
            _includeSourceIndex = bool.Parse(includeIndex);

        if (config.TryGetValue(SequenceConnectorConfig.SourceIndexHeaderConfig, out var indexHeader))
            _sourceIndexHeader = indexHeader;

        if (config.TryGetValue(SequenceConnectorConfig.CompletionBehaviorConfig, out var behavior))
            _completionBehavior = behavior;

        _currentSourceIndex = 0;
        _consecutiveEmptyPolls = 0;
        _allSourcesCompleted = false;

        if (_sourceConfigs.Count > 0)
        {
            StartCurrentSource();
        }
    }

    public override void Stop()
    {
        StopCurrentSource();
        _sourceConfigs.Clear();
        _currentSourceIndex = 0;
        _consecutiveEmptyPolls = 0;
        _allSourcesCompleted = false;
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_allSourcesCompleted || _sourceConfigs.Count == 0)
            return [];

        if (_currentTask == null)
            return [];

        try
        {
            var records = await _currentTask.PollAsync(cancellationToken);

            if (records.Count == 0)
            {
                _consecutiveEmptyPolls++;

                if (_consecutiveEmptyPolls >= _emptyPollsBeforeAdvance)
                {
                    // Current source is considered complete
                    await AdvanceToNextSourceAsync();
                }
                else if (_emptyPollDelayMs > 0)
                {
                    await Task.Delay(_emptyPollDelayMs, cancellationToken);
                }

                return [];
            }

            // Reset empty poll counter on successful poll
            _consecutiveEmptyPolls = 0;

            // Transform records to use our topic and optionally add source index header
            var transformedRecords = new List<SourceRecord>(records.Count);
            foreach (var record in records)
            {
                var newRecord = TransformRecord(record);
                transformedRecords.Add(newRecord);
            }

            return transformedRecords;
        }
        catch (Exception) when (_continueOnError)
        {
            // On error with continue-on-error enabled, advance to next source
            await AdvanceToNextSourceAsync();
            return [];
        }
    }

    /// <summary>
    /// Manually advance to the next source.
    /// </summary>
    public void AdvanceToNextSource()
    {
        AdvanceToNextSourceAsync().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Reset the sequence to start from the first source.
    /// </summary>
    public void Reset()
    {
        StopCurrentSource();
        _currentSourceIndex = 0;
        _consecutiveEmptyPolls = 0;
        _allSourcesCompleted = false;

        if (_sourceConfigs.Count > 0)
        {
            StartCurrentSource();
        }
    }

    private async Task AdvanceToNextSourceAsync()
    {
        var fromIndex = _currentSourceIndex;
        StopCurrentSource();

        _currentSourceIndex++;
        _consecutiveEmptyPolls = 0;

        if (_currentSourceIndex >= _sourceConfigs.Count)
        {
            if (_completionBehavior == SequenceConnectorConfig.CompletionBehaviorRestart)
            {
                _currentSourceIndex = 0;
                StartCurrentSource();
                OnSourceAdvance?.Invoke(fromIndex, _currentSourceIndex);
            }
            else
            {
                _allSourcesCompleted = true;
                OnAllSourcesCompleted?.Invoke();
            }
        }
        else
        {
            StartCurrentSource();
            OnSourceAdvance?.Invoke(fromIndex, _currentSourceIndex);
        }

        await Task.CompletedTask;
    }

    private void StartCurrentSource()
    {
        if (_currentSourceIndex >= _sourceConfigs.Count)
            return;

        var sourceConfig = _sourceConfigs[_currentSourceIndex];
        _currentTask = CreateSourceTask(sourceConfig);

        if (_currentTask != null)
        {
            var taskContext = new TaskContext();
            _currentTask.Initialize(taskContext);
            _currentTask.Start(sourceConfig.Config);
        }
    }

    private void StopCurrentSource()
    {
        if (_currentTask != null)
        {
            try
            {
                _currentTask.Stop();
                _currentTask.Dispose();
            }
            catch
            {
                // Ignore errors during cleanup
            }
            _currentTask = null;
        }
    }

    private SourceTask? CreateSourceTask(ChildSourceConfig sourceConfig)
    {
        try
        {
            var taskType = Type.GetType(sourceConfig.TaskClass);
            if (taskType == null)
            {
                // Try loading from assemblies in the current domain
                foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
                {
                    taskType = assembly.GetType(sourceConfig.TaskClass);
                    if (taskType != null)
                        break;
                }
            }

            if (taskType == null || !typeof(SourceTask).IsAssignableFrom(taskType))
                return null;

            return (SourceTask?)Activator.CreateInstance(taskType);
        }
        catch
        {
            return null;
        }
    }

    private SourceRecord TransformRecord(SourceRecord original)
    {
        IDictionary<string, byte[]>? headers = null;

        if (_includeSourceIndex)
        {
            headers = original.Headers != null
                ? new Dictionary<string, byte[]>(original.Headers)
                : new Dictionary<string, byte[]>();

            headers[_sourceIndexHeader] = Encoding.UTF8.GetBytes(_currentSourceIndex.ToString());
        }
        else
        {
            headers = original.Headers;
        }

        return new SourceRecord
        {
            Topic = _topic,
            Partition = original.Partition,
            Key = original.Key,
            Value = original.Value,
            Timestamp = original.Timestamp,
            Headers = headers,
            SourcePartition = new Dictionary<string, object>(original.SourcePartition)
            {
                ["sequence.source.index"] = _currentSourceIndex
            },
            SourceOffset = new Dictionary<string, object>(original.SourceOffset)
            {
                ["sequence.source.index"] = _currentSourceIndex
            }
        };
    }

    private static List<ChildSourceConfig> ParseSourceConfigs(string json)
    {
        var configs = new List<ChildSourceConfig>();

        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
                return configs;

            foreach (var element in doc.RootElement.EnumerateArray())
            {
                var config = new Dictionary<string, string>();
                string? taskClass = null;

                foreach (var prop in element.EnumerateObject())
                {
                    var value = prop.Value.ValueKind == JsonValueKind.String
                        ? prop.Value.GetString() ?? ""
                        : prop.Value.ToString();

                    if (prop.Name == "task.class")
                    {
                        taskClass = value;
                    }
                    else
                    {
                        config[prop.Name] = value;
                    }
                }

                if (!string.IsNullOrEmpty(taskClass))
                {
                    configs.Add(new ChildSourceConfig(taskClass, config));
                }
            }
        }
        catch
        {
            // Return empty list on parse error
        }

        return configs;
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                StopCurrentSource();
                _sourceConfigs.Clear();
            }
            _disposed = true;
        }
        base.Dispose(disposing);
    }
}

/// <summary>
/// Configuration for a child source in the sequence.
/// </summary>
internal sealed record ChildSourceConfig(string TaskClass, Dictionary<string, string> Config);
