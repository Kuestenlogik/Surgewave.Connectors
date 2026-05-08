using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Generator;

/// <summary>
/// Task that generates test messages using configurable templates.
/// Supports various placeholders for dynamic content generation.
/// </summary>
[SuppressMessage("Security", "CA5394:Do not use insecure randomness", Justification = "Non-cryptographic randomness is appropriate for test data generation")]
public sealed class GeneratorSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _topic = "";
    private long _messageCount;
    private long _intervalMs = GeneratorConnectorConfig.DefaultIntervalMs;
    private int _batchSize = GeneratorConnectorConfig.DefaultBatchSize;
    private string _keyTemplate = GeneratorConnectorConfig.DefaultKeyTemplate;
    private string _valueTemplate = GeneratorConnectorConfig.DefaultValueTemplate;
    private string _messageFormat = GeneratorConnectorConfig.DefaultMessageFormat;
    private long _sequenceStart = GeneratorConnectorConfig.DefaultSequenceStart;
    private long _sequenceStep = GeneratorConnectorConfig.DefaultSequenceStep;
    private int _randomStringLength = GeneratorConnectorConfig.DefaultRandomStringLength;
    private int _randomIntMin = GeneratorConnectorConfig.DefaultRandomIntMin;
    private int _randomIntMax = GeneratorConnectorConfig.DefaultRandomIntMax;
    private double _randomDoubleMin = GeneratorConnectorConfig.DefaultRandomDoubleMin;
    private double _randomDoubleMax = GeneratorConnectorConfig.DefaultRandomDoubleMax;

    private long _currentSequence;
    private long _messagesGenerated;
    private Random _random = new();
    private bool _completed;

    private readonly Dictionary<string, object> _sourcePartition = new();

    private const string AlphanumericChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config.TryGetValue(GeneratorConnectorConfig.Topic, out var topic) ? topic : "";

        if (config.TryGetValue(GeneratorConnectorConfig.MessageCount, out var mc))
            _messageCount = long.Parse(mc);

        if (config.TryGetValue(GeneratorConnectorConfig.IntervalMs, out var im))
            _intervalMs = long.Parse(im);

        if (config.TryGetValue(GeneratorConnectorConfig.BatchSize, out var bs))
            _batchSize = int.Parse(bs);

        _keyTemplate = config.TryGetValue(GeneratorConnectorConfig.KeyTemplate, out var kt) ? kt : GeneratorConnectorConfig.DefaultKeyTemplate;
        _valueTemplate = config.TryGetValue(GeneratorConnectorConfig.ValueTemplate, out var vt) ? vt : GeneratorConnectorConfig.DefaultValueTemplate;
        _messageFormat = config.TryGetValue(GeneratorConnectorConfig.MessageFormat, out var mf) ? mf : GeneratorConnectorConfig.DefaultMessageFormat;

        if (config.TryGetValue(GeneratorConnectorConfig.SequenceStart, out var ss))
            _sequenceStart = long.Parse(ss);

        if (config.TryGetValue(GeneratorConnectorConfig.SequenceStep, out var step))
            _sequenceStep = long.Parse(step);

        if (config.TryGetValue(GeneratorConnectorConfig.RandomSeed, out var seed))
        {
            var seedValue = long.Parse(seed);
            _random = seedValue != 0 ? new Random((int)seedValue) : new Random();
        }

        if (config.TryGetValue(GeneratorConnectorConfig.RandomStringLength, out var rsl))
            _randomStringLength = int.Parse(rsl);

        if (config.TryGetValue(GeneratorConnectorConfig.RandomIntMin, out var rim))
            _randomIntMin = int.Parse(rim);

        if (config.TryGetValue(GeneratorConnectorConfig.RandomIntMax, out var rix))
            _randomIntMax = int.Parse(rix);

        if (config.TryGetValue(GeneratorConnectorConfig.RandomDoubleMin, out var rdm))
            _randomDoubleMin = double.Parse(rdm, CultureInfo.InvariantCulture);

        if (config.TryGetValue(GeneratorConnectorConfig.RandomDoubleMax, out var rdx))
            _randomDoubleMax = double.Parse(rdx, CultureInfo.InvariantCulture);

        _currentSequence = _sequenceStart;
        _messagesGenerated = 0;
        _completed = false;

        _sourcePartition["connector"] = "generator";
        _sourcePartition["topic"] = _topic;

        // Check for stored offset
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null)
        {
            if (storedOffset.TryGetValue("sequence", out var seq))
            {
                _currentSequence = Convert.ToInt64(seq);
            }
            if (storedOffset.TryGetValue("count", out var cnt))
            {
                _messagesGenerated = Convert.ToInt64(cnt);
            }
        }
    }

    public override void Stop()
    {
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        // Check if we've reached the message limit
        if (_messageCount > 0 && _messagesGenerated >= _messageCount)
        {
            if (!_completed)
            {
                _completed = true;
            }
            await Task.Delay((int)_intervalMs, cancellationToken);
            return records;
        }

        // Generate batch of records
        var count = _batchSize;
        if (_messageCount > 0)
        {
            var remaining = _messageCount - _messagesGenerated;
            count = (int)Math.Min(count, remaining);
        }

        for (var i = 0; i < count && !cancellationToken.IsCancellationRequested; i++)
        {
            var record = GenerateRecord();
            records.Add(record);
            _messagesGenerated++;
        }

        // Wait for the configured interval
        if (_intervalMs > 0)
        {
            await Task.Delay((int)_intervalMs, cancellationToken);
        }

        return records;
    }

    private SourceRecord GenerateRecord()
    {
        var key = ProcessTemplate(_keyTemplate);
        var value = ProcessTemplate(_valueTemplate);

        var keyBytes = string.IsNullOrEmpty(key) ? null : Encoding.UTF8.GetBytes(key);
        var valueBytes = Encoding.UTF8.GetBytes(value);

        var sourceOffset = new Dictionary<string, object>
        {
            ["sequence"] = _currentSequence,
            ["count"] = _messagesGenerated + 1
        };

        var record = new SourceRecord
        {
            Topic = _topic,
            Key = keyBytes,
            Value = valueBytes,
            SourcePartition = _sourcePartition,
            SourceOffset = sourceOffset,
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["generator.sequence"] = Encoding.UTF8.GetBytes(_currentSequence.ToString()),
                ["generator.count"] = Encoding.UTF8.GetBytes((_messagesGenerated + 1).ToString())
            }
        };

        // Advance sequence
        _currentSequence += _sequenceStep;

        return record;
    }

    private string ProcessTemplate(string template)
    {
        if (string.IsNullOrEmpty(template))
            return template;

        var result = template;

        // Replace placeholders
        result = result.Replace(GeneratorConnectorConfig.PlaceholderSequence, _currentSequence.ToString());
        result = result.Replace(GeneratorConnectorConfig.PlaceholderTimestamp, DateTimeOffset.UtcNow.ToString("O"));
        result = result.Replace(GeneratorConnectorConfig.PlaceholderTimestampMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString());
        result = result.Replace(GeneratorConnectorConfig.PlaceholderUuid, Guid.NewGuid().ToString());
        result = result.Replace(GeneratorConnectorConfig.PlaceholderRandomInt, GenerateRandomInt().ToString());
        result = result.Replace(GeneratorConnectorConfig.PlaceholderRandomDouble, GenerateRandomDouble().ToString("F6"));
        result = result.Replace(GeneratorConnectorConfig.PlaceholderRandomString, GenerateRandomString());
        result = result.Replace(GeneratorConnectorConfig.PlaceholderRandomBool, (_random.Next(2) == 1).ToString().ToLowerInvariant());
        result = result.Replace(GeneratorConnectorConfig.PlaceholderPartition, "0"); // Single partition by default
        result = result.Replace(GeneratorConnectorConfig.PlaceholderTopic, _topic);

        return result;
    }

    private int GenerateRandomInt()
    {
        return _random.Next(_randomIntMin, _randomIntMax + 1);
    }

    private double GenerateRandomDouble()
    {
        return _randomDoubleMin + (_random.NextDouble() * (_randomDoubleMax - _randomDoubleMin));
    }

    private string GenerateRandomString()
    {
        var chars = new char[_randomStringLength];
        for (var i = 0; i < _randomStringLength; i++)
        {
            chars[i] = AlphanumericChars[_random.Next(AlphanumericChars.Length)];
        }
        return new string(chars);
    }
}
