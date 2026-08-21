using System.Globalization;
using System.Text;
using System.Text.Json;
using Ical.Net;
using Calendar = Ical.Net.Calendar;
using Ical.Net.CalendarComponents;
using Ical.Net.DataTypes;
using Ical.Net.Serialization;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.ICal;

/// <summary>
/// Task that generates iCal/ICS calendar events from records.
/// </summary>
public sealed class ICalSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _outputMode = ICalConnectorConfig.DefaultOutputMode;
    private string _outputPath = "";
    private string _outputTopic = "";
    private string _calendarName = ICalConnectorConfig.DefaultCalendarName;
    private string _calendarProductId = ICalConnectorConfig.DefaultCalendarProductId;
    private int _defaultDurationMinutes = ICalConnectorConfig.DefaultDurationMinutes;
    private string _summaryField = "summary";
    private string _descriptionField = "description";
    private string _startField = "start";
    private string _endField = "end";
    private string _locationField = "location";
    private string _uidField = "uid";
    private int _maxEventsPerFile = ICalConnectorConfig.DefaultMaxEventsPerFile;
    private int _flushIntervalMs = ICalConnectorConfig.DefaultFlushIntervalMs;

    private Calendar? _currentCalendar;
    private int _eventCount;
    private DateTime _lastFlushUtc = DateTime.UtcNow;
    private int _fileSequence;

    public override void Start(IDictionary<string, string> config)
    {
        _outputMode = config.TryGetValue(ICalConnectorConfig.OutputModeConfig, out var mode)
            ? mode : ICalConnectorConfig.DefaultOutputMode;
        _outputPath = config.TryGetValue(ICalConnectorConfig.OutputPathConfig, out var path)
            ? path : "";
        _outputTopic = config.TryGetValue(ICalConnectorConfig.OutputTopicConfig, out var outputTopic)
            ? outputTopic : "";
        _calendarName = config.TryGetValue(ICalConnectorConfig.CalendarNameConfig, out var name)
            ? name : ICalConnectorConfig.DefaultCalendarName;
        _calendarProductId = config.TryGetValue(ICalConnectorConfig.CalendarProductIdConfig, out var prodId)
            ? prodId : ICalConnectorConfig.DefaultCalendarProductId;
        _defaultDurationMinutes = config.TryGetValue(ICalConnectorConfig.DefaultDurationMinutesConfig, out var dur)
            ? int.Parse(dur) : ICalConnectorConfig.DefaultDurationMinutes;

        _summaryField = config.TryGetValue(ICalConnectorConfig.SummaryFieldConfig, out var sf)
            ? sf : "summary";
        _descriptionField = config.TryGetValue(ICalConnectorConfig.DescriptionFieldConfig, out var df)
            ? df : "description";
        _startField = config.TryGetValue(ICalConnectorConfig.StartFieldConfig, out var stf)
            ? stf : "start";
        _endField = config.TryGetValue(ICalConnectorConfig.EndFieldConfig, out var ef)
            ? ef : "end";
        _locationField = config.TryGetValue(ICalConnectorConfig.LocationFieldConfig, out var lf)
            ? lf : "location";
        _uidField = config.TryGetValue(ICalConnectorConfig.UidFieldConfig, out var uf)
            ? uf : "uid";
        _maxEventsPerFile = config.TryGetValue(ICalConnectorConfig.MaxEventsPerFileConfig, out var max)
            ? int.Parse(max) : ICalConnectorConfig.DefaultMaxEventsPerFile;
        _flushIntervalMs = config.TryGetValue(ICalConnectorConfig.FlushIntervalMsConfig, out var flushInterval)
            ? int.Parse(flushInterval, CultureInfo.InvariantCulture) : ICalConnectorConfig.DefaultFlushIntervalMs;

        if (_outputMode == ICalConnectorConfig.OutputModeFile)
        {
            _currentCalendar = CreateCalendar();
        }
        else if (_outputMode == ICalConnectorConfig.OutputModeRecord)
        {
            // Record mode produces the generated ICS to its own topic; without one the
            // consumed records would have nowhere to go.
            if (string.IsNullOrWhiteSpace(_outputTopic))
            {
                throw new ArgumentException(
                    $"Output mode '{ICalConnectorConfig.OutputModeRecord}' requires {ICalConnectorConfig.OutputTopicConfig}",
                    nameof(config));
            }
        }
        else
        {
            throw new ArgumentException(
                $"Unknown {ICalConnectorConfig.OutputModeConfig} '{_outputMode}'. Valid values: " +
                $"'{ICalConnectorConfig.OutputModeFile}', '{ICalConnectorConfig.OutputModeRecord}'",
                nameof(config));
        }

        _lastFlushUtc = DateTime.UtcNow;
    }

    public override void Stop()
    {
        FlushCalendar();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            FlushCalendar();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        var lastTopic = "events";

        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0)
                continue;

            lastTopic = record.Topic;

            CalendarEvent evt;
            try
            {
                var json = Encoding.UTF8.GetString(record.Value);
                using var doc = JsonDocument.Parse(json);
                evt = CreateEvent(doc.RootElement, record);
            }
            catch (JsonException ex)
            {
                // Poison record: skip it, but keep it visible instead of dropping it silently.
                Context?.RaiseError?.Invoke(ex);
                continue;
            }

            if (_outputMode == ICalConnectorConfig.OutputModeFile)
            {
                _currentCalendar ??= CreateCalendar();
                _currentCalendar.Events.Add(evt);
                _eventCount++;

                if (_eventCount >= _maxEventsPerFile)
                {
                    await FlushCalendarAsync(record.Topic, cancellationToken);
                }
            }
            else
            {
                // Record mode: emit the ICS as the transformed record value.
                await ProduceEventAsync(evt, cancellationToken);
            }
        }

        // ical.flush.interval.ms: rotate a partially filled file once it has aged out.
        if (_outputMode == ICalConnectorConfig.OutputModeFile &&
            _eventCount > 0 &&
            DateTime.UtcNow - _lastFlushUtc >= TimeSpan.FromMilliseconds(_flushIntervalMs))
        {
            await FlushCalendarAsync(lastTopic, cancellationToken);
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        if (_outputMode == ICalConnectorConfig.OutputModeFile && _currentCalendar?.Events.Count > 0)
        {
            await FlushCalendarAsync("events", cancellationToken);
        }
    }

    private async Task ProduceEventAsync(CalendarEvent evt, CancellationToken cancellationToken)
    {
        var producer = Context?.Producer
            ?? throw new InvalidOperationException(
                $"Output mode '{ICalConnectorConfig.OutputModeRecord}' emits the ICS to '{_outputTopic}', " +
                "but the runtime provided no producer.");

        var calendar = CreateCalendar();
        calendar.Events.Add(evt);

        var key = evt.Uid is { Length: > 0 } uid ? Encoding.UTF8.GetBytes(uid) : null;
        var value = Encoding.UTF8.GetBytes(SerializeCalendar(calendar));

        await producer.ProduceAsync(_outputTopic, key, value, cancellationToken);
    }

    private Calendar CreateCalendar()
    {
        var calendar = new Calendar();
        calendar.AddProperty("X-WR-CALNAME", _calendarName);
        calendar.ProductId = _calendarProductId;
        return calendar;
    }

    /// <summary>
    /// Parses a timestamp for <see cref="CalDateTime"/>, which only accepts Utc or
    /// Unspecified kinds — a plain TryParse turns a trailing 'Z' into local time.
    /// </summary>
    private static bool TryParseCalendarDate(string value, out DateTime result)
    {
        return DateTime.TryParse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AdjustToUniversal | DateTimeStyles.AllowWhiteSpaces,
            out result);
    }

    private CalendarEvent CreateEvent(JsonElement root, SinkRecord record)
    {
        var evt = new CalendarEvent();

        // UID
        if (TryGetString(root, _uidField, out var uid))
        {
            evt.Uid = uid;
        }
        else if (record.Key != null && record.Key.Length > 0)
        {
            evt.Uid = Encoding.UTF8.GetString(record.Key);
        }
        else
        {
            evt.Uid = Guid.NewGuid().ToString();
        }

        // Summary
        if (TryGetString(root, _summaryField, out var summary))
        {
            evt.Summary = summary;
        }

        // Description
        if (TryGetString(root, _descriptionField, out var description))
        {
            evt.Description = description;
        }

        // Location
        if (TryGetString(root, _locationField, out var location))
        {
            evt.Location = location;
        }

        // Start time
        if (TryGetString(root, _startField, out var startStr) &&
            TryParseCalendarDate(startStr, out var startDt))
        {
            evt.DtStart = new CalDateTime(startDt);
        }
        else
        {
            evt.DtStart = new CalDateTime(DateTime.UtcNow);
        }

        // End time
        if (TryGetString(root, _endField, out var endStr) &&
            TryParseCalendarDate(endStr, out var endDt))
        {
            evt.DtEnd = new CalDateTime(endDt);
        }
        else
        {
            evt.DtEnd = new CalDateTime(evt.DtStart.Value.AddMinutes(_defaultDurationMinutes));
        }

        // Additional fields
        if (TryGetString(root, "status", out var status))
        {
            evt.Status = status;
        }

        if (TryGetInt(root, "priority", out var priority))
        {
            evt.Priority = priority;
        }

        if (TryGetString(root, "categories", out var categories))
        {
            evt.Categories.Add(categories);
        }

        evt.Created = new CalDateTime(DateTime.UtcNow);
        evt.LastModified = new CalDateTime(DateTime.UtcNow);

        return evt;
    }

    private static bool TryGetString(JsonElement element, string propertyName, out string value)
    {
        value = "";
        if (element.TryGetProperty(propertyName, out var prop) &&
            prop.ValueKind == JsonValueKind.String)
        {
            value = prop.GetString() ?? "";
            return !string.IsNullOrEmpty(value);
        }
        return false;
    }

    private static bool TryGetInt(JsonElement element, string propertyName, out int value)
    {
        value = 0;
        if (element.TryGetProperty(propertyName, out var prop) &&
            prop.ValueKind == JsonValueKind.Number)
        {
            value = prop.GetInt32();
            return true;
        }
        return false;
    }

    private void FlushCalendar()
    {
        FlushCalendarAsync("events", CancellationToken.None).GetAwaiter().GetResult();
    }

    private async Task FlushCalendarAsync(string topic, CancellationToken cancellationToken)
    {
        if (_currentCalendar == null || _currentCalendar.Events.Count == 0)
            return;

        var icsContent = SerializeCalendar(_currentCalendar);
        var outputPath = ResolveOutputPath(topic);

        // Ensure directory exists
        var dir = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
        {
            Directory.CreateDirectory(dir);
        }

        await File.WriteAllTextAsync(outputPath, icsContent, cancellationToken);

        // Reset for next batch
        _currentCalendar = CreateCalendar();
        _eventCount = 0;
        _lastFlushUtc = DateTime.UtcNow;
    }

    /// <summary>
    /// Resolves the rotation target. A path without a ${timestamp} placeholder - or two
    /// rotations inside the same second - would resolve to a file that already holds
    /// flushed events, so the name is uniquified instead of overwriting them.
    /// </summary>
    private string ResolveOutputPath(string topic)
    {
        var outputPath = _outputPath
            .Replace("${topic}", topic, StringComparison.Ordinal)
            .Replace("${timestamp}", DateTime.UtcNow.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture),
                StringComparison.Ordinal);

        if (!File.Exists(outputPath))
            return outputPath;

        var dir = Path.GetDirectoryName(outputPath);
        var stem = Path.GetFileNameWithoutExtension(outputPath);
        var extension = Path.GetExtension(outputPath);

        while (true)
        {
            _fileSequence++;
            var name = $"{stem}-{_fileSequence.ToString(CultureInfo.InvariantCulture)}{extension}";
            var rotated = string.IsNullOrEmpty(dir) ? name : Path.Combine(dir, name);

            if (!File.Exists(rotated))
                return rotated;
        }
    }

    private static string SerializeCalendar(Calendar calendar)
    {
        var serializer = new CalendarSerializer();
        return serializer.SerializeToString(calendar) ?? string.Empty;
    }
}
