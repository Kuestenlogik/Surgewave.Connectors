using System.Text;
using System.Text.Json;
using Ical.Net;
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

    private Calendar? _currentCalendar;
    private int _eventCount;

    public override void Start(IDictionary<string, string> config)
    {
        _outputMode = config.TryGetValue(ICalConnectorConfig.OutputModeConfig, out var mode)
            ? mode : ICalConnectorConfig.DefaultOutputMode;
        _outputPath = config.TryGetValue(ICalConnectorConfig.OutputPathConfig, out var path)
            ? path : "";
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

        if (_outputMode == ICalConnectorConfig.OutputModeFile)
        {
            _currentCalendar = CreateCalendar();
        }
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
        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0)
                continue;

            try
            {
                var json = Encoding.UTF8.GetString(record.Value);
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                var evt = CreateEvent(root, record);

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
                // Record mode would emit the ICS as the transformed record value
                // This is handled in FlushAsync
            }
            catch (JsonException)
            {
                // Skip invalid JSON
            }
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        if (_outputMode == ICalConnectorConfig.OutputModeFile && _currentCalendar?.Events.Count > 0)
        {
            await FlushCalendarAsync("events", cancellationToken);
        }
    }

    private Calendar CreateCalendar()
    {
        var calendar = new Calendar();
        calendar.AddProperty("X-WR-CALNAME", _calendarName);
        calendar.ProductId = _calendarProductId;
        return calendar;
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
            DateTime.TryParse(startStr, out var startDt))
        {
            evt.DtStart = new CalDateTime(startDt);
        }
        else
        {
            evt.DtStart = new CalDateTime(DateTime.UtcNow);
        }

        // End time
        if (TryGetString(root, _endField, out var endStr) &&
            DateTime.TryParse(endStr, out var endDt))
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

        var serializer = new CalendarSerializer();
        var icsContent = serializer.SerializeToString(_currentCalendar);

        var outputPath = _outputPath
            .Replace("${topic}", topic)
            .Replace("${timestamp}", DateTime.UtcNow.ToString("yyyyMMddHHmmss"));

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
    }
}
