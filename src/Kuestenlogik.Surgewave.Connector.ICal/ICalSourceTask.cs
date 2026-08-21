using System.Globalization;
using System.Text;
using System.Text.Json;
using Ical.Net;
using Ical.Net.CalendarComponents;
using Calendar = Ical.Net.Calendar;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.ICal;

internal static class JsonOptions
{
    public static readonly JsonSerializerOptions Default = new()
    {
        WriteIndented = false,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };
}

/// <summary>
/// Task that reads iCal/ICS files and produces calendar events as records.
/// </summary>
public sealed class ICalSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _sourceMode = ICalConnectorConfig.DefaultSourceMode;
    private string _topic = "";
    private Uri? _url;
    private string _filePath = "";
    private int _pollIntervalMs = ICalConnectorConfig.DefaultPollIntervalMs;
    private bool _includePastEvents;
    private int _timeWindowDays = ICalConnectorConfig.DefaultTimeWindowDays;
    private HttpClient? _httpClient;
    private DateTimeOffset _lastPollTime = DateTimeOffset.MinValue;
    private long _lastEventStampMs;
    private readonly HashSet<string> _emittedEventUids = [];
    private readonly Queue<string> _emittedEventOrder = new();
    private readonly Dictionary<string, object> _sourcePartition = [];

    /// <summary>Upper bound for the in-memory dedup set so a long-lived feed cannot grow it forever.</summary>
    private const int MaxTrackedEventKeys = 5000;

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[ICalConnectorConfig.TopicConfig];
        _sourceMode = config.TryGetValue(ICalConnectorConfig.SourceModeConfig, out var mode)
            ? mode : ICalConnectorConfig.DefaultSourceMode;

        _url = config.TryGetValue(ICalConnectorConfig.UrlConfig, out var url) && !string.IsNullOrWhiteSpace(url)
            ? new Uri(url)
            : null;
        _filePath = config.TryGetValue(ICalConnectorConfig.FilePathConfig, out var path) ? path : "";

        _pollIntervalMs = config.TryGetValue(ICalConnectorConfig.PollIntervalMsConfig, out var poll)
            ? int.Parse(poll) : ICalConnectorConfig.DefaultPollIntervalMs;
        _includePastEvents = config.TryGetValue(ICalConnectorConfig.IncludePastEventsConfig, out var past)
            && bool.Parse(past);
        _timeWindowDays = config.TryGetValue(ICalConnectorConfig.TimeWindowDaysConfig, out var window)
            ? int.Parse(window) : ICalConnectorConfig.DefaultTimeWindowDays;

        var timeoutMs = config.TryGetValue(ICalConnectorConfig.TimeoutMsConfig, out var timeout)
            ? int.Parse(timeout) : ICalConnectorConfig.DefaultTimeoutMs;

        if (_sourceMode == ICalConnectorConfig.SourceModeUrl)
        {
            _sourcePartition["url"] = _url?.ToString() ?? "";

            _httpClient = new HttpClient { Timeout = TimeSpan.FromMilliseconds(timeoutMs) };

            // Add auth header
            var authHeader = config.TryGetValue(ICalConnectorConfig.AuthHeaderConfig, out var ah)
                ? ah : ICalConnectorConfig.DefaultAuthHeader;
            if (config.TryGetValue(ICalConnectorConfig.AuthTokenConfig, out var authToken) &&
                !string.IsNullOrWhiteSpace(authToken))
            {
                _httpClient.DefaultRequestHeaders.Add(authHeader, authToken);
            }

            // Add custom headers
            if (config.TryGetValue(ICalConnectorConfig.HeadersConfig, out var headers) &&
                !string.IsNullOrWhiteSpace(headers))
            {
                foreach (var header in headers.Split(';', StringSplitOptions.RemoveEmptyEntries))
                {
                    var parts = header.Split('=', 2);
                    if (parts.Length == 2)
                    {
                        _httpClient.DefaultRequestHeaders.TryAddWithoutValidation(
                            parts[0].Trim(), parts[1].Trim());
                    }
                }
            }
        }
        else
        {
            _sourcePartition["file"] = _filePath;
        }

        // Restore offset
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null)
        {
            if (storedOffset.TryGetValue(ICalConnectorConfig.OffsetLastPoll, out var lastPoll))
                _lastPollTime = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(lastPoll, CultureInfo.InvariantCulture));

            // Without the change watermark and the last committed event key, every restart
            // would re-emit the whole time window as duplicates.
            if (storedOffset.TryGetValue(ICalConnectorConfig.OffsetLastModified, out var lastModified))
                _lastEventStampMs = Convert.ToInt64(lastModified, CultureInfo.InvariantCulture);

            if (storedOffset.TryGetValue(ICalConnectorConfig.OffsetLastEventUid, out var lastEventUid) &&
                lastEventUid?.ToString() is { Length: > 0 } lastEventKey)
            {
                TrackEmitted(lastEventKey);
            }
        }
    }

    public override void Stop()
    {
        _httpClient?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _httpClient?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var elapsed = (now - _lastPollTime).TotalMilliseconds;

        if (elapsed < _pollIntervalMs)
        {
            var waitTime = (int)(_pollIntervalMs - elapsed);
            await Task.Delay(waitTime, cancellationToken);
        }

        try
        {
            string icsContent;

            if (_sourceMode == ICalConnectorConfig.SourceModeUrl)
            {
                if (_httpClient == null || _url == null)
                    return [];

                var response = await _httpClient.GetAsync(_url, cancellationToken);
                response.EnsureSuccessStatusCode();
                icsContent = await response.Content.ReadAsStringAsync(cancellationToken);
            }
            else
            {
                if (!File.Exists(_filePath))
                    return [];

                icsContent = await File.ReadAllTextAsync(_filePath, cancellationToken);
            }

            var calendar = Calendar.Load(icsContent);
            if (calendar == null) return [];

            var records = new List<SourceRecord>();

            var windowStart = _includePastEvents ? DateTime.MinValue : DateTime.UtcNow;
            var windowEnd = DateTime.UtcNow.AddDays(_timeWindowDays);

            var candidates = new List<(CalendarEvent Event, string Key, long StampMs)>();

            foreach (var evt in calendar.Events)
            {
                // Skip if already emitted (for incremental polling)
                var eventUid = evt.Uid ?? Guid.NewGuid().ToString();
                var eventKey = $"{eventUid}_{evt.DtStart?.Value:yyyyMMddHHmmss}";

                if (_emittedEventUids.Contains(eventKey))
                    continue;

                // Check time window
                var eventStart = evt.DtStart?.Value ?? DateTime.MinValue;
                var eventEnd = evt.DtEnd?.Value ?? eventStart;

                if (!_includePastEvents && eventEnd < windowStart)
                    continue;

                if (eventStart > windowEnd)
                    continue;

                // The persisted watermark is the change stamp of the last committed event;
                // anything strictly older than it was already emitted before the restart.
                var stampMs = ChangeStampMs(evt);
                if (stampMs < _lastEventStampMs)
                    continue;

                candidates.Add((evt, eventKey, stampMs));
            }

            // Emit the oldest change first so the committed watermark never moves backwards.
            candidates.Sort(static (left, right) => left.StampMs.CompareTo(right.StampMs));

            foreach (var (evt, eventKey, stampMs) in candidates)
            {
                records.Add(CreateEventRecord(evt, eventKey, stampMs));
                TrackEmitted(eventKey);

                if (stampMs > _lastEventStampMs)
                    _lastEventStampMs = stampMs;
            }

            _lastPollTime = DateTimeOffset.UtcNow;
            return records;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (HttpRequestException ex)
        {
            Context?.RaiseError?.Invoke(ex);
            await Task.Delay(5000, cancellationToken);
            return [];
        }
        catch (IOException ex)
        {
            Context?.RaiseError?.Invoke(ex);
            await Task.Delay(5000, cancellationToken);
            return [];
        }
        catch (Exception ex)
        {
            // Calendar parsing error - surface it, a permanently malformed feed must not
            // fail silently forever.
            Context?.RaiseError?.Invoke(ex);
            _lastPollTime = DateTimeOffset.UtcNow;
            return [];
        }
    }

    /// <summary>
    /// The change stamp an event is ordered and deduplicated by across restarts.
    /// </summary>
    private static long ChangeStampMs(CalendarEvent evt)
    {
        var stamp = evt.LastModified?.Value ?? evt.Created?.Value ?? evt.DtStart?.Value;

        return stamp.HasValue
            ? new DateTimeOffset(DateTime.SpecifyKind(stamp.Value, DateTimeKind.Utc)).ToUnixTimeMilliseconds()
            : 0;
    }

    private void TrackEmitted(string eventKey)
    {
        if (!_emittedEventUids.Add(eventKey))
            return;

        _emittedEventOrder.Enqueue(eventKey);

        while (_emittedEventOrder.Count > MaxTrackedEventKeys)
        {
            _emittedEventUids.Remove(_emittedEventOrder.Dequeue());
        }
    }

    private SourceRecord CreateEventRecord(CalendarEvent evt, string eventKey, long stampMs)
    {
        var eventData = new Dictionary<string, object?>
        {
            ["uid"] = evt.Uid,
            ["summary"] = evt.Summary,
            ["description"] = evt.Description,
            ["location"] = evt.Location,
            ["start"] = evt.DtStart?.Value.ToString("o"),
            ["end"] = evt.DtEnd?.Value.ToString("o"),
            ["startTimezone"] = evt.DtStart?.TzId,
            ["endTimezone"] = evt.DtEnd?.TzId,
            ["allDay"] = evt.IsAllDay,
            ["status"] = evt.Status,
            ["created"] = evt.Created?.Value.ToString("o"),
            ["lastModified"] = evt.LastModified?.Value.ToString("o"),
            ["sequence"] = evt.Sequence,
            ["transparency"] = evt.Transparency?.ToString(),
            ["priority"] = evt.Priority,
            ["organizer"] = evt.Organizer?.Value?.ToString(),
            ["categories"] = evt.Categories?.Count > 0 ? string.Join(",", evt.Categories) : null,
            ["recurrenceRule"] = evt.RecurrenceRule?.ToString()
        };

        // Add attendees
        if (evt.Attendees?.Count > 0)
        {
            eventData["attendees"] = evt.Attendees
                .Select(a => new Dictionary<string, object?>
                {
                    ["email"] = a.Value?.ToString(),
                    ["name"] = a.CommonName,
                    ["role"] = a.Role,
                    ["status"] = a.ParticipationStatus?.ToString()
                })
                .ToList();
        }

        var json = JsonSerializer.Serialize(eventData, JsonOptions.Default);

        var offset = new Dictionary<string, object>
        {
            [ICalConnectorConfig.OffsetLastPoll] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [ICalConnectorConfig.OffsetLastEventUid] = eventKey,
            [ICalConnectorConfig.OffsetLastModified] = stampMs
        };

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = offset,
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(evt.Uid ?? eventKey),
            Value = Encoding.UTF8.GetBytes(json),
            Timestamp = DateTimeOffset.UtcNow
        };
    }
}
