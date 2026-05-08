namespace Kuestenlogik.Surgewave.Connector.Http;

using System.Text;

/// <summary>
/// Represents a parsed Server-Sent Event (SSE).
/// </summary>
/// <param name="Data">The event data (concatenated from all data: fields).</param>
/// <param name="EventType">The event type (from event: field), or null if not specified.</param>
/// <param name="Id">The event ID (from id: field), or null if not specified.</param>
/// <param name="ReceivedAt">When the event was received.</param>
public sealed record SseEvent(
    string Data,
    string? EventType,
    string? Id,
    DateTimeOffset ReceivedAt);

/// <summary>
/// Builder for constructing SSE events from streaming data.
/// Handles multi-line data fields by concatenating with newlines.
/// </summary>
internal sealed class SseEventBuilder
{
    private readonly StringBuilder _dataBuilder = new();
    private bool _hasData;

    public string? EventType { get; set; }
    public string? Id { get; set; }

    public bool HasData => _hasData;

    public void AppendData(string value)
    {
        if (_hasData)
        {
            _dataBuilder.Append('\n');
        }
        _dataBuilder.Append(value);
        _hasData = true;
    }

    public SseEvent Build()
    {
        return new SseEvent(
            Data: _dataBuilder.ToString(),
            EventType: EventType,
            Id: Id,
            ReceivedAt: DateTimeOffset.UtcNow);
    }
}
