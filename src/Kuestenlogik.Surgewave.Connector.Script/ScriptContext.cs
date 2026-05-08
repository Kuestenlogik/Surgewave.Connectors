using System.Text;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Connector.Script;

/// <summary>
/// Context object exposed to scripts for record transformation.
/// </summary>
public sealed class ScriptContext
{
    /// <summary>
    /// The input record key as bytes.
    /// </summary>
    public byte[]? Key { get; init; }

    /// <summary>
    /// The input record value as bytes.
    /// </summary>
    public byte[]? Value { get; init; }

    /// <summary>
    /// The record timestamp.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// The source topic.
    /// </summary>
    public string Topic { get; init; } = "";

    /// <summary>
    /// The source partition.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// The record offset.
    /// </summary>
    public long Offset { get; init; }

    /// <summary>
    /// Record headers.
    /// </summary>
    public IReadOnlyDictionary<string, byte[]> Headers { get; init; } = new Dictionary<string, byte[]>();

    /// <summary>
    /// Custom metadata that can be passed between transformations.
    /// </summary>
    public IDictionary<string, object?> Metadata { get; } = new Dictionary<string, object?>();

    /// <summary>
    /// Gets the key as a UTF-8 string.
    /// </summary>
    public string? KeyString => Key != null ? Encoding.UTF8.GetString(Key) : null;

    /// <summary>
    /// Gets the value as a UTF-8 string.
    /// </summary>
    public string? ValueString => Value != null ? Encoding.UTF8.GetString(Value) : null;

    /// <summary>
    /// Parses the value as JSON.
    /// </summary>
    public JsonDocument? ValueJson => Value != null ? JsonDocument.Parse(Value) : null;

    /// <summary>
    /// Deserializes the value as the specified type.
    /// </summary>
    public T? ValueAs<T>() where T : class
    {
        return Value != null ? JsonSerializer.Deserialize<T>(Value) : null;
    }

    /// <summary>
    /// Gets a header value as a string.
    /// </summary>
    public string? GetHeader(string name)
    {
        return Headers.TryGetValue(name, out var value) ? Encoding.UTF8.GetString(value) : null;
    }
}

/// <summary>
/// Result of a script transformation.
/// </summary>
public sealed class ScriptResult
{
    /// <summary>
    /// Output records to emit.
    /// </summary>
    public List<ScriptOutput> Records { get; } = [];

    /// <summary>
    /// Whether to skip (drop) the input record.
    /// </summary>
    public bool Skip { get; set; }

    /// <summary>
    /// Emits a single output record.
    /// </summary>
    public void Emit(byte[]? key, byte[] value, string? topic = null)
    {
        Records.Add(new ScriptOutput { Key = key, Value = value, Topic = topic });
    }

    /// <summary>
    /// Emits a single output record with string values.
    /// </summary>
    public void Emit(string? key, string value, string? topic = null)
    {
        Emit(
            key != null ? Encoding.UTF8.GetBytes(key) : null,
            Encoding.UTF8.GetBytes(value),
            topic);
    }

    /// <summary>
    /// Emits an output record serialized as JSON.
    /// </summary>
    public void EmitJson<T>(string? key, T value, string? topic = null)
    {
        Emit(
            key != null ? Encoding.UTF8.GetBytes(key) : null,
            JsonSerializer.SerializeToUtf8Bytes(value),
            topic);
    }

    /// <summary>
    /// Emits an output record with the same key as input.
    /// </summary>
    public void EmitWithSameKey(byte[] value, string? topic = null)
    {
        Records.Add(new ScriptOutput { Value = value, Topic = topic, UseInputKey = true });
    }
}

/// <summary>
/// A single output record from a script.
/// </summary>
public sealed class ScriptOutput
{
    public byte[]? Key { get; init; }
    public byte[]? Value { get; init; }
    public string? Topic { get; init; }
    public bool UseInputKey { get; init; }
    public IDictionary<string, byte[]>? Headers { get; init; }
}
