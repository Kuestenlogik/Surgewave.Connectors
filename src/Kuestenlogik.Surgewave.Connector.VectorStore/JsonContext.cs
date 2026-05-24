using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Connector.VectorStore;

/// <summary>
/// Source-generated JSON serialization context for AOT compatibility and performance.
/// </summary>
[JsonSerializable(typeof(VectorEntry))]
[JsonSerializable(typeof(SearchResponse))]
[JsonSerializable(typeof(SearchResultItem))]
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.SnakeCaseLower,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
internal sealed partial class JsonContext : JsonSerializerContext;
