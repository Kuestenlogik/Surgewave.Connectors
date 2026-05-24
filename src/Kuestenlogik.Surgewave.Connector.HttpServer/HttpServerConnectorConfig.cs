namespace Kuestenlogik.Surgewave.Connector.HttpServer;

/// <summary>
/// Configuration constants for the HTTP Server connector.
/// </summary>
public static class HttpServerConnectorConfig
{
    // Server settings
    public const string Host = "http.server.host";
    public const string Port = "http.server.port";
    public const string BasePath = "http.server.base.path";
    public const string EnableCors = "http.server.cors.enabled";
    public const string CorsOrigins = "http.server.cors.origins";

    // Source settings (receiving requests)
    public const string SourceTopic = "http.source.topic";
    public const string SourcePath = "http.source.path";
    public const string SourceMethods = "http.source.methods";
    public const string SourceIncludeHeaders = "http.source.include.headers";
    public const string SourceIncludeQueryParams = "http.source.include.query.params";

    // Sink settings (serving topic data)
    public const string SinkTopics = "http.sink.topics";
    public const string SinkMaxMessages = "http.sink.max.messages";
    public const string SinkDefaultLimit = "http.sink.default.limit";
    public const string SinkEnableStreaming = "http.sink.streaming.enabled";

    // Authentication
    public const string AuthEnabled = "http.auth.enabled";
    public const string AuthType = "http.auth.type";
    public const string AuthApiKeys = "http.auth.api.keys";
    public const string AuthApiKeyHeader = "http.auth.api.key.header";
    public const string AuthBasicUsers = "http.auth.basic.users";

    // Defaults
    public const string DefaultHost = "localhost";
    public const int DefaultPort = 8080;
    public const string DefaultBasePath = "/api";
    public const string DefaultSourcePath = "/ingest";
    public const string DefaultSourceMethods = "POST,PUT";
    public const int DefaultSinkMaxMessages = 10000;
    public const int DefaultSinkDefaultLimit = 100;
    public const string DefaultApiKeyHeader = "X-API-Key";

    // Auth types
    public const string AuthTypeNone = "none";
    public const string AuthTypeApiKey = "api_key";
    public const string AuthTypeBasic = "basic";
}
