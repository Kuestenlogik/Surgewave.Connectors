using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.HttpServer;

/// <summary>
/// A sink connector that serves topic data via an HTTP REST API.
/// Consumed messages are available through GET endpoints.
/// </summary>
[ConnectorMetadata(
    Name = "HTTP Server Sink",
    Description = "Serves topic data via HTTP REST API endpoints for consumption by external clients.",
    Author = "KL Surgewave",
    Tags = "http,server,rest,api,serve,query",
    Icon = "Server")]
public sealed class HttpServerSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(HttpServerSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        // Server settings
        .Define(HttpServerConnectorConfig.Host, ConfigType.String, HttpServerConnectorConfig.DefaultHost, Importance.Medium,
            "Host/IP to bind the HTTP server to")
        .Define(HttpServerConnectorConfig.Port, ConfigType.Int, HttpServerConnectorConfig.DefaultPort, Importance.High,
            "Port to listen on")
        .Define(HttpServerConnectorConfig.BasePath, ConfigType.String, HttpServerConnectorConfig.DefaultBasePath, Importance.Medium,
            "Base path prefix for all endpoints")

        // Sink settings
        .Define(HttpServerConnectorConfig.SinkTopics, ConfigType.String, Importance.High,
            "Topics to consume and serve (comma-separated)")
        .Define(HttpServerConnectorConfig.SinkMaxMessages, ConfigType.Int, HttpServerConnectorConfig.DefaultSinkMaxMessages, Importance.Medium,
            "Maximum number of messages to buffer per topic")
        .Define(HttpServerConnectorConfig.SinkDefaultLimit, ConfigType.Int, HttpServerConnectorConfig.DefaultSinkDefaultLimit, Importance.Low,
            "Default number of messages returned per request")
        .Define(HttpServerConnectorConfig.SinkEnableStreaming, ConfigType.Boolean, false, Importance.Low,
            "Enable Server-Sent Events streaming endpoint")

        // CORS
        .Define(HttpServerConnectorConfig.EnableCors, ConfigType.Boolean, false, Importance.Low,
            "Enable CORS support")
        .Define(HttpServerConnectorConfig.CorsOrigins, ConfigType.String, "*", Importance.Low,
            "Allowed CORS origins (comma-separated, or * for all)")

        // Authentication
        .Define(HttpServerConnectorConfig.AuthEnabled, ConfigType.Boolean, false, Importance.Medium,
            "Enable authentication for API requests")
        .Define(HttpServerConnectorConfig.AuthType, ConfigType.String, HttpServerConnectorConfig.AuthTypeNone, Importance.Medium,
            "Authentication type: none, api_key, or basic")
        .Define(HttpServerConnectorConfig.AuthApiKeys, ConfigType.Password, "", Importance.Medium,
            "Comma-separated list of valid API keys")
        .Define(HttpServerConnectorConfig.AuthApiKeyHeader, ConfigType.String, HttpServerConnectorConfig.DefaultApiKeyHeader, Importance.Low,
            "Header name for API key authentication")
        .Define(HttpServerConnectorConfig.AuthBasicUsers, ConfigType.Password, "", Importance.Medium,
            "Basic auth users in format user1:pass1,user2:pass2");

    public override void Start(IDictionary<string, string> config)
    {
        _config = new Dictionary<string, string>(config);

        // Validate required settings
        if (!config.TryGetValue(HttpServerConnectorConfig.SinkTopics, out var topics) || string.IsNullOrEmpty(topics))
        {
            throw new ArgumentException($"Missing required config: {HttpServerConnectorConfig.SinkTopics}");
        }

        // Validate port
        var port = config.TryGetValue(HttpServerConnectorConfig.Port, out var portStr) && int.TryParse(portStr, out var p)
            ? p : HttpServerConnectorConfig.DefaultPort;
        if (port < 1 || port > 65535)
        {
            throw new ArgumentException($"Invalid port: {port}. Must be between 1 and 65535.");
        }

        // Validate auth config
        var authEnabled = config.TryGetValue(HttpServerConnectorConfig.AuthEnabled, out var ae) && bool.TryParse(ae, out var enabled) && enabled;
        if (authEnabled)
        {
            var authType = config.TryGetValue(HttpServerConnectorConfig.AuthType, out var at) ? at : HttpServerConnectorConfig.AuthTypeNone;
            ValidateAuthConfig(authType, config);
        }
    }

    private static void ValidateAuthConfig(string authType, IDictionary<string, string> config)
    {
        switch (authType)
        {
            case HttpServerConnectorConfig.AuthTypeApiKey:
                if (!config.TryGetValue(HttpServerConnectorConfig.AuthApiKeys, out var keys) || string.IsNullOrEmpty(keys))
                    throw new ArgumentException($"API key auth requires {HttpServerConnectorConfig.AuthApiKeys}");
                break;

            case HttpServerConnectorConfig.AuthTypeBasic:
                if (!config.TryGetValue(HttpServerConnectorConfig.AuthBasicUsers, out var users) || string.IsNullOrEmpty(users))
                    throw new ArgumentException($"Basic auth requires {HttpServerConnectorConfig.AuthBasicUsers}");
                break;
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - one HTTP server instance
        return [new Dictionary<string, string>(_config)];
    }
}
