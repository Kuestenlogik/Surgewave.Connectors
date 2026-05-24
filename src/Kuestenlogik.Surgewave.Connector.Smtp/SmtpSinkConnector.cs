using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Smtp;

/// <summary>
/// Sink connector that sends emails via SMTP.
/// </summary>
public class SmtpSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(SmtpSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        // Topics
        .Define(SmtpConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)

        // SMTP server
        .Define(SmtpConnectorConfig.HostConfig, ConfigType.String, Importance.High,
            "SMTP server hostname")
        .Define(SmtpConnectorConfig.PortConfig, ConfigType.Int, SmtpConnectorConfig.DefaultPort, Importance.Medium,
            "SMTP server port (default: 587 for STARTTLS, 465 for SSL)")
        .Define(SmtpConnectorConfig.UsernameConfig, ConfigType.String, Importance.Medium,
            "SMTP authentication username")
        .Define(SmtpConnectorConfig.PasswordConfig, ConfigType.Password, Importance.Medium,
            "SMTP authentication password")
        .Define(SmtpConnectorConfig.UseSslConfig, ConfigType.Boolean, SmtpConnectorConfig.DefaultUseSsl, Importance.Medium,
            "Use implicit SSL/TLS connection (port 465)")
        .Define(SmtpConnectorConfig.UseStartTlsConfig, ConfigType.Boolean, SmtpConnectorConfig.DefaultUseStartTls, Importance.Medium,
            "Use STARTTLS to upgrade connection (port 587)")
        .Define(SmtpConnectorConfig.TimeoutSecondsConfig, ConfigType.Int, SmtpConnectorConfig.DefaultTimeoutSeconds, Importance.Low,
            "Connection and send timeout in seconds")
        .Define(SmtpConnectorConfig.AcceptInvalidCertificatesConfig, ConfigType.Boolean, false, Importance.Low,
            "Accept invalid SSL certificates (not recommended for production)")

        // Email defaults
        .Define(SmtpConnectorConfig.FromAddressConfig, ConfigType.String, Importance.High,
            "Default sender email address")
        .Define(SmtpConnectorConfig.FromNameConfig, ConfigType.String, Importance.Low,
            "Default sender display name")
        .Define(SmtpConnectorConfig.ReplyToConfig, ConfigType.String, Importance.Low,
            "Reply-to email address")
        .Define(SmtpConnectorConfig.DefaultSubjectConfig, ConfigType.String, SmtpConnectorConfig.DefaultSubject, Importance.Low,
            "Default email subject if not specified in record")

        // Field mappings
        .Define(SmtpConnectorConfig.ToFieldConfig, ConfigType.String, "to", Importance.Medium,
            "JSON field containing recipient email address(es)")
        .Define(SmtpConnectorConfig.CcFieldConfig, ConfigType.String, "cc", Importance.Low,
            "JSON field containing CC email address(es)")
        .Define(SmtpConnectorConfig.BccFieldConfig, ConfigType.String, "bcc", Importance.Low,
            "JSON field containing BCC email address(es)")
        .Define(SmtpConnectorConfig.SubjectFieldConfig, ConfigType.String, "subject", Importance.Medium,
            "JSON field containing email subject")
        .Define(SmtpConnectorConfig.BodyFieldConfig, ConfigType.String, "body", Importance.Medium,
            "JSON field containing plain text body")
        .Define(SmtpConnectorConfig.BodyHtmlFieldConfig, ConfigType.String, "bodyHtml", Importance.Medium,
            "JSON field containing HTML body")
        .Define(SmtpConnectorConfig.AttachmentsFieldConfig, ConfigType.String, "attachments", Importance.Low,
            "JSON field containing attachments array")
        .Define(SmtpConnectorConfig.HeadersFieldConfig, ConfigType.String, "headers", Importance.Low,
            "JSON field containing custom headers object")

        // Templates
        .Define(SmtpConnectorConfig.BodyTemplateConfig, ConfigType.String, Importance.Low,
            "Plain text body template with ${field} placeholders", EditorHint.Multiline)
        .Define(SmtpConnectorConfig.BodyHtmlTemplateConfig, ConfigType.String, Importance.Low,
            "HTML body template with ${field} placeholders", EditorHint.Multiline)
        .Define(SmtpConnectorConfig.SubjectTemplateConfig, ConfigType.String, Importance.Low,
            "Subject template with ${field} placeholders", EditorHint.Multiline)

        // Behavior
        .Define(SmtpConnectorConfig.SendAsHtmlConfig, ConfigType.Boolean, SmtpConnectorConfig.DefaultSendAsHtml, Importance.Low,
            "Send body as HTML by default")
        .Define(SmtpConnectorConfig.BatchSizeConfig, ConfigType.Int, SmtpConnectorConfig.DefaultBatchSize, Importance.Low,
            "Maximum emails per connection")
        .Define(SmtpConnectorConfig.RetryCountConfig, ConfigType.Int, SmtpConnectorConfig.DefaultRetryCount, Importance.Low,
            "Number of retry attempts on failure")
        .Define(SmtpConnectorConfig.RetryDelayMsConfig, ConfigType.Int, SmtpConnectorConfig.DefaultRetryDelayMs, Importance.Low,
            "Delay between retries in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        // Validate required config
        if (!config.TryGetValue(SmtpConnectorConfig.TopicsConfig, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"Missing required configuration: {SmtpConnectorConfig.TopicsConfig}");
        }

        if (!config.TryGetValue(SmtpConnectorConfig.HostConfig, out var host) ||
            string.IsNullOrWhiteSpace(host))
        {
            throw new ArgumentException($"Missing required configuration: {SmtpConnectorConfig.HostConfig}");
        }

        if (!config.TryGetValue(SmtpConnectorConfig.FromAddressConfig, out var fromAddress) ||
            string.IsNullOrWhiteSpace(fromAddress))
        {
            throw new ArgumentException($"Missing required configuration: {SmtpConnectorConfig.FromAddressConfig}");
        }
    }

    public override void Stop()
    {
        // No cleanup needed
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // SMTP connector uses a single task - emails are sent sequentially
        return [new Dictionary<string, string>(_config)];
    }
}
