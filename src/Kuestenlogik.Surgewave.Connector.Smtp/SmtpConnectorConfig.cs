namespace Kuestenlogik.Surgewave.Connector.Smtp;

/// <summary>
/// Configuration constants for the SMTP connector.
/// </summary>
public static class SmtpConnectorConfig
{
    // Common config
    public const string TopicsConfig = "topics";

    // SMTP server settings
    public const string HostConfig = "smtp.host";
    public const string PortConfig = "smtp.port";
    public const string UsernameConfig = "smtp.username";
    public const string PasswordConfig = "smtp.password";
    public const string UseSslConfig = "smtp.use.ssl";
    public const string UseStartTlsConfig = "smtp.use.starttls";
    public const string TimeoutSecondsConfig = "smtp.timeout.seconds";
    public const string AcceptInvalidCertificatesConfig = "smtp.accept.invalid.certificates";

    // Email defaults
    public const string FromAddressConfig = "smtp.from.address";
    public const string FromNameConfig = "smtp.from.name";
    public const string ReplyToConfig = "smtp.reply.to";
    public const string DefaultSubjectConfig = "smtp.default.subject";

    // Email field mappings (JSON fields in record value)
    public const string ToFieldConfig = "smtp.to.field";
    public const string CcFieldConfig = "smtp.cc.field";
    public const string BccFieldConfig = "smtp.bcc.field";
    public const string SubjectFieldConfig = "smtp.subject.field";
    public const string BodyFieldConfig = "smtp.body.field";
    public const string BodyHtmlFieldConfig = "smtp.body.html.field";
    public const string AttachmentsFieldConfig = "smtp.attachments.field";
    public const string HeadersFieldConfig = "smtp.headers.field";

    // Template settings
    public const string BodyTemplateConfig = "smtp.body.template";
    public const string BodyHtmlTemplateConfig = "smtp.body.html.template";
    public const string SubjectTemplateConfig = "smtp.subject.template";

    // Behavior settings
    public const string SendAsHtmlConfig = "smtp.send.as.html";
    public const string BatchSizeConfig = "smtp.batch.size";
    public const string RetryCountConfig = "smtp.retry.count";
    public const string RetryDelayMsConfig = "smtp.retry.delay.ms";

    // Default values
    public const int DefaultPort = 587;
    public const int DefaultPortSsl = 465;
    public const int DefaultTimeoutSeconds = 30;
    public const bool DefaultUseSsl = false;
    public const bool DefaultUseStartTls = true;
    public const bool DefaultSendAsHtml = false;
    public const int DefaultBatchSize = 10;
    public const int DefaultRetryCount = 3;
    public const int DefaultRetryDelayMs = 1000;
    public const string DefaultSubject = "Message from Surgewave";
}
