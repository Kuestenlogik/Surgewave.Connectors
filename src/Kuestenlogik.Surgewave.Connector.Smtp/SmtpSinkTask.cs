using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using MailKit.Net.Smtp;
using MailKit.Security;
using MimeKit;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Smtp;

/// <summary>
/// Task that sends emails via SMTP.
/// </summary>
public sealed partial class SmtpSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _host = "";
    private int _port = SmtpConnectorConfig.DefaultPort;
    private string _username = "";
    private string _password = "";
    private bool _useSsl;
    private bool _useStartTls = SmtpConnectorConfig.DefaultUseStartTls;
    private int _timeoutSeconds = SmtpConnectorConfig.DefaultTimeoutSeconds;
    private bool _acceptInvalidCertificates;

    private string _fromAddress = "";
    private string _fromName = "";
    private string _replyTo = "";
    private string _defaultSubject = SmtpConnectorConfig.DefaultSubject;

    private string _toField = "to";
    private string _ccField = "cc";
    private string _bccField = "bcc";
    private string _subjectField = "subject";
    private string _bodyField = "body";
    private string _bodyHtmlField = "bodyHtml";
    private string _attachmentsField = "attachments";
    private string _headersField = "headers";

    private string _bodyTemplate = "";
    private string _bodyHtmlTemplate = "";
    private string _subjectTemplate = "";

    private bool _sendAsHtml;
    private int _batchSize = SmtpConnectorConfig.DefaultBatchSize;
    private int _retryCount = SmtpConnectorConfig.DefaultRetryCount;
    private int _retryDelayMs = SmtpConnectorConfig.DefaultRetryDelayMs;

    private SmtpClient? _client;
    private int _messagesSinceConnect;

    public override void Start(IDictionary<string, string> config)
    {
        _host = config[SmtpConnectorConfig.HostConfig];
        _port = config.TryGetValue(SmtpConnectorConfig.PortConfig, out var port)
            ? int.Parse(port) : SmtpConnectorConfig.DefaultPort;
        _username = config.TryGetValue(SmtpConnectorConfig.UsernameConfig, out var user) ? user : "";
        _password = config.TryGetValue(SmtpConnectorConfig.PasswordConfig, out var pass) ? pass : "";
        _useSsl = config.TryGetValue(SmtpConnectorConfig.UseSslConfig, out var ssl) && bool.Parse(ssl);
        _useStartTls = !config.TryGetValue(SmtpConnectorConfig.UseStartTlsConfig, out var tls) || bool.Parse(tls);
        _timeoutSeconds = config.TryGetValue(SmtpConnectorConfig.TimeoutSecondsConfig, out var timeout)
            ? int.Parse(timeout) : SmtpConnectorConfig.DefaultTimeoutSeconds;
        _acceptInvalidCertificates = config.TryGetValue(SmtpConnectorConfig.AcceptInvalidCertificatesConfig, out var accept)
            && bool.Parse(accept);

        _fromAddress = config[SmtpConnectorConfig.FromAddressConfig];
        _fromName = config.TryGetValue(SmtpConnectorConfig.FromNameConfig, out var fn) ? fn : "";
        _replyTo = config.TryGetValue(SmtpConnectorConfig.ReplyToConfig, out var rt) ? rt : "";
        _defaultSubject = config.TryGetValue(SmtpConnectorConfig.DefaultSubjectConfig, out var ds)
            ? ds : SmtpConnectorConfig.DefaultSubject;

        _toField = config.TryGetValue(SmtpConnectorConfig.ToFieldConfig, out var tf) ? tf : "to";
        _ccField = config.TryGetValue(SmtpConnectorConfig.CcFieldConfig, out var cf) ? cf : "cc";
        _bccField = config.TryGetValue(SmtpConnectorConfig.BccFieldConfig, out var bf) ? bf : "bcc";
        _subjectField = config.TryGetValue(SmtpConnectorConfig.SubjectFieldConfig, out var sf) ? sf : "subject";
        _bodyField = config.TryGetValue(SmtpConnectorConfig.BodyFieldConfig, out var bodyF) ? bodyF : "body";
        _bodyHtmlField = config.TryGetValue(SmtpConnectorConfig.BodyHtmlFieldConfig, out var htmlF) ? htmlF : "bodyHtml";
        _attachmentsField = config.TryGetValue(SmtpConnectorConfig.AttachmentsFieldConfig, out var af) ? af : "attachments";
        _headersField = config.TryGetValue(SmtpConnectorConfig.HeadersFieldConfig, out var hf) ? hf : "headers";

        _bodyTemplate = config.TryGetValue(SmtpConnectorConfig.BodyTemplateConfig, out var bt) ? bt : "";
        _bodyHtmlTemplate = config.TryGetValue(SmtpConnectorConfig.BodyHtmlTemplateConfig, out var ht) ? ht : "";
        _subjectTemplate = config.TryGetValue(SmtpConnectorConfig.SubjectTemplateConfig, out var st) ? st : "";

        _sendAsHtml = config.TryGetValue(SmtpConnectorConfig.SendAsHtmlConfig, out var html) && bool.Parse(html);
        _batchSize = config.TryGetValue(SmtpConnectorConfig.BatchSizeConfig, out var bs)
            ? int.Parse(bs) : SmtpConnectorConfig.DefaultBatchSize;
        _retryCount = config.TryGetValue(SmtpConnectorConfig.RetryCountConfig, out var rc)
            ? int.Parse(rc) : SmtpConnectorConfig.DefaultRetryCount;
        _retryDelayMs = config.TryGetValue(SmtpConnectorConfig.RetryDelayMsConfig, out var rd)
            ? int.Parse(rd) : SmtpConnectorConfig.DefaultRetryDelayMs;
    }

    public override void Stop()
    {
        DisconnectClient();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisconnectClient();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0 || records.All(r => r.Value == null || r.Value.Length == 0))
            return;

        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0)
                continue;

            MimeMessage? message = null;
            try
            {
                message = BuildMessage(record);
                if (message != null)
                {
                    await SendWithRetryAsync(message, cancellationToken);
                }
            }
            catch (JsonException)
            {
                // Skip invalid JSON records
            }
            finally
            {
                message?.Dispose();
            }
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // Nothing to flush - emails are sent immediately
        return Task.CompletedTask;
    }

    private MimeMessage? BuildMessage(SinkRecord record)
    {
        var rawValue = Encoding.UTF8.GetString(record.Value);
        using var doc = JsonDocument.Parse(rawValue);
        var root = doc.RootElement;

        // Get recipients
        var toAddresses = GetAddresses(root, _toField);
        if (toAddresses.Count == 0)
            return null;

        MimeMessage? message = null;
        try
        {
            message = new MimeMessage();

            // From
            if (!string.IsNullOrWhiteSpace(_fromName))
                message.From.Add(new MailboxAddress(_fromName, _fromAddress));
            else
                message.From.Add(MailboxAddress.Parse(_fromAddress));

            // To
            foreach (var addr in toAddresses)
                message.To.Add(addr);

            // CC
            foreach (var addr in GetAddresses(root, _ccField))
                message.Cc.Add(addr);

            // BCC
            foreach (var addr in GetAddresses(root, _bccField))
                message.Bcc.Add(addr);

            // Reply-To
            if (!string.IsNullOrWhiteSpace(_replyTo))
                message.ReplyTo.Add(MailboxAddress.Parse(_replyTo));

            // Subject
            var subject = GetSubject(root, record);
            message.Subject = subject;

            // Body
            var builder = new BodyBuilder();
            var (textBody, htmlBody) = GetBody(root, record);

            if (!string.IsNullOrWhiteSpace(textBody))
                builder.TextBody = textBody;

            if (!string.IsNullOrWhiteSpace(htmlBody))
                builder.HtmlBody = htmlBody;

            // If no HTML body but sendAsHtml is true, use text as HTML
            if (string.IsNullOrWhiteSpace(htmlBody) && _sendAsHtml && !string.IsNullOrWhiteSpace(textBody))
                builder.HtmlBody = $"<pre>{System.Net.WebUtility.HtmlEncode(textBody)}</pre>";

            // Attachments
            AddAttachments(root, builder);

            message.Body = builder.ToMessageBody();

            // Custom headers
            AddCustomHeaders(root, message);

            var result = message;
            message = null; // Transfer ownership
            return result;
        }
        finally
        {
            message?.Dispose();
        }
    }

    private static List<MailboxAddress> GetAddresses(JsonElement root, string fieldName)
    {
        var addresses = new List<MailboxAddress>();

        if (!root.TryGetProperty(fieldName, out var prop))
            return addresses;

        if (prop.ValueKind == JsonValueKind.String)
        {
            var value = prop.GetString();
            if (!string.IsNullOrWhiteSpace(value))
            {
                foreach (var addr in value.Split([',', ';'], StringSplitOptions.RemoveEmptyEntries))
                {
                    var trimmed = addr.Trim();
                    if (!string.IsNullOrWhiteSpace(trimmed))
                        addresses.Add(MailboxAddress.Parse(trimmed));
                }
            }
        }
        else if (prop.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in prop.EnumerateArray())
            {
                if (item.ValueKind == JsonValueKind.String)
                {
                    var addr = item.GetString();
                    if (!string.IsNullOrWhiteSpace(addr))
                        addresses.Add(MailboxAddress.Parse(addr.Trim()));
                }
            }
        }

        return addresses;
    }

    private string GetSubject(JsonElement root, SinkRecord record)
    {
        // First try subject field
        if (root.TryGetProperty(_subjectField, out var subjectProp) &&
            subjectProp.ValueKind == JsonValueKind.String)
        {
            var subject = subjectProp.GetString();
            if (!string.IsNullOrWhiteSpace(subject))
                return subject;
        }

        // Then try template
        if (!string.IsNullOrWhiteSpace(_subjectTemplate))
            return ApplyTemplate(_subjectTemplate, root, record);

        return _defaultSubject;
    }

    private (string? text, string? html) GetBody(JsonElement root, SinkRecord record)
    {
        string? textBody = null;
        string? htmlBody = null;

        // Get from fields
        if (root.TryGetProperty(_bodyField, out var bodyProp) &&
            bodyProp.ValueKind == JsonValueKind.String)
        {
            textBody = bodyProp.GetString();
        }

        if (root.TryGetProperty(_bodyHtmlField, out var htmlProp) &&
            htmlProp.ValueKind == JsonValueKind.String)
        {
            htmlBody = htmlProp.GetString();
        }

        // Apply templates if configured
        if (!string.IsNullOrWhiteSpace(_bodyTemplate))
            textBody = ApplyTemplate(_bodyTemplate, root, record);

        if (!string.IsNullOrWhiteSpace(_bodyHtmlTemplate))
            htmlBody = ApplyTemplate(_bodyHtmlTemplate, root, record);

        return (textBody, htmlBody);
    }

    private string ApplyTemplate(string template, JsonElement root, SinkRecord record)
    {
        var result = template;

        // Replace ${field} placeholders
        result = TemplatePlaceholderRegex().Replace(result, match =>
        {
            var fieldName = match.Groups[1].Value;

            // Special placeholders
            return fieldName switch
            {
                "topic" => record.Topic,
                "partition" => record.Partition.ToString(),
                "offset" => record.Offset.ToString(),
                "timestamp" => DateTimeOffset.UtcNow.ToString("o"),
                _ => root.TryGetProperty(fieldName, out var prop) && prop.ValueKind == JsonValueKind.String
                    ? prop.GetString() ?? ""
                    : ""
            };
        });

        return result;
    }

    [GeneratedRegex(@"\$\{(\w+)\}")]
    private static partial Regex TemplatePlaceholderRegex();

    private void AddAttachments(JsonElement root, BodyBuilder builder)
    {
        if (!root.TryGetProperty(_attachmentsField, out var attachmentsProp) ||
            attachmentsProp.ValueKind != JsonValueKind.Array)
            return;

        foreach (var attachment in attachmentsProp.EnumerateArray())
        {
            if (attachment.ValueKind != JsonValueKind.Object)
                continue;

            var name = attachment.TryGetProperty("name", out var nameProp)
                ? nameProp.GetString() ?? "attachment" : "attachment";

            var contentType = attachment.TryGetProperty("contentType", out var ctProp)
                ? ctProp.GetString() ?? "application/octet-stream" : "application/octet-stream";

            byte[]? data = null;

            if (attachment.TryGetProperty("content", out var contentProp))
            {
                var content = contentProp.GetString();
                if (!string.IsNullOrWhiteSpace(content))
                {
                    // Try base64 decode
                    try
                    {
                        data = Convert.FromBase64String(content);
                    }
                    catch (FormatException)
                    {
                        data = Encoding.UTF8.GetBytes(content);
                    }
                }
            }

            if (data != null)
            {
                builder.Attachments.Add(name, data, ContentType.Parse(contentType));
            }
        }
    }

    private static void AddCustomHeaders(JsonElement root, MimeMessage message)
    {
        if (!root.TryGetProperty("headers", out var headersProp) ||
            headersProp.ValueKind != JsonValueKind.Object)
            return;

        foreach (var header in headersProp.EnumerateObject())
        {
            if (header.Value.ValueKind == JsonValueKind.String)
            {
                var value = header.Value.GetString();
                if (!string.IsNullOrWhiteSpace(value))
                {
                    message.Headers.Add(header.Name, value);
                }
            }
        }
    }

    private async Task SendWithRetryAsync(MimeMessage message, CancellationToken cancellationToken)
    {
        var attempts = 0;
        while (true)
        {
            try
            {
                await EnsureConnectedAsync(cancellationToken);
                await _client!.SendAsync(message, cancellationToken);
                _messagesSinceConnect++;

                // Reconnect periodically to avoid connection issues
                if (_messagesSinceConnect >= _batchSize)
                {
                    DisconnectClient();
                }

                return;
            }
            catch (Exception) when (++attempts < _retryCount)
            {
                DisconnectClient();
                await Task.Delay(_retryDelayMs, cancellationToken);
            }
        }
    }

    private async Task EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        if (_client?.IsConnected == true)
            return;

        _client?.Dispose();
        _client = new SmtpClient
        {
            Timeout = _timeoutSeconds * 1000
        };

        if (_acceptInvalidCertificates)
        {
            // Intentional: This is an opt-in feature for development/testing environments
#pragma warning disable CA5359
            _client.ServerCertificateValidationCallback = (_, _, _, _) => true;
#pragma warning restore CA5359
        }

        var options = _useSsl
            ? SecureSocketOptions.SslOnConnect
            : _useStartTls
                ? SecureSocketOptions.StartTls
                : SecureSocketOptions.None;

        await _client.ConnectAsync(_host, _port, options, cancellationToken);

        if (!string.IsNullOrWhiteSpace(_username))
        {
            await _client.AuthenticateAsync(_username, _password, cancellationToken);
        }

        _messagesSinceConnect = 0;
    }

    private void DisconnectClient()
    {
        if (_client == null)
            return;

        try
        {
            if (_client.IsConnected)
            {
                _client.Disconnect(true);
            }
        }
        catch
        {
            // Ignore disconnect errors
        }
        finally
        {
            _client.Dispose();
            _client = null;
        }
    }
}
