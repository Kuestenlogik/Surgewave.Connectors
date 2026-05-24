using Kuestenlogik.Surgewave.Connector.Smtp;

namespace Kuestenlogik.Surgewave.Connector.Smtp.Tests;

public class SmtpConnectorConfigTests
{
    [Fact]
    public void TopicsConfig_HasExpectedValue()
    {
        Assert.Equal("topics", SmtpConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void HostConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.host", SmtpConnectorConfig.HostConfig);
    }

    [Fact]
    public void PortConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.port", SmtpConnectorConfig.PortConfig);
    }

    [Fact]
    public void UsernameConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.username", SmtpConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void PasswordConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.password", SmtpConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void UseSslConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.use.ssl", SmtpConnectorConfig.UseSslConfig);
    }

    [Fact]
    public void UseStartTlsConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.use.starttls", SmtpConnectorConfig.UseStartTlsConfig);
    }

    [Fact]
    public void TimeoutSecondsConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.timeout.seconds", SmtpConnectorConfig.TimeoutSecondsConfig);
    }

    [Fact]
    public void AcceptInvalidCertificatesConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.accept.invalid.certificates", SmtpConnectorConfig.AcceptInvalidCertificatesConfig);
    }

    [Fact]
    public void FromAddressConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.from.address", SmtpConnectorConfig.FromAddressConfig);
    }

    [Fact]
    public void FromNameConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.from.name", SmtpConnectorConfig.FromNameConfig);
    }

    [Fact]
    public void ReplyToConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.reply.to", SmtpConnectorConfig.ReplyToConfig);
    }

    [Fact]
    public void DefaultSubjectConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.default.subject", SmtpConnectorConfig.DefaultSubjectConfig);
    }

    [Fact]
    public void ToFieldConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.to.field", SmtpConnectorConfig.ToFieldConfig);
    }

    [Fact]
    public void CcFieldConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.cc.field", SmtpConnectorConfig.CcFieldConfig);
    }

    [Fact]
    public void BccFieldConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.bcc.field", SmtpConnectorConfig.BccFieldConfig);
    }

    [Fact]
    public void SubjectFieldConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.subject.field", SmtpConnectorConfig.SubjectFieldConfig);
    }

    [Fact]
    public void BodyFieldConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.body.field", SmtpConnectorConfig.BodyFieldConfig);
    }

    [Fact]
    public void BodyHtmlFieldConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.body.html.field", SmtpConnectorConfig.BodyHtmlFieldConfig);
    }

    [Fact]
    public void AttachmentsFieldConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.attachments.field", SmtpConnectorConfig.AttachmentsFieldConfig);
    }

    [Fact]
    public void HeadersFieldConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.headers.field", SmtpConnectorConfig.HeadersFieldConfig);
    }

    [Fact]
    public void BodyTemplateConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.body.template", SmtpConnectorConfig.BodyTemplateConfig);
    }

    [Fact]
    public void BodyHtmlTemplateConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.body.html.template", SmtpConnectorConfig.BodyHtmlTemplateConfig);
    }

    [Fact]
    public void SubjectTemplateConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.subject.template", SmtpConnectorConfig.SubjectTemplateConfig);
    }

    [Fact]
    public void SendAsHtmlConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.send.as.html", SmtpConnectorConfig.SendAsHtmlConfig);
    }

    [Fact]
    public void BatchSizeConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.batch.size", SmtpConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void RetryCountConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.retry.count", SmtpConnectorConfig.RetryCountConfig);
    }

    [Fact]
    public void RetryDelayMsConfig_HasExpectedValue()
    {
        Assert.Equal("smtp.retry.delay.ms", SmtpConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void DefaultPort_HasExpectedValue()
    {
        Assert.Equal(587, SmtpConnectorConfig.DefaultPort);
    }

    [Fact]
    public void DefaultPortSsl_HasExpectedValue()
    {
        Assert.Equal(465, SmtpConnectorConfig.DefaultPortSsl);
    }

    [Fact]
    public void DefaultTimeoutSeconds_HasExpectedValue()
    {
        Assert.Equal(30, SmtpConnectorConfig.DefaultTimeoutSeconds);
    }

    [Fact]
    public void DefaultUseSsl_HasExpectedValue()
    {
        Assert.False(SmtpConnectorConfig.DefaultUseSsl);
    }

    [Fact]
    public void DefaultUseStartTls_HasExpectedValue()
    {
        Assert.True(SmtpConnectorConfig.DefaultUseStartTls);
    }

    [Fact]
    public void DefaultSendAsHtml_HasExpectedValue()
    {
        Assert.False(SmtpConnectorConfig.DefaultSendAsHtml);
    }

    [Fact]
    public void DefaultBatchSize_HasExpectedValue()
    {
        Assert.Equal(10, SmtpConnectorConfig.DefaultBatchSize);
    }

    [Fact]
    public void DefaultRetryCount_HasExpectedValue()
    {
        Assert.Equal(3, SmtpConnectorConfig.DefaultRetryCount);
    }

    [Fact]
    public void DefaultRetryDelayMs_HasExpectedValue()
    {
        Assert.Equal(1000, SmtpConnectorConfig.DefaultRetryDelayMs);
    }

    [Fact]
    public void DefaultSubject_HasExpectedValue()
    {
        Assert.Equal("Message from Surgewave", SmtpConnectorConfig.DefaultSubject);
    }
}
