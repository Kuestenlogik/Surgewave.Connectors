using Kuestenlogik.Surgewave.Connector.Sftp;

namespace Kuestenlogik.Surgewave.Connector.Sftp.Tests;

public class SftpConnectorConfigTests
{
    [Fact]
    public void TopicConfig_HasExpectedValue()
    {
        Assert.Equal("topic", SftpConnectorConfig.TopicConfig);
    }

    [Fact]
    public void TopicsConfig_HasExpectedValue()
    {
        Assert.Equal("topics", SftpConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void HostConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.host", SftpConnectorConfig.HostConfig);
    }

    [Fact]
    public void PortConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.port", SftpConnectorConfig.PortConfig);
    }

    [Fact]
    public void UsernameConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.username", SftpConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void PasswordConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.password", SftpConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void PrivateKeyPathConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.private.key.path", SftpConnectorConfig.PrivateKeyPathConfig);
    }

    [Fact]
    public void PrivateKeyPassphraseConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.private.key.passphrase", SftpConnectorConfig.PrivateKeyPassphraseConfig);
    }

    [Fact]
    public void PrivateKeyContentConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.private.key.content", SftpConnectorConfig.PrivateKeyContentConfig);
    }

    [Fact]
    public void HostKeyFingerprintConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.host.key.fingerprint", SftpConnectorConfig.HostKeyFingerprintConfig);
    }

    [Fact]
    public void TimeoutSecondsConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.timeout.seconds", SftpConnectorConfig.TimeoutSecondsConfig);
    }

    [Fact]
    public void RemotePathConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.remote.path", SftpConnectorConfig.RemotePathConfig);
    }

    [Fact]
    public void FilePatternConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.file.pattern", SftpConnectorConfig.FilePatternConfig);
    }

    [Fact]
    public void RecursiveConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.recursive", SftpConnectorConfig.RecursiveConfig);
    }

    [Fact]
    public void PollIntervalMsConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.poll.interval.ms", SftpConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void DeleteAfterReadConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.delete.after.read", SftpConnectorConfig.DeleteAfterReadConfig);
    }

    [Fact]
    public void MoveAfterReadConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.move.after.read", SftpConnectorConfig.MoveAfterReadConfig);
    }

    [Fact]
    public void MoveToPathConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.move.to.path", SftpConnectorConfig.MoveToPathConfig);
    }

    [Fact]
    public void IncludeMetadataConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.include.metadata", SftpConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void MaxFileSizeBytesConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.max.file.size.bytes", SftpConnectorConfig.MaxFileSizeBytesConfig);
    }

    [Fact]
    public void MinFileSizeBytesConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.min.file.size.bytes", SftpConnectorConfig.MinFileSizeBytesConfig);
    }

    [Fact]
    public void StartFromConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.start.from", SftpConnectorConfig.StartFromConfig);
    }

    [Fact]
    public void OutputPathConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.output.path", SftpConnectorConfig.OutputPathConfig);
    }

    [Fact]
    public void OutputModeConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.output.mode", SftpConnectorConfig.OutputModeConfig);
    }

    [Fact]
    public void FileNameFieldConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.file.name.field", SftpConnectorConfig.FileNameFieldConfig);
    }

    [Fact]
    public void ContentFieldConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.content.field", SftpConnectorConfig.ContentFieldConfig);
    }

    [Fact]
    public void CreateDirectoriesConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.create.directories", SftpConnectorConfig.CreateDirectoriesConfig);
    }

    [Fact]
    public void OverwriteConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.overwrite", SftpConnectorConfig.OverwriteConfig);
    }

    [Fact]
    public void TempSuffixConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.temp.suffix", SftpConnectorConfig.TempSuffixConfig);
    }

    [Fact]
    public void FlushIntervalMsConfig_HasExpectedValue()
    {
        Assert.Equal("sftp.flush.interval.ms", SftpConnectorConfig.FlushIntervalMsConfig);
    }

    [Fact]
    public void OutputModeFile_HasExpectedValue()
    {
        Assert.Equal("file", SftpConnectorConfig.OutputModeFile);
    }

    [Fact]
    public void OutputModeAppend_HasExpectedValue()
    {
        Assert.Equal("append", SftpConnectorConfig.OutputModeAppend);
    }

    [Fact]
    public void StartFromLatest_HasExpectedValue()
    {
        Assert.Equal("latest", SftpConnectorConfig.StartFromLatest);
    }

    [Fact]
    public void StartFromEarliest_HasExpectedValue()
    {
        Assert.Equal("earliest", SftpConnectorConfig.StartFromEarliest);
    }

    [Fact]
    public void DefaultPort_HasExpectedValue()
    {
        Assert.Equal(22, SftpConnectorConfig.DefaultPort);
    }

    [Fact]
    public void DefaultTimeoutSeconds_HasExpectedValue()
    {
        Assert.Equal(30, SftpConnectorConfig.DefaultTimeoutSeconds);
    }

    [Fact]
    public void DefaultPollIntervalMs_HasExpectedValue()
    {
        Assert.Equal(30000, SftpConnectorConfig.DefaultPollIntervalMs);
    }

    [Fact]
    public void DefaultFlushIntervalMs_HasExpectedValue()
    {
        Assert.Equal(10000, SftpConnectorConfig.DefaultFlushIntervalMs);
    }

    [Fact]
    public void DefaultFilePattern_HasExpectedValue()
    {
        Assert.Equal("*", SftpConnectorConfig.DefaultFilePattern);
    }

    [Fact]
    public void DefaultOutputMode_HasExpectedValue()
    {
        Assert.Equal("file", SftpConnectorConfig.DefaultOutputMode);
    }

    [Fact]
    public void DefaultStartFrom_HasExpectedValue()
    {
        Assert.Equal("latest", SftpConnectorConfig.DefaultStartFrom);
    }

    [Fact]
    public void DefaultTempSuffix_HasExpectedValue()
    {
        Assert.Equal(".tmp", SftpConnectorConfig.DefaultTempSuffix);
    }

    [Fact]
    public void DefaultMaxFileSizeBytes_HasExpectedValue()
    {
        Assert.Equal(104857600L, SftpConnectorConfig.DefaultMaxFileSizeBytes);
    }

    [Fact]
    public void OffsetLastModified_HasExpectedValue()
    {
        Assert.Equal("last_modified", SftpConnectorConfig.OffsetLastModified);
    }

    [Fact]
    public void OffsetLastFileName_HasExpectedValue()
    {
        Assert.Equal("last_file_name", SftpConnectorConfig.OffsetLastFileName);
    }

    [Fact]
    public void OffsetLastPoll_HasExpectedValue()
    {
        Assert.Equal("last_poll", SftpConnectorConfig.OffsetLastPoll);
    }
}
