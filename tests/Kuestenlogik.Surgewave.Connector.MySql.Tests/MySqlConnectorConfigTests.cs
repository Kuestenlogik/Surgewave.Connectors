using Xunit;
using Kuestenlogik.Surgewave.Connector.MySql;

namespace Kuestenlogik.Surgewave.Connector.MySql.Tests;

public class MySqlConnectorConfigTests
{
    [Fact]
    public void ConnectionSettings_HaveExpectedValues()
    {
        Assert.Equal("mysql.host", MySqlConnectorConfig.Host);
        Assert.Equal("mysql.port", MySqlConnectorConfig.Port);
        Assert.Equal("mysql.database", MySqlConnectorConfig.Database);
        Assert.Equal("mysql.username", MySqlConnectorConfig.Username);
        Assert.Equal("mysql.password", MySqlConnectorConfig.Password);
    }

    [Fact]
    public void SslSettings_HaveExpectedValues()
    {
        Assert.Equal("mysql.ssl.mode", MySqlConnectorConfig.SslMode);
        Assert.Equal("mysql.ssl.ca", MySqlConnectorConfig.SslCa);
        Assert.Equal("mysql.ssl.cert", MySqlConnectorConfig.SslCert);
        Assert.Equal("mysql.ssl.key", MySqlConnectorConfig.SslKey);
    }

    [Fact]
    public void CdcSettings_HaveExpectedValues()
    {
        Assert.Equal("mysql.server.id", MySqlConnectorConfig.ServerId);
        Assert.Equal("mysql.tables", MySqlConnectorConfig.Tables);
        Assert.Equal("mysql.topic.prefix", MySqlConnectorConfig.TopicPrefix);
        Assert.Equal("mysql.topic.pattern", MySqlConnectorConfig.TopicPattern);
        Assert.Equal("mysql.include.schema", MySqlConnectorConfig.IncludeSchema);
        Assert.Equal("mysql.include.before.values", MySqlConnectorConfig.IncludeBeforeValues);
    }

    [Fact]
    public void SnapshotSettings_HaveExpectedValues()
    {
        Assert.Equal("mysql.snapshot.mode", MySqlConnectorConfig.SnapshotMode);
        Assert.Equal("mysql.snapshot.locking.mode", MySqlConnectorConfig.SnapshotLockingMode);
    }

    [Fact]
    public void BinlogSettings_HaveExpectedValues()
    {
        Assert.Equal("mysql.binlog.filename", MySqlConnectorConfig.BinlogFilename);
        Assert.Equal("mysql.binlog.position", MySqlConnectorConfig.BinlogPosition);
        Assert.Equal("mysql.gtid.set", MySqlConnectorConfig.GtidSet);
    }

    [Fact]
    public void PollingSettings_HaveExpectedValues()
    {
        Assert.Equal("mysql.poll.interval.ms", MySqlConnectorConfig.PollIntervalMs);
        Assert.Equal("mysql.batch.max.records", MySqlConnectorConfig.BatchMaxRecords);
    }

    [Fact]
    public void OffsetTrackingKeys_HaveExpectedValues()
    {
        Assert.Equal("binlog.filename", MySqlConnectorConfig.OffsetBinlogFilename);
        Assert.Equal("binlog.position", MySqlConnectorConfig.OffsetBinlogPosition);
        Assert.Equal("gtid", MySqlConnectorConfig.OffsetGtid);
        Assert.Equal("snapshot.completed", MySqlConnectorConfig.OffsetSnapshotCompleted);
        Assert.Equal("timestamp", MySqlConnectorConfig.OffsetTimestamp);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        Assert.Equal("localhost", MySqlConnectorConfig.DefaultHost);
        Assert.Equal(3306, MySqlConnectorConfig.DefaultPort);
        Assert.Equal(65535u, MySqlConnectorConfig.DefaultServerId);
        Assert.Equal("${database}.${table}", MySqlConnectorConfig.DefaultTopicPattern);
        Assert.Equal(100L, MySqlConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal(1000, MySqlConnectorConfig.DefaultBatchMaxRecords);
    }

    [Fact]
    public void SnapshotModes_HaveExpectedValues()
    {
        Assert.Equal("initial", MySqlConnectorConfig.SnapshotModeInitial);
        Assert.Equal("never", MySqlConnectorConfig.SnapshotModeNever);
        Assert.Equal("always", MySqlConnectorConfig.SnapshotModeAlways);
        Assert.Equal("schema_only", MySqlConnectorConfig.SnapshotModeSchemaOnly);
    }

    [Fact]
    public void SslModes_HaveExpectedValues()
    {
        Assert.Equal("none", MySqlConnectorConfig.SslModeNone);
        Assert.Equal("preferred", MySqlConnectorConfig.SslModePreferred);
        Assert.Equal("required", MySqlConnectorConfig.SslModeRequired);
        Assert.Equal("verify_ca", MySqlConnectorConfig.SslModeVerifyCa);
        Assert.Equal("verify_full", MySqlConnectorConfig.SslModeVerifyFull);
    }
}
