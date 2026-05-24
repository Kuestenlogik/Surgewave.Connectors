using Kuestenlogik.Surgewave.Connector.Snowflake;

namespace Kuestenlogik.Surgewave.Connector.Snowflake.Tests;

public class SnowflakeConnectorConfigTests
{
    [Fact]
    public void ConnectionSettings_HaveExpectedValues()
    {
        Assert.Equal("snowflake.account", SnowflakeConnectorConfig.AccountConfig);
        Assert.Equal("snowflake.user", SnowflakeConnectorConfig.UserConfig);
        Assert.Equal("snowflake.password", SnowflakeConnectorConfig.PasswordConfig);
        Assert.Equal("snowflake.database", SnowflakeConnectorConfig.DatabaseConfig);
        Assert.Equal("snowflake.schema", SnowflakeConnectorConfig.SchemaConfig);
        Assert.Equal("snowflake.warehouse", SnowflakeConnectorConfig.WarehouseConfig);
        Assert.Equal("snowflake.role", SnowflakeConnectorConfig.RoleConfig);
    }

    [Fact]
    public void AuthenticationSettings_HaveExpectedValues()
    {
        Assert.Equal("snowflake.authenticator", SnowflakeConnectorConfig.AuthenticatorConfig);
        Assert.Equal("snowflake.private.key.file", SnowflakeConnectorConfig.PrivateKeyFileConfig);
        Assert.Equal("snowflake.private.key.passphrase", SnowflakeConnectorConfig.PrivateKeyPassphraseConfig);
        Assert.Equal("snowflake.oauth.token", SnowflakeConnectorConfig.OAuthTokenConfig);
    }

    [Fact]
    public void SourceSettings_HaveExpectedValues()
    {
        Assert.Equal("snowflake.table", SnowflakeConnectorConfig.TableConfig);
        Assert.Equal("snowflake.query", SnowflakeConnectorConfig.QueryConfig);
        Assert.Equal("snowflake.stream.name", SnowflakeConnectorConfig.StreamNameConfig);
        Assert.Equal("snowflake.topic.pattern", SnowflakeConnectorConfig.TopicPatternConfig);
        Assert.Equal("snowflake.poll.interval.ms", SnowflakeConnectorConfig.PollIntervalMsConfig);
        Assert.Equal("snowflake.max.rows.per.poll", SnowflakeConnectorConfig.MaxRowsPerPollConfig);
        Assert.Equal("snowflake.include.metadata", SnowflakeConnectorConfig.IncludeMetadataConfig);
        Assert.Equal("snowflake.mode", SnowflakeConnectorConfig.ModeConfig);
        Assert.Equal("snowflake.timestamp.column", SnowflakeConnectorConfig.TimestampColumnConfig);
        Assert.Equal("snowflake.incrementing.column", SnowflakeConnectorConfig.IncrementingColumnConfig);
    }

    [Fact]
    public void SinkSettings_HaveExpectedValues()
    {
        Assert.Equal("topics", SnowflakeConnectorConfig.TopicsConfig);
        Assert.Equal("snowflake.write.mode", SnowflakeConnectorConfig.WriteModeConfig);
        Assert.Equal("snowflake.batch.size", SnowflakeConnectorConfig.BatchSizeConfig);
        Assert.Equal("snowflake.stage.name", SnowflakeConnectorConfig.StageNameConfig);
        Assert.Equal("snowflake.use.snowpipe", SnowflakeConnectorConfig.UseSnowpipeConfig);
        Assert.Equal("snowflake.pipe.name", SnowflakeConnectorConfig.PipeNameConfig);
        Assert.Equal("snowflake.max.retry.count", SnowflakeConnectorConfig.MaxRetryCountConfig);
        Assert.Equal("snowflake.retry.delay.ms", SnowflakeConnectorConfig.RetryDelayMsConfig);
        Assert.Equal("snowflake.key.columns", SnowflakeConnectorConfig.KeyColumnsConfig);
        Assert.Equal("snowflake.auto.create.table", SnowflakeConnectorConfig.AutoCreateTableConfig);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        Assert.Equal("snowflake.${database}.${schema}.${table}", SnowflakeConnectorConfig.DefaultTopicPattern);
        Assert.Equal(5000L, SnowflakeConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal(10000, SnowflakeConnectorConfig.DefaultMaxRowsPerPoll);
        Assert.Equal("insert", SnowflakeConnectorConfig.DefaultWriteMode);
        Assert.Equal(10000, SnowflakeConnectorConfig.DefaultBatchSize);
        Assert.Equal(3, SnowflakeConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(1000L, SnowflakeConnectorConfig.DefaultRetryDelayMs);
        Assert.Equal("table", SnowflakeConnectorConfig.DefaultMode);
    }

    [Fact]
    public void HeaderNames_HaveExpectedValues()
    {
        Assert.Equal("snowflake.account", SnowflakeConnectorConfig.HeaderAccount);
        Assert.Equal("snowflake.database", SnowflakeConnectorConfig.HeaderDatabase);
        Assert.Equal("snowflake.schema", SnowflakeConnectorConfig.HeaderSchema);
        Assert.Equal("snowflake.table", SnowflakeConnectorConfig.HeaderTable);
        Assert.Equal("snowflake.warehouse", SnowflakeConnectorConfig.HeaderWarehouse);
        Assert.Equal("snowflake.stream.name", SnowflakeConnectorConfig.HeaderStreamName);
        Assert.Equal("snowflake.action.type", SnowflakeConnectorConfig.HeaderActionType);
        Assert.Equal("snowflake.row.id", SnowflakeConnectorConfig.HeaderRowId);
        Assert.Equal("snowflake.timestamp", SnowflakeConnectorConfig.HeaderTimestamp);
    }

    [Fact]
    public void OffsetTrackingKeys_HaveExpectedValues()
    {
        Assert.Equal("timestamp", SnowflakeConnectorConfig.OffsetTimestamp);
        Assert.Equal("incrementing_value", SnowflakeConnectorConfig.OffsetIncrementingColumn);
        Assert.Equal("stream_position", SnowflakeConnectorConfig.OffsetStreamPosition);
        Assert.Equal("table", SnowflakeConnectorConfig.OffsetTable);
    }

    [Fact]
    public void StreamMetadataColumns_HaveExpectedValues()
    {
        Assert.Equal("METADATA$ACTION", SnowflakeConnectorConfig.MetadataAction);
        Assert.Equal("METADATA$ISUPDATE", SnowflakeConnectorConfig.MetadataIsUpdate);
        Assert.Equal("METADATA$ROW_ID", SnowflakeConnectorConfig.MetadataRowId);
    }

    [Fact]
    public void ActionTypes_HaveExpectedValues()
    {
        Assert.Equal("INSERT", SnowflakeConnectorConfig.ActionInsert);
        Assert.Equal("DELETE", SnowflakeConnectorConfig.ActionDelete);
    }

    [Fact]
    public void DefaultWriteMode_IsInsert()
    {
        Assert.Equal("insert", SnowflakeConnectorConfig.DefaultWriteMode);
    }

    [Fact]
    public void DefaultMode_IsTable()
    {
        Assert.Equal("table", SnowflakeConnectorConfig.DefaultMode);
    }

    [Fact]
    public void DefaultPollInterval_IsReasonable()
    {
        Assert.Equal(5000L, SnowflakeConnectorConfig.DefaultPollIntervalMs);
        Assert.True(SnowflakeConnectorConfig.DefaultPollIntervalMs >= 1000);
    }

    [Fact]
    public void DefaultBatchSize_IsReasonable()
    {
        Assert.Equal(10000, SnowflakeConnectorConfig.DefaultBatchSize);
        Assert.True(SnowflakeConnectorConfig.DefaultBatchSize > 0);
        Assert.True(SnowflakeConnectorConfig.DefaultBatchSize <= 16000); // Snowflake typical limits
    }

    [Fact]
    public void DefaultMaxRowsPerPoll_IsReasonable()
    {
        Assert.Equal(10000, SnowflakeConnectorConfig.DefaultMaxRowsPerPoll);
        Assert.True(SnowflakeConnectorConfig.DefaultMaxRowsPerPoll > 0);
    }

    [Fact]
    public void DefaultRetrySettings_AreReasonable()
    {
        Assert.Equal(3, SnowflakeConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(1000L, SnowflakeConnectorConfig.DefaultRetryDelayMs);
        Assert.True(SnowflakeConnectorConfig.DefaultMaxRetryCount > 0);
        Assert.True(SnowflakeConnectorConfig.DefaultRetryDelayMs > 0);
    }
}
