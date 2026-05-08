namespace Kuestenlogik.Surgewave.Connector.Google.Drive.Tests;

using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class GoogleDriveTaskTests
{
    [Fact]
    public void GoogleDriveSourceTask_HasCorrectVersion()
    {
        using var task = new GoogleDriveSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void GoogleDriveSinkTask_HasCorrectVersion()
    {
        using var task = new GoogleDriveSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void GoogleDriveSourceTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new GoogleDriveSourceTask();
        task.Initialize(CreateSourceTaskContext());

        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void GoogleDriveSinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new GoogleDriveSinkTask();
        task.Initialize(CreateSinkTaskContext());

        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void GoogleDriveSourceTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new GoogleDriveSourceTask();
        task.Initialize(CreateSourceTaskContext());

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public void GoogleDriveSinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new GoogleDriveSinkTask();
        task.Initialize(CreateSinkTaskContext());

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public async Task GoogleDriveSinkTask_PutAsync_HandlesEmptyRecords()
    {
        using var task = new GoogleDriveSinkTask();
        task.Initialize(CreateSinkTaskContext());

        // Should not throw on empty records
        await task.PutAsync(Array.Empty<SinkRecord>(), CancellationToken.None);
    }

    [Fact]
    public async Task GoogleDriveSourceTask_PollAsync_HandlesNoServiceGracefully()
    {
        using var task = new GoogleDriveSourceTask();
        task.Initialize(CreateSourceTaskContext());

        // Should return empty without throwing when service not started
        var records = await task.PollAsync(CancellationToken.None);
        Assert.Empty(records);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasCorrectTopicsConfig()
    {
        Assert.Equal("topics", GoogleDriveConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasCorrectAuthConfig()
    {
        Assert.Equal("google.credentials.json", GoogleDriveConnectorConfig.CredentialsJsonConfig);
        Assert.Equal("google.credentials.file", GoogleDriveConnectorConfig.CredentialsFileConfig);
        Assert.Equal("google.service.account.email", GoogleDriveConnectorConfig.ServiceAccountEmailConfig);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasCorrectModeConfig()
    {
        Assert.Equal("mode", GoogleDriveConnectorConfig.ModeConfig);
        Assert.Equal("source-watch", GoogleDriveConnectorConfig.ModeSourceWatch);
        Assert.Equal("source-list", GoogleDriveConnectorConfig.ModeSourceList);
        Assert.Equal("sink-upload", GoogleDriveConnectorConfig.ModeSinkUpload);
        Assert.Equal("sink-update", GoogleDriveConnectorConfig.ModeSinkUpdate);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasCorrectFolderConfig()
    {
        Assert.Equal("folder.id", GoogleDriveConnectorConfig.FolderIdConfig);
        Assert.Equal("root", GoogleDriveConnectorConfig.DefaultFolderId);
        Assert.Equal("recursive", GoogleDriveConnectorConfig.RecursiveConfig);
        Assert.False(GoogleDriveConnectorConfig.DefaultRecursive);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasCorrectFileFilterConfig()
    {
        Assert.Equal("file.pattern", GoogleDriveConnectorConfig.FilePatternConfig);
        Assert.Equal("*", GoogleDriveConnectorConfig.DefaultFilePattern);
        Assert.Equal("mime.type.filter", GoogleDriveConnectorConfig.MimeTypeFilterConfig);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasCorrectPollingConfig()
    {
        Assert.Equal("poll.interval.ms", GoogleDriveConnectorConfig.PollIntervalMsConfig);
        Assert.Equal(30000, GoogleDriveConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal("track.changes", GoogleDriveConnectorConfig.TrackChangesConfig);
        Assert.True(GoogleDriveConnectorConfig.DefaultTrackChanges);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasCorrectUploadConfig()
    {
        Assert.Equal("upload.folder.id", GoogleDriveConnectorConfig.UploadFolderIdConfig);
        Assert.Equal("filename.field", GoogleDriveConnectorConfig.FileNameFieldConfig);
        Assert.Equal("filename", GoogleDriveConnectorConfig.DefaultFileNameField);
        Assert.Equal("content.field", GoogleDriveConnectorConfig.ContentFieldConfig);
        Assert.Equal("content", GoogleDriveConnectorConfig.DefaultContentField);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasCorrectUpdateModeConfig()
    {
        Assert.Equal("update.mode", GoogleDriveConnectorConfig.UpdateModeConfig);
        Assert.Equal("create", GoogleDriveConnectorConfig.UpdateModeCreate);
        Assert.Equal("replace", GoogleDriveConnectorConfig.UpdateModeReplace);
        Assert.Equal("create-or-replace", GoogleDriveConnectorConfig.UpdateModeCreateOrReplace);
        Assert.Equal("create-or-replace", GoogleDriveConnectorConfig.DefaultUpdateMode);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasCorrectContentConfig()
    {
        Assert.Equal("include.content", GoogleDriveConnectorConfig.IncludeContentConfig);
        Assert.False(GoogleDriveConnectorConfig.DefaultIncludeContent);
        Assert.Equal("max.file.size.bytes", GoogleDriveConnectorConfig.MaxFileSizeBytesConfig);
        Assert.Equal(10 * 1024 * 1024, GoogleDriveConnectorConfig.DefaultMaxFileSizeBytes);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasCorrectOutputConfig()
    {
        Assert.Equal("output.format", GoogleDriveConnectorConfig.OutputFormatConfig);
        Assert.Equal("json", GoogleDriveConnectorConfig.FormatJson);
        Assert.Equal("bytes", GoogleDriveConnectorConfig.FormatBytes);
        Assert.Equal("json", GoogleDriveConnectorConfig.DefaultOutputFormat);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasCorrectBatchingDefaults()
    {
        Assert.Equal(10, GoogleDriveConnectorConfig.DefaultBatchSize);
        Assert.Equal(3, GoogleDriveConnectorConfig.DefaultRetryMax);
        Assert.Equal(1000, GoogleDriveConnectorConfig.DefaultRetryBackoffMs);
    }

    private static TaskContext CreateSourceTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }

    private static TaskContext CreateSinkTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
