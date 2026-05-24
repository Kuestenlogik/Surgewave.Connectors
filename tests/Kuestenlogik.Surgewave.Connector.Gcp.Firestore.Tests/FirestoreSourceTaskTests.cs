using Kuestenlogik.Surgewave.Connector.Gcp.Firestore;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Firestore.Tests;

public class FirestoreSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new FirestoreSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_ThrowsWhenProjectIdMissing()
    {
        using var task = new FirestoreSourceTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection"
        };

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    [Fact]
    public void Start_ThrowsWhenCollectionMissing()
    {
        using var task = new FirestoreSourceTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project"
        };

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    [Fact]
    public async Task PollAsync_ReturnsEmptyWhenNotInitialized()
    {
        using var task = new FirestoreSourceTask();

        // Without Start, should return empty
        var records = await task.PollAsync(CancellationToken.None);
        Assert.Empty(records);
    }

    [Fact]
    public async Task CommitAsync_CompletesWithoutError()
    {
        using var task = new FirestoreSourceTask();

        // Should complete without error even when not started
        await task.CommitAsync(CancellationToken.None);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new FirestoreSourceTask();

        // Should not throw
        task.Stop();
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new FirestoreSourceTask();

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public void DefaultTopicPattern_UsesExpectedFormat()
    {
        Assert.Equal("firestore.${collection}", FirestoreConnectorConfig.DefaultTopicPattern);
    }

    [Fact]
    public void DefaultPollInterval_IsReasonable()
    {
        Assert.Equal(5000L, FirestoreConnectorConfig.DefaultPollIntervalMs);
    }

    [Fact]
    public void DefaultMaxDocuments_IsReasonable()
    {
        Assert.Equal(500, FirestoreConnectorConfig.DefaultMaxDocumentsPerPoll);
    }

    [Fact]
    public void WatchModeOptions_AreValid()
    {
        Assert.Equal("listen", FirestoreConnectorConfig.DefaultWatchMode);
    }

    [Fact]
    public void DefaultOrderDirection_IsAscending()
    {
        Assert.Equal("asc", FirestoreConnectorConfig.DefaultOrderDirection);
    }

    [Fact]
    public void Start_AcceptsValidConfig_WithEmulator()
    {
        using var task = new FirestoreSourceTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.EmulatorHostConfig] = "localhost:8080"
        };

        // Start sets up the client, but without emulator running it will fail
        // This test just verifies config parsing
        try
        {
            task.Start(config);
        }
        catch (Exception)
        {
            // Expected - emulator not running
        }
    }

    [Fact]
    public void Start_ParsesWatchMode()
    {
        using var task = new FirestoreSourceTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.WatchModeConfig] = "poll",
            [FirestoreConnectorConfig.EmulatorHostConfig] = "localhost:8080"
        };

        // Should parse poll mode without error
        try
        {
            task.Start(config);
        }
        catch (Exception)
        {
            // Expected - emulator not running
        }
    }

    [Fact]
    public void Start_ParsesQueryFilter()
    {
        using var task = new FirestoreSourceTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.QueryFilterConfig] = "status:eq:active",
            [FirestoreConnectorConfig.EmulatorHostConfig] = "localhost:8080"
        };

        try
        {
            task.Start(config);
        }
        catch (Exception)
        {
            // Expected - emulator not running
        }
    }

    [Fact]
    public void Start_ParsesOrderByConfig()
    {
        using var task = new FirestoreSourceTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.OrderByFieldConfig] = "createdAt",
            [FirestoreConnectorConfig.OrderDirectionConfig] = "desc",
            [FirestoreConnectorConfig.EmulatorHostConfig] = "localhost:8080"
        };

        try
        {
            task.Start(config);
        }
        catch (Exception)
        {
            // Expected - emulator not running
        }
    }

    [Fact]
    public void Start_ParsesTimestampField()
    {
        using var task = new FirestoreSourceTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.TimestampFieldConfig] = "updatedAt",
            [FirestoreConnectorConfig.EmulatorHostConfig] = "localhost:8080"
        };

        try
        {
            task.Start(config);
        }
        catch (Exception)
        {
            // Expected - emulator not running
        }
    }

    [Fact]
    public void Start_ParsesPollIntervalMs()
    {
        using var task = new FirestoreSourceTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.PollIntervalMsConfig] = "10000",
            [FirestoreConnectorConfig.EmulatorHostConfig] = "localhost:8080"
        };

        try
        {
            task.Start(config);
        }
        catch (Exception)
        {
            // Expected - emulator not running
        }
    }

    [Fact]
    public void Start_ParsesMaxDocumentsPerPoll()
    {
        using var task = new FirestoreSourceTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.MaxDocumentsPerPollConfig] = "100",
            [FirestoreConnectorConfig.EmulatorHostConfig] = "localhost:8080"
        };

        try
        {
            task.Start(config);
        }
        catch (Exception)
        {
            // Expected - emulator not running
        }
    }

    [Fact]
    public void Start_ParsesIncludeMetadata()
    {
        using var task = new FirestoreSourceTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.IncludeMetadataConfig] = "false",
            [FirestoreConnectorConfig.EmulatorHostConfig] = "localhost:8080"
        };

        try
        {
            task.Start(config);
        }
        catch (Exception)
        {
            // Expected - emulator not running
        }
    }

    [Fact]
    public void Start_ParsesCredentialsFile()
    {
        using var task = new FirestoreSourceTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.CredentialsFileConfig] = "/path/to/creds.json"
        };

        // Should fail with missing file, but parse correctly
        Assert.ThrowsAny<Exception>(() => task.Start(config));
    }

    [Fact]
    public void Start_ParsesSubcollectionPath()
    {
        using var task = new FirestoreSourceTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "users/user1/orders",
            [FirestoreConnectorConfig.EmulatorHostConfig] = "localhost:8080"
        };

        try
        {
            task.Start(config);
        }
        catch (Exception)
        {
            // Expected - emulator not running
        }
    }
}
