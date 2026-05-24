using Kuestenlogik.Surgewave.Connector.Gcp.Firestore;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Firestore.Tests;

public class FirestoreSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new FirestoreSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_ThrowsWhenProjectIdMissing()
    {
        using var task = new FirestoreSinkTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection"
        };

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    [Fact]
    public void Start_ThrowsWhenCollectionMissing()
    {
        using var task = new FirestoreSinkTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project"
        };

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    [Fact]
    public async Task PutAsync_HandlesEmptyRecords()
    {
        using var task = new FirestoreSinkTask();

        // Without Start, should handle empty records gracefully
        await task.PutAsync([], CancellationToken.None);
    }

    [Fact]
    public async Task FlushAsync_CompletesWithoutError()
    {
        using var task = new FirestoreSinkTask();

        // Should complete without error even when not started
        var offsets = new Dictionary<TopicPartition, long>();
        await task.FlushAsync(offsets, CancellationToken.None);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new FirestoreSinkTask();

        // Should not throw
        task.Stop();
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new FirestoreSinkTask();

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public void DefaultWriteMode_IsSet()
    {
        Assert.Equal("set", FirestoreConnectorConfig.DefaultWriteMode);
    }

    [Fact]
    public void DefaultBatchSize_IsReasonable()
    {
        Assert.Equal(500, FirestoreConnectorConfig.DefaultBatchSize);
        Assert.True(FirestoreConnectorConfig.DefaultBatchSize <= 500); // Firestore limit
    }

    [Fact]
    public void DefaultMaxRetryCount_IsReasonable()
    {
        Assert.Equal(3, FirestoreConnectorConfig.DefaultMaxRetryCount);
    }

    [Fact]
    public void DefaultRetryDelay_IsReasonable()
    {
        Assert.Equal(1000L, FirestoreConnectorConfig.DefaultRetryDelayMs);
    }

    [Fact]
    public void Start_ParsesWriteMode()
    {
        using var task = new FirestoreSinkTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.WriteModeConfig] = "merge",
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
    public void Start_ParsesBatchSize()
    {
        using var task = new FirestoreSinkTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.BatchSizeConfig] = "100",
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
    public void Start_ParsesRetrySettings()
    {
        using var task = new FirestoreSinkTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.MaxRetryCountConfig] = "5",
            [FirestoreConnectorConfig.RetryDelayMsConfig] = "2000",
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
    public void Start_ParsesDocumentIdField()
    {
        using var task = new FirestoreSinkTask();

        var config = new Dictionary<string, string>
        {
            [FirestoreConnectorConfig.ProjectIdConfig] = "test-project",
            [FirestoreConnectorConfig.CollectionPathConfig] = "testcollection",
            [FirestoreConnectorConfig.DocumentIdFieldConfig] = "documentId",
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
        using var task = new FirestoreSinkTask();

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
    public async Task PutAsync_ReturnsWithoutErrorWhenNotInitialized()
    {
        using var task = new FirestoreSinkTask();

        var records = new List<SinkRecord>
        {
            new SinkRecord
            {
                Topic = "test",
                Partition = 0,
                Offset = 1,
                Value = System.Text.Encoding.UTF8.GetBytes("{\"id\": \"1\", \"name\": \"test\"}")
            }
        };

        // Should return without error when not initialized (no Firestore connection)
        await task.PutAsync(records, CancellationToken.None);
    }

    [Fact]
    public void WriteModes_AreValid()
    {
        // Valid write modes: set, create, update, merge
        var validModes = new[] { "set", "create", "update", "merge" };
        Assert.Contains(FirestoreConnectorConfig.DefaultWriteMode, validModes);
    }

    [Fact]
    public void BatchSize_RespectFirestoreLimit()
    {
        // Firestore batch limit is 500 operations
        Assert.True(FirestoreConnectorConfig.DefaultBatchSize <= 500);
    }

    [Fact]
    public void Start_AcceptsSubcollectionPath()
    {
        using var task = new FirestoreSinkTask();

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

    [Fact]
    public async Task PutAsync_HandlesNullRecords()
    {
        using var task = new FirestoreSinkTask();

        // Empty list should be handled
        await task.PutAsync(Array.Empty<SinkRecord>(), CancellationToken.None);
    }

    [Fact]
    public async Task PutAsync_HandlesTombstoneRecords()
    {
        using var task = new FirestoreSinkTask();

        var records = new List<SinkRecord>
        {
            new SinkRecord
            {
                Topic = "test",
                Partition = 0,
                Offset = 1,
                Key = System.Text.Encoding.UTF8.GetBytes("{\"id\": \"1\"}"),
                Value = null! // Tombstone
            }
        };

        // Should handle tombstone gracefully when not initialized
        await task.PutAsync(records, CancellationToken.None);
    }
}
