using System.Text;
using System.Text.Json;
using Google.Api.Gax.Grpc;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Google.Protobuf;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Bigtable;

/// <summary>
/// Task that writes rows to Google Cloud Bigtable.
/// </summary>
public sealed class BigtableSinkTask : SinkTask
{
    private BigtableClient? _client;
    private TableName? _tableName;
    private string _rowKeyField = null!;
    private string _defaultColumnFamily = null!;
    private string _writeMode = null!;
    private int _batchSize;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var projectId = config[BigtableConnectorConfig.ProjectId];
        var instanceId = config[BigtableConnectorConfig.InstanceId];
        var tableId = config[BigtableConnectorConfig.TableId];

        _rowKeyField = config.GetValueOrDefault(BigtableConnectorConfig.RowKeyField, "rowKey")!;
        _defaultColumnFamily = config.GetValueOrDefault(BigtableConnectorConfig.DefaultColumnFamily,
            BigtableConnectorConfig.DefaultColumnFamilyName)!;
        _writeMode = config.GetValueOrDefault(BigtableConnectorConfig.WriteMode,
            BigtableConnectorConfig.DefaultWriteMode)!.ToLowerInvariant();
        _batchSize = int.Parse(config.GetValueOrDefault(BigtableConnectorConfig.BatchSize,
            BigtableConnectorConfig.DefaultBatchSize.ToString())!);

        _tableName = new TableName(projectId, instanceId, tableId);

        // Build client
        var clientBuilder = new BigtableClientBuilder();

        var emulatorHost = config.GetValueOrDefault(BigtableConnectorConfig.EmulatorHost, null);
        if (!string.IsNullOrWhiteSpace(emulatorHost))
        {
            clientBuilder.Endpoint = emulatorHost;
            // Emulator doesn't require authentication
            clientBuilder.Settings = new BigtableServiceApiSettings();
        }
        else
        {
            var credentialsJson = config.GetValueOrDefault(BigtableConnectorConfig.CredentialsJson, null);
            var credentialsFile = config.GetValueOrDefault(BigtableConnectorConfig.CredentialsFile, null);

#pragma warning disable CS0618 // GoogleCredential.FromJson/FromFile - CredentialFactory alternative requires internal IGoogleCredential
            if (!string.IsNullOrWhiteSpace(credentialsJson))
            {
                clientBuilder.GoogleCredential = GoogleCredential.FromJson(credentialsJson);
            }
            else if (!string.IsNullOrWhiteSpace(credentialsFile))
            {
                clientBuilder.GoogleCredential = GoogleCredential.FromFile(credentialsFile);
            }
#pragma warning restore CS0618
        }

        _client = clientBuilder.Build();
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        var entries = new List<MutateRowsRequest.Types.Entry>();

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                var entry = CreateMutationEntry(record);
                if (entry != null)
                {
                    entries.Add(entry);
                }

                // Flush batch if full
                if (entries.Count >= _batchSize)
                {
                    await FlushEntriesAsync(entries, cancellationToken);
                    entries.Clear();
                }
            }
            catch (Exception)
            {
                // Log and continue
            }
        }

        // Flush remaining entries
        if (entries.Count > 0)
        {
            await FlushEntriesAsync(entries, cancellationToken);
        }
    }

    private MutateRowsRequest.Types.Entry? CreateMutationEntry(SinkRecord record)
    {
        using var doc = JsonDocument.Parse(record.Value!);
        var root = doc.RootElement;

        // Get row key
        string rowKey;
        if (root.TryGetProperty(_rowKeyField, out var rowKeyProp))
        {
            rowKey = rowKeyProp.GetString() ?? "";
        }
        else if (record.Key != null)
        {
            rowKey = Encoding.UTF8.GetString(record.Key);
        }
        else
        {
            return null;
        }

        if (string.IsNullOrEmpty(rowKey)) return null;

        var mutations = new List<Mutation>();
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000; // Microseconds

        // Check for families structure in JSON
        if (root.TryGetProperty("families", out var familiesObj))
        {
            foreach (var family in familiesObj.EnumerateObject())
            {
                var familyName = family.Name;
                foreach (var column in family.Value.EnumerateObject())
                {
                    AddMutation(mutations, familyName, column.Name, column.Value, timestamp);
                }
            }
        }
        else
        {
            // Flat structure - all columns go to default family
            foreach (var prop in root.EnumerateObject())
            {
                if (prop.Name == _rowKeyField) continue;
                AddMutation(mutations, _defaultColumnFamily, prop.Name, prop.Value, timestamp);
            }
        }

        if (mutations.Count == 0) return null;

        return new MutateRowsRequest.Types.Entry
        {
            RowKey = ByteString.CopyFromUtf8(rowKey),
            Mutations = { mutations }
        };
    }

    private void AddMutation(List<Mutation> mutations, string family, string column, JsonElement value, long timestamp)
    {
        byte[] cellValue;

        if (value.ValueKind == JsonValueKind.Object && value.TryGetProperty("value", out var valueProp))
        {
            // Object with value field - may be base64 encoded
            var valueStr = valueProp.GetString() ?? "";
            try
            {
                cellValue = Convert.FromBase64String(valueStr);
            }
            catch
            {
                cellValue = Encoding.UTF8.GetBytes(valueStr);
            }
        }
        else if (value.ValueKind == JsonValueKind.String)
        {
            cellValue = Encoding.UTF8.GetBytes(value.GetString() ?? "");
        }
        else if (value.ValueKind == JsonValueKind.Number)
        {
            if (_writeMode == "increment" && value.TryGetInt64(out var longVal))
            {
                // For increment, store as big-endian 8 bytes
                cellValue = BitConverter.GetBytes(System.Net.IPAddress.HostToNetworkOrder(longVal));
            }
            else
            {
                cellValue = Encoding.UTF8.GetBytes(value.ToString());
            }
        }
        else
        {
            cellValue = Encoding.UTF8.GetBytes(value.ToString());
        }

        // Note: Append and Increment operations require ReadModifyWriteRow API
        // For simplicity, we use SetCell for all cases
        var mutation = Mutations.SetCell(family, ByteString.CopyFromUtf8(column), ByteString.CopyFrom(cellValue), new BigtableVersion(timestamp));

        mutations.Add(mutation);
    }

    private async Task FlushEntriesAsync(List<MutateRowsRequest.Types.Entry> entries, CancellationToken ct)
    {
        if (entries.Count == 0) return;

        try
        {
            await _client!.MutateRowsAsync(_tableName, entries, CallSettings.FromCancellationToken(ct));
        }
        catch (Exception)
        {
            // Log and continue
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public override void Stop()
    {
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
    }
}
