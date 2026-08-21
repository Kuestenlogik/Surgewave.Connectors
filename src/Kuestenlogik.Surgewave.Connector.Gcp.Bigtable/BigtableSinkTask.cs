using System.Globalization;
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
        if (_writeMode is "increment" or "append")
        {
            // Increment and append are read-modify-write operations and cannot be
            // expressed as MutateRows entries.
            foreach (var record in records)
            {
                if (record.Value == null) continue;

                await ApplyReadModifyWriteAsync(record, cancellationToken);
            }

            return;
        }

        var entries = new List<MutateRowsRequest.Types.Entry>();

        foreach (var record in records)
        {
            if (record.Value == null) continue;

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

    private static void AddMutation(List<Mutation> mutations, string family, string column, JsonElement value, long timestamp)
    {
        var mutation = Mutations.SetCell(family, ByteString.CopyFromUtf8(column), ByteString.CopyFrom(DecodeCellValue(value)), new BigtableVersion(timestamp));

        mutations.Add(mutation);
    }

    private static byte[] DecodeCellValue(JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Object && value.TryGetProperty("value", out var valueProp))
        {
            // Object with value field - may be base64 encoded
            var valueStr = valueProp.GetString() ?? "";
            try
            {
                return Convert.FromBase64String(valueStr);
            }
            catch (FormatException)
            {
                return Encoding.UTF8.GetBytes(valueStr);
            }
        }

        if (value.ValueKind == JsonValueKind.String)
        {
            return Encoding.UTF8.GetBytes(value.GetString() ?? "");
        }

        return Encoding.UTF8.GetBytes(value.ToString());
    }

    private async Task ApplyReadModifyWriteAsync(SinkRecord record, CancellationToken ct)
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
            return;
        }

        if (string.IsNullOrEmpty(rowKey)) return;

        var rules = new List<ReadModifyWriteRule>();

        if (root.TryGetProperty("families", out var familiesObj))
        {
            foreach (var family in familiesObj.EnumerateObject())
            {
                foreach (var column in family.Value.EnumerateObject())
                {
                    rules.Add(CreateReadModifyWriteRule(family.Name, column.Name, column.Value));
                }
            }
        }
        else
        {
            // Flat structure - all columns go to default family
            foreach (var prop in root.EnumerateObject())
            {
                if (prop.Name == _rowKeyField) continue;
                rules.Add(CreateReadModifyWriteRule(_defaultColumnFamily, prop.Name, prop.Value));
            }
        }

        if (rules.Count == 0) return;

        try
        {
            await _client!.ReadModifyWriteRowAsync(_tableName, rowKey, rules, CallSettings.FromCancellationToken(ct));
        }
        catch (Exception ex)
        {
            Context.RaiseError?.Invoke(ex);
            throw;
        }
    }

    private ReadModifyWriteRule CreateReadModifyWriteRule(string family, string column, JsonElement value)
    {
        if (_writeMode == "increment")
        {
            if (value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var longVal))
            {
                return ReadModifyWriteRules.Increment(family, column, longVal);
            }

            if (value.ValueKind == JsonValueKind.String
                && long.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
            {
                return ReadModifyWriteRules.Increment(family, column, parsed);
            }

            throw new InvalidOperationException(
                $"Write mode 'increment' requires an integer value for column '{family}:{column}'.");
        }

        return ReadModifyWriteRules.Append(family, column, DecodeCellValue(value));
    }

    private async Task FlushEntriesAsync(List<MutateRowsRequest.Types.Entry> entries, CancellationToken ct)
    {
        if (entries.Count == 0) return;

        try
        {
            var response = await _client!.MutateRowsAsync(_tableName, entries, CallSettings.FromCancellationToken(ct));

            // MutateRows is not atomic - each entry carries its own status
            var failedCount = 0;
            string? firstError = null;
            foreach (var entry in response.Entries)
            {
                var status = entry.Status;
                if (status == null || status.Code == (int)Google.Rpc.Code.Ok) continue;
                failedCount++;
                firstError ??= $"{status.Message} (code {status.Code})";
            }

            if (failedCount > 0)
            {
                throw new InvalidOperationException(
                    $"Bigtable rejected {failedCount} of {entries.Count} mutations; first error: {firstError}");
            }
        }
        catch (Exception ex)
        {
            Context.RaiseError?.Invoke(ex);
            throw;
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
