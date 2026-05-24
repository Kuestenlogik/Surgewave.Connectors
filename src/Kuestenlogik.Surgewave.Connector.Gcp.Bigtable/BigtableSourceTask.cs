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
/// Task that reads rows from Google Cloud Bigtable.
/// </summary>
public sealed class BigtableSourceTask : SourceTask
{
    private BigtableClient? _client;
    private TableName? _tableName;
    private string _topic = null!;
    private int _pollIntervalMs;
    private string? _rowKeyPrefix;
    private string? _rowKeyStart;
    private string? _rowKeyEnd;
    private string? _columnFamily;
    private string[]? _columns;
    private int _rowLimit;
    private bool _includeTimestamp;
    private DateTime _lastPoll = DateTime.MinValue;
    private string? _lastRowKey;
    private long _messageId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var projectId = config[BigtableConnectorConfig.ProjectId];
        var instanceId = config[BigtableConnectorConfig.InstanceId];
        var tableId = config[BigtableConnectorConfig.TableId];
        _topic = config[BigtableConnectorConfig.Topic];

        _pollIntervalMs = int.Parse(config.GetValueOrDefault(BigtableConnectorConfig.PollIntervalMs,
            BigtableConnectorConfig.DefaultPollIntervalMs.ToString())!);
        _rowKeyPrefix = config.GetValueOrDefault(BigtableConnectorConfig.RowKeyPrefix, null);
        _rowKeyStart = config.GetValueOrDefault(BigtableConnectorConfig.RowKeyStart, null);
        _rowKeyEnd = config.GetValueOrDefault(BigtableConnectorConfig.RowKeyEnd, null);
        _columnFamily = config.GetValueOrDefault(BigtableConnectorConfig.ColumnFamily, null);
        _rowLimit = int.Parse(config.GetValueOrDefault(BigtableConnectorConfig.RowLimit,
            BigtableConnectorConfig.DefaultRowLimit.ToString())!);
        _includeTimestamp = config.GetValueOrDefault(BigtableConnectorConfig.IncludeTimestamp, "true") == "true";

        var columnsStr = config.GetValueOrDefault(BigtableConnectorConfig.Columns, "");
        if (!string.IsNullOrWhiteSpace(columnsStr))
        {
            _columns = columnsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

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

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if ((DateTime.UtcNow - _lastPoll).TotalMilliseconds < _pollIntervalMs)
        {
            return [];
        }

        _lastPoll = DateTime.UtcNow;
        var records = new List<SourceRecord>();

        try
        {
            // Build row set
            var rowSet = BuildRowSet();

            // Build filter
            var filter = BuildFilter();

            // Read rows
            var readRowsRequest = new ReadRowsRequest
            {
                TableNameAsTableName = _tableName,
                Rows = rowSet,
                Filter = filter,
                RowsLimit = _rowLimit
            };

            var rows = _client!.ReadRows(readRowsRequest);

            await foreach (var row in rows)
            {
                var record = CreateRecord(row);
                records.Add(record);
                _lastRowKey = row.Key.ToStringUtf8();
            }
        }
        catch (Exception)
        {
            // Log and continue
        }

        return records;
    }

    private RowSet BuildRowSet()
    {
        var rowSet = new RowSet();

        if (!string.IsNullOrEmpty(_rowKeyPrefix))
        {
            // Create a prefix range manually
            var prefixBytes = Encoding.UTF8.GetBytes(_rowKeyPrefix);
            var endBytes = new byte[prefixBytes.Length];
            Array.Copy(prefixBytes, endBytes, prefixBytes.Length);
            // Increment the last byte to create an exclusive end key
            endBytes[^1]++;

            rowSet.RowRanges.Add(new RowRange
            {
                StartKeyClosed = ByteString.CopyFrom(prefixBytes),
                EndKeyOpen = ByteString.CopyFrom(endBytes)
            });
        }
        else if (!string.IsNullOrEmpty(_rowKeyStart) || !string.IsNullOrEmpty(_rowKeyEnd))
        {
            var range = new RowRange();
            if (!string.IsNullOrEmpty(_rowKeyStart))
            {
                range.StartKeyClosed = ByteString.CopyFromUtf8(_rowKeyStart);
            }
            if (!string.IsNullOrEmpty(_rowKeyEnd))
            {
                range.EndKeyOpen = ByteString.CopyFromUtf8(_rowKeyEnd);
            }
            rowSet.RowRanges.Add(range);
        }
        else if (!string.IsNullOrEmpty(_lastRowKey))
        {
            // Continue from last row key
            rowSet.RowRanges.Add(new RowRange
            {
                StartKeyOpen = ByteString.CopyFromUtf8(_lastRowKey)
            });
        }

        return rowSet;
    }

    private RowFilter? BuildFilter()
    {
        var filters = new List<RowFilter>();

        if (!string.IsNullOrEmpty(_columnFamily))
        {
            filters.Add(RowFilters.FamilyNameExact(_columnFamily));
        }

        if (_columns != null && _columns.Length > 0)
        {
            var columnFilters = _columns.Select(c =>
                RowFilters.ColumnQualifierExact(ByteString.CopyFromUtf8(c))).ToArray();

            if (columnFilters.Length == 1)
            {
                filters.Add(columnFilters[0]);
            }
            else
            {
                filters.Add(RowFilters.Interleave(columnFilters));
            }
        }

        // Only get latest version
        filters.Add(RowFilters.CellsPerColumnLimit(1));

        return filters.Count switch
        {
            0 => null,
            1 => filters[0],
            _ => RowFilters.Chain(filters.ToArray())
        };
    }

    private SourceRecord CreateRecord(Row row)
    {
        var msgId = Interlocked.Increment(ref _messageId);
        var rowKey = row.Key.ToStringUtf8();

        // Build cell data
        var families = new Dictionary<string, Dictionary<string, object>>();

        foreach (var family in row.Families)
        {
            var columns = new Dictionary<string, object>();
            foreach (var column in family.Columns)
            {
                var qualifier = column.Qualifier.ToStringUtf8();
                var latestCell = column.Cells.FirstOrDefault();
                if (latestCell != null)
                {
                    if (_includeTimestamp)
                    {
                        columns[qualifier] = new
                        {
                            value = Convert.ToBase64String(latestCell.Value.ToByteArray()),
                            timestamp = latestCell.TimestampMicros
                        };
                    }
                    else
                    {
                        // Try to decode as UTF-8 string, fall back to base64
                        try
                        {
                            columns[qualifier] = latestCell.Value.ToStringUtf8();
                        }
                        catch
                        {
                            columns[qualifier] = Convert.ToBase64String(latestCell.Value.ToByteArray());
                        }
                    }
                }
            }
            families[family.Name] = columns;
        }

        var payload = new
        {
            rowKey,
            families,
            timestamp = DateTime.UtcNow
        };

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["source"] = "bigtable",
                ["table"] = _tableName!.TableId
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["row_key"] = rowKey
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(rowKey),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["bigtable.table"] = Encoding.UTF8.GetBytes(_tableName.TableId),
                ["bigtable.rowkey"] = Encoding.UTF8.GetBytes(rowKey)
            }
        };
    }

    public override void Stop()
    {
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
    }
}
