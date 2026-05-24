using System.Numerics;
using System.Text.Json;
using TigerBeetle;
using Kuestenlogik.Surgewave.Connect;

using TBClient = TigerBeetle.Client;

namespace Kuestenlogik.Surgewave.Connector.TigerBeetle;

/// <summary>
/// Task that creates accounts and transfers in TigerBeetle.
/// </summary>
public sealed class TigerBeetleSinkTask : SinkTask
{
    private TBClient? _client;
    private string _operationType = null!;
    private int _batchSize;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var addresses = config[TigerBeetleConnectorConfig.ClusterAddresses]
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var clusterId = UInt128.Parse(config.GetValueOrDefault(TigerBeetleConnectorConfig.ClusterId, "0")!);

        var maxConcurrency = uint.Parse(config.GetValueOrDefault(TigerBeetleConnectorConfig.MaxConcurrency,
            TigerBeetleConnectorConfig.DefaultMaxConcurrency.ToString())!);

        _operationType = config.GetValueOrDefault(TigerBeetleConnectorConfig.OperationType,
            TigerBeetleConnectorConfig.DefaultOperationType)!.ToLowerInvariant();

        _batchSize = int.Parse(config.GetValueOrDefault(TigerBeetleConnectorConfig.BatchSize,
            TigerBeetleConnectorConfig.DefaultBatchSize.ToString())!);

        _client = new TBClient(clusterId, addresses);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        var accountBatch = new List<Account>();
        var transferBatch = new List<Transfer>();

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                using var doc = JsonDocument.Parse(record.Value);
                var root = doc.RootElement;

                // Determine operation type from message or config
                var opType = _operationType;
                if (root.TryGetProperty("type", out var typeProp))
                {
                    var type = typeProp.GetString()?.ToLowerInvariant();
                    if (type == "account") opType = "create_account";
                    else if (type == "transfer") opType = "create_transfer";
                }

                switch (opType)
                {
                    case "create_account":
                        var account = ParseAccount(root);
                        if (account.HasValue)
                        {
                            accountBatch.Add(account.Value);
                            if (accountBatch.Count >= _batchSize)
                            {
                                await FlushAccountsAsync(accountBatch);
                                accountBatch.Clear();
                            }
                        }
                        break;

                    case "create_transfer":
                        var transfer = ParseTransfer(root);
                        if (transfer.HasValue)
                        {
                            transferBatch.Add(transfer.Value);
                            if (transferBatch.Count >= _batchSize)
                            {
                                await FlushTransfersAsync(transferBatch);
                                transferBatch.Clear();
                            }
                        }
                        break;
                }
            }
            catch (Exception)
            {
                // Log and continue
            }
        }

        // Flush remaining
        if (accountBatch.Count > 0)
        {
            await FlushAccountsAsync(accountBatch);
        }
        if (transferBatch.Count > 0)
        {
            await FlushTransfersAsync(transferBatch);
        }
    }

    private Account? ParseAccount(JsonElement root)
    {
        if (!root.TryGetProperty("id", out var idProp))
        {
            return null;
        }

        var account = new Account
        {
            Id = ParseUInt128(idProp),
            Ledger = root.TryGetProperty("ledger", out var ledger) ? ledger.GetUInt32() : 1u,
            Code = root.TryGetProperty("code", out var code) ? code.GetUInt16() : (ushort)1
        };

        if (root.TryGetProperty("user_data_128", out var ud128))
        {
            account.UserData128 = ParseUInt128(ud128);
        }
        if (root.TryGetProperty("user_data_64", out var ud64))
        {
            account.UserData64 = ud64.GetUInt64();
        }
        if (root.TryGetProperty("user_data_32", out var ud32))
        {
            account.UserData32 = ud32.GetUInt32();
        }
        if (root.TryGetProperty("flags", out var flags))
        {
            account.Flags = (AccountFlags)flags.GetInt32();
        }

        return account;
    }

    private Transfer? ParseTransfer(JsonElement root)
    {
        if (!root.TryGetProperty("id", out var idProp))
        {
            return null;
        }

        if (!root.TryGetProperty("debit_account_id", out var debitIdProp) ||
            !root.TryGetProperty("credit_account_id", out var creditIdProp))
        {
            return null;
        }

        if (!root.TryGetProperty("amount", out var amountProp))
        {
            return null;
        }

        var transfer = new Transfer
        {
            Id = ParseUInt128(idProp),
            DebitAccountId = ParseUInt128(debitIdProp),
            CreditAccountId = ParseUInt128(creditIdProp),
            Amount = ParseUInt128(amountProp),
            Ledger = root.TryGetProperty("ledger", out var ledger) ? ledger.GetUInt32() : 1u,
            Code = root.TryGetProperty("code", out var code) ? code.GetUInt16() : (ushort)1
        };

        if (root.TryGetProperty("pending_id", out var pendingId))
        {
            transfer.PendingId = ParseUInt128(pendingId);
        }
        if (root.TryGetProperty("user_data_128", out var ud128))
        {
            transfer.UserData128 = ParseUInt128(ud128);
        }
        if (root.TryGetProperty("user_data_64", out var ud64))
        {
            transfer.UserData64 = ud64.GetUInt64();
        }
        if (root.TryGetProperty("user_data_32", out var ud32))
        {
            transfer.UserData32 = ud32.GetUInt32();
        }
        if (root.TryGetProperty("timeout", out var timeout))
        {
            transfer.Timeout = timeout.GetUInt32();
        }
        if (root.TryGetProperty("flags", out var flags))
        {
            transfer.Flags = (TransferFlags)flags.GetInt32();
        }

        return transfer;
    }

    private UInt128 ParseUInt128(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.String)
        {
            return UInt128.Parse(element.GetString()!);
        }
        if (element.ValueKind == JsonValueKind.Number)
        {
            return (UInt128)element.GetUInt64();
        }
        return 0;
    }

    private async Task FlushAccountsAsync(List<Account> accounts)
    {
        if (accounts.Count == 0) return;

        try
        {
            var results = await _client!.CreateAccountsAsync(accounts.ToArray());
            // Could log errors from results
        }
        catch (Exception)
        {
            // Log and continue
        }
    }

    private async Task FlushTransfersAsync(List<Transfer> transfers)
    {
        if (transfers.Count == 0) return;

        try
        {
            var results = await _client!.CreateTransfersAsync(transfers.ToArray());
            // Could log errors from results
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
        if (disposing)
        {
            _client?.Dispose();
        }
        base.Dispose(disposing);
    }
}
