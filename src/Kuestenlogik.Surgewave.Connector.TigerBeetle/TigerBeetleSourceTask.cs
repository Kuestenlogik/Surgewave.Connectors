using System.Numerics;
using System.Text;
using System.Text.Json;
using TigerBeetle;
using Kuestenlogik.Surgewave.Connect;

using TBClient = TigerBeetle.Client;

namespace Kuestenlogik.Surgewave.Connector.TigerBeetle;

/// <summary>
/// Task that reads accounts and transfers from TigerBeetle.
/// </summary>
public sealed class TigerBeetleSourceTask : SourceTask
{
    private TBClient? _client;
    private string _topic = null!;
    private int _pollIntervalMs;
    private UInt128[]? _watchAccounts;
    private bool _includeTransfers;
    private int _lookupBatchSize;
    private DateTime _lastPoll = DateTime.MinValue;
    private long _messageId;
    private readonly Dictionary<UInt128, AccountState> _accountStates = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[TigerBeetleConnectorConfig.Topic];

        var addresses = config[TigerBeetleConnectorConfig.ClusterAddresses]
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var clusterId = UInt128.Parse(config.GetValueOrDefault(TigerBeetleConnectorConfig.ClusterId, "0")!);

        var maxConcurrency = uint.Parse(config.GetValueOrDefault(TigerBeetleConnectorConfig.MaxConcurrency,
            TigerBeetleConnectorConfig.DefaultMaxConcurrency.ToString())!);

        _pollIntervalMs = int.Parse(config.GetValueOrDefault(TigerBeetleConnectorConfig.PollIntervalMs,
            TigerBeetleConnectorConfig.DefaultPollIntervalMs.ToString())!);

        _includeTransfers = config.GetValueOrDefault(TigerBeetleConnectorConfig.IncludeTransfers, "true") == "true";

        _lookupBatchSize = int.Parse(config.GetValueOrDefault(TigerBeetleConnectorConfig.LookupBatchSize,
            TigerBeetleConnectorConfig.DefaultLookupBatchSize.ToString())!);

        // Parse watch accounts
        var watchAccountsStr = config.GetValueOrDefault(TigerBeetleConnectorConfig.WatchAccounts, "");
        if (!string.IsNullOrWhiteSpace(watchAccountsStr))
        {
            _watchAccounts = watchAccountsStr
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(s => UInt128.Parse(s))
                .ToArray();
        }

        _client = new TBClient(clusterId, addresses);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if ((DateTime.UtcNow - _lastPoll).TotalMilliseconds < _pollIntervalMs)
        {
            return [];
        }

        _lastPoll = DateTime.UtcNow;
        var records = new List<SourceRecord>();

        if (_watchAccounts == null || _watchAccounts.Length == 0)
        {
            return [];
        }

        try
        {
            // Lookup accounts
            var accounts = await _client!.LookupAccountsAsync(_watchAccounts);

            foreach (var account in accounts)
            {
                // Check if account state changed
                if (_accountStates.TryGetValue(account.Id, out var prevState))
                {
                    if (prevState.CreditsPosted == account.CreditsPosted &&
                        prevState.DebitsPosted == account.DebitsPosted &&
                        prevState.CreditsPending == account.CreditsPending &&
                        prevState.DebitsPending == account.DebitsPending)
                    {
                        continue; // No change
                    }
                }

                // Emit account state change
                var record = CreateAccountRecord(account);
                records.Add(record);

                // Update tracked state
                _accountStates[account.Id] = new AccountState
                {
                    CreditsPosted = account.CreditsPosted,
                    DebitsPosted = account.DebitsPosted,
                    CreditsPending = account.CreditsPending,
                    DebitsPending = account.DebitsPending
                };

                // Optionally lookup transfers for this account
                if (_includeTransfers)
                {
                    var transferRecords = await GetAccountTransfersAsync(account.Id);
                    records.AddRange(transferRecords);
                }
            }
        }
        catch (Exception)
        {
            // Log and continue
        }

        return records;
    }

    private async Task<IEnumerable<SourceRecord>> GetAccountTransfersAsync(UInt128 accountId)
    {
        var records = new List<SourceRecord>();

        try
        {
            // Lookup account transfers
            var filter = new AccountFilter
            {
                AccountId = accountId,
                TimestampMin = 0,
                TimestampMax = 0,
                Limit = (uint)_lookupBatchSize,
                Flags = AccountFilterFlags.Credits | AccountFilterFlags.Debits
            };

            var transfers = await _client!.GetAccountTransfersAsync(filter);

            foreach (var transfer in transfers)
            {
                var record = CreateTransferRecord(transfer);
                records.Add(record);
            }
        }
        catch
        {
            // Log and continue
        }

        return records;
    }

    private SourceRecord CreateAccountRecord(Account account)
    {
        var msgId = Interlocked.Increment(ref _messageId);

        var payload = new
        {
            type = "account",
            id = account.Id.ToString(),
            debits_pending = account.DebitsPending.ToString(),
            debits_posted = account.DebitsPosted.ToString(),
            credits_pending = account.CreditsPending.ToString(),
            credits_posted = account.CreditsPosted.ToString(),
            user_data_128 = account.UserData128.ToString(),
            user_data_64 = account.UserData64.ToString(),
            user_data_32 = account.UserData32.ToString(),
            ledger = account.Ledger,
            code = account.Code,
            flags = (int)account.Flags,
            timestamp = account.Timestamp.ToString()
        };

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["source"] = "tigerbeetle",
                ["type"] = "account"
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["account_id"] = account.Id.ToString()
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(account.Id.ToString()),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["tigerbeetle.type"] = Encoding.UTF8.GetBytes("account"),
                ["tigerbeetle.id"] = Encoding.UTF8.GetBytes(account.Id.ToString())
            }
        };
    }

    private SourceRecord CreateTransferRecord(Transfer transfer)
    {
        var msgId = Interlocked.Increment(ref _messageId);

        var payload = new
        {
            type = "transfer",
            id = transfer.Id.ToString(),
            debit_account_id = transfer.DebitAccountId.ToString(),
            credit_account_id = transfer.CreditAccountId.ToString(),
            amount = transfer.Amount.ToString(),
            pending_id = transfer.PendingId.ToString(),
            user_data_128 = transfer.UserData128.ToString(),
            user_data_64 = transfer.UserData64.ToString(),
            user_data_32 = transfer.UserData32.ToString(),
            timeout = transfer.Timeout,
            ledger = transfer.Ledger,
            code = transfer.Code,
            flags = (int)transfer.Flags,
            timestamp = transfer.Timestamp.ToString()
        };

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["source"] = "tigerbeetle",
                ["type"] = "transfer"
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["transfer_id"] = transfer.Id.ToString()
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(transfer.Id.ToString()),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["tigerbeetle.type"] = Encoding.UTF8.GetBytes("transfer"),
                ["tigerbeetle.id"] = Encoding.UTF8.GetBytes(transfer.Id.ToString())
            }
        };
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

    private sealed class AccountState
    {
        public UInt128 CreditsPosted { get; set; }
        public UInt128 DebitsPosted { get; set; }
        public UInt128 CreditsPending { get; set; }
        public UInt128 DebitsPending { get; set; }
    }
}
