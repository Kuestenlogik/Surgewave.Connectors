using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using TigerBeetle;

namespace Kuestenlogik.Surgewave.Connector.TigerBeetle.Tests;

/// <summary>
/// Tests for the records <see cref="TigerBeetleSourceTask"/> emits. TigerBeetle balances and ids
/// are 128 bit values that JSON numbers cannot hold, so they have to travel as strings - a
/// downstream consumer that receives a truncated balance is worse than one that receives nothing.
/// </summary>
public class TigerBeetleSourceTaskTests
{
    private const string Topic = "ledger-events";

    /// <summary>Larger than <see cref="ulong.MaxValue"/>, so a JSON number would lose it.</summary>
    private const string HugeAmount = "340282366920938463463374607431768211454";

    [Fact]
    public void CreateAccountRecord_KeysTheRecordByTheAccountId()
    {
        using var task = StartTask();

        var record = task.CreateAccountRecord(new Account { Id = (UInt128)42, Ledger = 700, Code = 10 });

        Assert.Equal(Topic, record.Topic);
        Assert.Equal("42", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("account", Encoding.UTF8.GetString(record.Headers!["tigerbeetle.type"]));
        Assert.Equal("42", Encoding.UTF8.GetString(record.Headers["tigerbeetle.id"]));
        Assert.Equal("tigerbeetle", record.SourcePartition["source"]);
        Assert.Equal("account", record.SourcePartition["type"]);
        Assert.Equal("42", record.SourceOffset["account_id"]);
    }

    [Fact]
    public void CreateAccountRecord_CarriesTheBalancesAsStringsSoLargeValuesSurvive()
    {
        using var task = StartTask();

        // Balances are server-assigned and read-only on Account, so this pins the
        // serialization FORM (128-bit values as JSON strings, never numbers); the
        // huge-value case itself is covered by the transfer amount below.
        var record = task.CreateAccountRecord(new Account
        {
            Id = (UInt128)42,
            Ledger = 700,
            Code = 10
        });

        using var document = JsonDocument.Parse(record.Value);
        var payload = document.RootElement;
        Assert.Equal("account", payload.GetProperty("type").GetString());
        Assert.Equal(JsonValueKind.String, payload.GetProperty("credits_posted").ValueKind);
        Assert.Equal(JsonValueKind.String, payload.GetProperty("debits_pending").ValueKind);
        Assert.Equal(700, payload.GetProperty("ledger").GetInt32());
        Assert.Equal(10, payload.GetProperty("code").GetInt32());
    }

    [Fact]
    public void CreateTransferRecord_DescribesBothSidesOfTheMovement()
    {
        using var task = StartTask();

        var record = task.CreateTransferRecord(new Transfer
        {
            Id = (UInt128)7,
            DebitAccountId = (UInt128)42,
            CreditAccountId = (UInt128)43,
            Amount = UInt128.Parse(HugeAmount, CultureInfo.InvariantCulture),
            Ledger = 700,
            Code = 9,
            Flags = TransferFlags.Pending
        });

        Assert.Equal("7", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("transfer", Encoding.UTF8.GetString(record.Headers!["tigerbeetle.type"]));
        Assert.Equal("transfer", record.SourcePartition["type"]);
        Assert.Equal("7", record.SourceOffset["transfer_id"]);

        using var document = JsonDocument.Parse(record.Value);
        var payload = document.RootElement;
        Assert.Equal("transfer", payload.GetProperty("type").GetString());
        Assert.Equal("42", payload.GetProperty("debit_account_id").GetString());
        Assert.Equal("43", payload.GetProperty("credit_account_id").GetString());
        Assert.Equal(HugeAmount, payload.GetProperty("amount").GetString());
        Assert.Equal((int)TransferFlags.Pending, payload.GetProperty("flags").GetInt32());
    }

    [Fact]
    public void CreateRecord_GivesEveryRecordItsOwnMessageId()
    {
        using var task = StartTask();

        var first = task.CreateAccountRecord(new Account { Id = (UInt128)1 });
        var second = task.CreateTransferRecord(new Transfer { Id = (UInt128)2 });

        Assert.Equal(1L, MessageIdOf(first));
        Assert.Equal(2L, MessageIdOf(second));
    }

    [Fact]
    public void CreateAccountRecord_UsesTheConfiguredDestinationTopic()
    {
        using var task = StartTask((TigerBeetleConnectorConfig.Topic, "other-ledger"));

        var record = task.CreateAccountRecord(new Account { Id = (UInt128)1 });

        Assert.Equal("other-ledger", record.Topic);
    }

    private static TigerBeetleSourceTask StartTask(params (string Key, string Value)[] settings)
    {
        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TigerBeetleConnectorConfig.Topic] = Topic,
            [TigerBeetleConnectorConfig.ClusterAddresses] = "127.0.0.1:3000"
        };

        foreach (var (key, value) in settings)
        {
            config[key] = value;
        }

        var task = new TigerBeetleSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        // Configure without opening a TigerBeetle client - record building never talks to
        // the cluster.
        task.ApplyConfiguration(config);
        return task;
    }

    private static long MessageIdOf(SourceRecord record) =>
        Convert.ToInt64(record.SourceOffset["message_id"], CultureInfo.InvariantCulture);
}
