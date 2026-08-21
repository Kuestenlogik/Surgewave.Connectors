using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using TigerBeetle;

namespace Kuestenlogik.Surgewave.Connector.TigerBeetle.Tests;

/// <summary>
/// Tests for how <see cref="TigerBeetleSinkTask"/> turns a Surgewave record into TigerBeetle
/// events. Accounting ids are 128 bit, so anything that quietly truncates them - or a poison
/// record that vanishes without a trace - corrupts a ledger rather than just losing a message.
/// </summary>
public class TigerBeetleSinkTaskTests
{
    /// <summary>Larger than <see cref="ulong.MaxValue"/>, so it only survives as a JSON string.</summary>
    private const string HugeId = "340282366920938463463374607431768211454";

    [Fact]
    public void ParseAccount_WithoutAnId_IsRejected()
    {
        using var task = StartTask();
        using var document = JsonDocument.Parse("""{"ledger":700,"code":10}""");

        Assert.Null(task.ParseAccount(document.RootElement));
    }

    [Fact]
    public void ParseAccount_KeepsAnIdThatDoesNotFitIntoSixtyFourBits()
    {
        using var task = StartTask();
        using var document = JsonDocument.Parse($$"""{"id":"{{HugeId}}","ledger":700,"code":10}""");

        var account = Assert.NotNull(task.ParseAccount(document.RootElement));

        Assert.Equal(UInt128.Parse(HugeId, CultureInfo.InvariantCulture), account.Id);
        Assert.Equal(700u, account.Ledger);
        Assert.Equal((ushort)10, account.Code);
    }

    [Fact]
    public void ParseAccount_DefaultsTheLedgerAndCodeWhenTheyAreAbsent()
    {
        using var task = StartTask();
        using var document = JsonDocument.Parse("""{"id":42}""");

        var account = Assert.NotNull(task.ParseAccount(document.RootElement));

        Assert.Equal((UInt128)42, account.Id);
        Assert.Equal(1u, account.Ledger);
        Assert.Equal((ushort)1, account.Code);
        Assert.Equal(AccountFlags.None, account.Flags);
    }

    [Fact]
    public void ParseAccount_MapsTheOptionalUserDataAndFlags()
    {
        using var task = StartTask();
        using var document = JsonDocument.Parse(
            $$"""{"id":42,"user_data_128":"{{HugeId}}","user_data_64":9007199254740993,"user_data_32":7,"flags":2}""");

        var account = Assert.NotNull(task.ParseAccount(document.RootElement));

        Assert.Equal(UInt128.Parse(HugeId, CultureInfo.InvariantCulture), account.UserData128);
        Assert.Equal(9007199254740993UL, account.UserData64);
        Assert.Equal(7u, account.UserData32);
        Assert.Equal(AccountFlags.DebitsMustNotExceedCredits, account.Flags);
    }

    [Theory]
    [InlineData("""{"credit_account_id":2,"amount":5}""")]
    [InlineData("""{"id":1,"credit_account_id":2,"amount":5}""")]
    [InlineData("""{"id":1,"debit_account_id":2,"amount":5}""")]
    [InlineData("""{"id":1,"debit_account_id":2,"credit_account_id":3}""")]
    public void ParseTransfer_WithoutTheMandatoryFields_IsRejected(string json)
    {
        using var task = StartTask();
        using var document = JsonDocument.Parse(json);

        Assert.Null(task.ParseTransfer(document.RootElement));
    }

    [Fact]
    public void ParseTransfer_MapsEveryFieldOfADoubleEntryMovement()
    {
        using var task = StartTask();
        using var document = JsonDocument.Parse(
            """
            {"id":1,"debit_account_id":2,"credit_account_id":3,"amount":1000,"ledger":700,"code":9,
             "pending_id":4,"user_data_64":11,"user_data_32":12,"timeout":30,"flags":2}
            """);

        var transfer = Assert.NotNull(task.ParseTransfer(document.RootElement));

        Assert.Equal((UInt128)1, transfer.Id);
        Assert.Equal((UInt128)2, transfer.DebitAccountId);
        Assert.Equal((UInt128)3, transfer.CreditAccountId);
        Assert.Equal((UInt128)1000, transfer.Amount);
        Assert.Equal(700u, transfer.Ledger);
        Assert.Equal((ushort)9, transfer.Code);
        Assert.Equal((UInt128)4, transfer.PendingId);
        Assert.Equal(11UL, transfer.UserData64);
        Assert.Equal(12u, transfer.UserData32);
        Assert.Equal(30u, transfer.Timeout);
        Assert.Equal(TransferFlags.Pending, transfer.Flags);
    }

    [Fact]
    public void ParseTransfer_KeepsAnAmountThatDoesNotFitIntoSixtyFourBits()
    {
        using var task = StartTask();
        using var document = JsonDocument.Parse(
            $$"""{"id":1,"debit_account_id":2,"credit_account_id":3,"amount":"{{HugeId}}"}""");

        var transfer = Assert.NotNull(task.ParseTransfer(document.RootElement));

        Assert.Equal(UInt128.Parse(HugeId, CultureInfo.InvariantCulture), transfer.Amount);
    }

    [Fact]
    public async Task PutAsync_WithAPoisonRecord_SurfacesItInsteadOfDroppingItSilently()
    {
        // A ledger entry that cannot be parsed must be visible to the worker (log/DLQ);
        // swallowing it means money-moving messages disappear without a trace.
        var errors = new List<Exception>();
        using var task = StartTask(errors.Add);

        await task.PutAsync([Record("this is not json")], CancellationToken.None);

        Assert.IsType<JsonException>(Assert.Single(errors), exactMatch: false);
    }

    [Fact]
    public async Task PutAsync_KeepsGoingAfterAPoisonRecord()
    {
        var errors = new List<Exception>();
        using var task = StartTask(errors.Add);

        await task.PutAsync([Record("{"), Record("also not json")], CancellationToken.None);

        Assert.Equal(2, errors.Count);
    }

    private static TigerBeetleSinkTask StartTask(
        Action<Exception>? raiseError = null,
        params (string Key, string Value)[] settings)
    {
        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TigerBeetleConnectorConfig.Topics] = "ledger",
            [TigerBeetleConnectorConfig.ClusterAddresses] = "127.0.0.1:3000"
        };

        foreach (var (key, value) in settings)
        {
            config[key] = value;
        }

        var task = new TigerBeetleSinkTask();
        task.Initialize(new TaskContext { RaiseError = raiseError ?? (_ => { }) });

        // Configure without opening a TigerBeetle client - the parsing under test never
        // touches the cluster.
        task.ApplyConfiguration(config);
        return task;
    }

    private static SinkRecord Record(string json) => new()
    {
        Topic = "ledger",
        Partition = 0,
        Offset = 1,
        Value = Encoding.UTF8.GetBytes(json)
    };
}
