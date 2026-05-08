using System.Diagnostics.CodeAnalysis;
using System.Text;
using Discord;
using Discord.WebSocket;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Discord;

/// <summary>
/// Task that sends messages to Discord channels.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class DiscordSinkTask : SinkTask
{
    private DiscordSocketClient? _client;
    private ulong _defaultChannelId;
    private string _messageFormat = "text";
    private int _embedColor;
    private string? _embedTitle;
    private TaskCompletionSource _readyTcs = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var token = config[DiscordConnectorConfig.BotToken];
        _defaultChannelId = ulong.Parse(config[DiscordConnectorConfig.DefaultChannelId]);
        _messageFormat = config.TryGetValue(DiscordConnectorConfig.MessageFormat, out var format) ? format : "text";
        _embedColor = int.Parse(config.TryGetValue(DiscordConnectorConfig.EmbedColor, out var color)
            ? color : DiscordConnectorConfig.DefaultEmbedColor.ToString());
        _embedTitle = config.TryGetValue(DiscordConnectorConfig.EmbedTitle, out var title) ? title : null;

        var socketConfig = new DiscordSocketConfig
        {
            GatewayIntents = GatewayIntents.Guilds
        };

        _client = new DiscordSocketClient(socketConfig);
        _client.Ready += () => { _readyTcs.TrySetResult(); return Task.CompletedTask; };

        // Start connection and wait for ready
        Task.Run(async () =>
        {
            await _client.LoginAsync(TokenType.Bot, token);
            await _client.StartAsync();
            await _readyTcs.Task;
        }).GetAwaiter().GetResult();
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Value == null) continue;

            // Get target channel from headers or use default
            var channelId = _defaultChannelId;
            if (record.Headers?.TryGetValue("discord.channel.id", out var channelBytes) == true)
            {
                if (ulong.TryParse(Encoding.UTF8.GetString(channelBytes), out var parsed))
                    channelId = parsed;
            }

            var channel = _client!.GetChannel(channelId) as IMessageChannel;
            if (channel == null) continue;

            var content = Encoding.UTF8.GetString(record.Value);

            if (_messageFormat == "embed")
            {
                var title = _embedTitle?.Replace("${topic}", record.Topic) ?? record.Topic;
                var embed = new EmbedBuilder()
                    .WithTitle(title)
                    .WithDescription(content)
                    .WithColor(new Color((uint)_embedColor))
                    .WithTimestamp(record.Timestamp == default ? DateTimeOffset.UtcNow : record.Timestamp)
                    .Build();

                await channel.SendMessageAsync(embed: embed);
            }
            else
            {
                await channel.SendMessageAsync(content);
            }
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public override void Stop()
    {
        _client?.StopAsync().GetAwaiter().GetResult();
        _client?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }
}
