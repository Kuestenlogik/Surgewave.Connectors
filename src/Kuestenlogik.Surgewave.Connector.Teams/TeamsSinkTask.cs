using System.Text;
using System.Text.Json;
using Azure.Identity;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Teams;

/// <summary>
/// Task that sends messages to Microsoft Teams channels or chats via Graph API.
/// </summary>
public sealed class TeamsSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private GraphServiceClient? _graphClient;
    private string? _teamId;
    private string? _channelId;
    private string? _chatId;
    private string _messageFormat = TeamsConnectorConfig.DefaultMessageFormat;
    private string _defaultSubject = "";

    public override void Start(IDictionary<string, string> config)
    {
        var tenantId = config[TeamsConnectorConfig.TenantId];
        var clientId = config[TeamsConnectorConfig.ClientId];
        var clientSecret = config[TeamsConnectorConfig.ClientSecret];

        var credential = new ClientSecretCredential(tenantId, clientId, clientSecret);
        _graphClient = new GraphServiceClient(credential);

        config.TryGetValue(TeamsConnectorConfig.TeamId, out _teamId);
        config.TryGetValue(TeamsConnectorConfig.ChannelId, out _channelId);
        config.TryGetValue(TeamsConnectorConfig.ChatId, out _chatId);

        if (config.TryGetValue(TeamsConnectorConfig.MessageFormat, out var format))
            _messageFormat = format;

        config.TryGetValue(TeamsConnectorConfig.DefaultSubject, out _defaultSubject!);
        _defaultSubject ??= "";
    }

    public override void Stop()
    {
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
            _graphClient?.Dispose();
            _graphClient = null;
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0 || _graphClient == null) return;

        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0) continue;

            try
            {
                var content = Encoding.UTF8.GetString(record.Value);
                var message = CreateMessage(content, record);

                if (!string.IsNullOrEmpty(_chatId))
                {
                    await _graphClient.Chats[_chatId].Messages.PostAsync(message, cancellationToken: cancellationToken);
                }
                else if (!string.IsNullOrEmpty(_teamId) && !string.IsNullOrEmpty(_channelId))
                {
                    await _graphClient.Teams[_teamId].Channels[_channelId].Messages.PostAsync(message, cancellationToken: cancellationToken);
                }
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(ex);
                throw;
            }
        }
    }

    private ChatMessage CreateMessage(string content, SinkRecord record)
    {
        var message = new ChatMessage();

        switch (_messageFormat)
        {
            case TeamsConnectorConfig.FormatHtml:
                message.Body = new ItemBody
                {
                    ContentType = BodyType.Html,
                    Content = content
                };
                break;

            case TeamsConnectorConfig.FormatAdaptiveCard:
                message.Body = new ItemBody
                {
                    ContentType = BodyType.Html,
                    Content = "<attachment id=\"adaptive-card\"></attachment>"
                };
                message.Attachments =
                [
                    new ChatMessageAttachment
                    {
                        Id = "adaptive-card",
                        ContentType = "application/vnd.microsoft.card.adaptive",
                        Content = content
                    }
                ];
                break;

            default: // Text
                message.Body = new ItemBody
                {
                    ContentType = BodyType.Text,
                    Content = content
                };
                break;
        }

        // Set subject from header or default
        var subject = _defaultSubject;
        if (record.Headers?.TryGetValue("teams_subject", out var subjectBytes) == true)
            subject = Encoding.UTF8.GetString(subjectBytes);

        if (!string.IsNullOrEmpty(subject))
            message.Subject = subject;

        // Add importance from header
        if (record.Headers?.TryGetValue("teams_importance", out var importanceBytes) == true)
        {
            var importance = Encoding.UTF8.GetString(importanceBytes).ToLowerInvariant();
            message.Importance = importance switch
            {
                "urgent" => ChatMessageImportance.Urgent,
                "high" => ChatMessageImportance.High,
                _ => ChatMessageImportance.Normal
            };
        }

        return message;
    }
}
