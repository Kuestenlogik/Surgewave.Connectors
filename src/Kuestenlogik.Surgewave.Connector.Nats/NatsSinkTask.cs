using System.Text;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nats;

/// <summary>
/// Task that publishes records to NATS JetStream.
/// </summary>
public sealed class NatsSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _natsUrl = NatsConnectorConfig.DefaultUrl;
    private string _streamName = "";
    private int _publishTimeoutMs = NatsConnectorConfig.DefaultPublishTimeoutMs;
    private int _retries = NatsConnectorConfig.DefaultRetries;
    private string? _credentialsFile;
    private string? _token;
    private string? _username;
    private string? _password;

    private NatsConnection? _connection;
    private NatsJSContext? _jetStream;

    public override void Start(IDictionary<string, string> config)
    {
        if (config.TryGetValue(NatsConnectorConfig.Url, out var url))
            _natsUrl = url;
        _streamName = config[NatsConnectorConfig.StreamName];

        if (config.TryGetValue(NatsConnectorConfig.PublishTimeoutMs, out var publishTimeout))
            _publishTimeoutMs = int.Parse(publishTimeout);
        if (config.TryGetValue(NatsConnectorConfig.Retries, out var retries))
            _retries = int.Parse(retries);
        if (config.TryGetValue(NatsConnectorConfig.CredentialsFile, out var creds))
            _credentialsFile = creds;
        if (config.TryGetValue(NatsConnectorConfig.Token, out var token))
            _token = token;
        if (config.TryGetValue(NatsConnectorConfig.Username, out var username))
            _username = username;
        if (config.TryGetValue(NatsConnectorConfig.Password, out var password))
            _password = password;

        ConnectAsync().GetAwaiter().GetResult();
    }

    private async Task ConnectAsync()
    {
        var opts = new NatsOpts
        {
            Url = _natsUrl
        };

        if (!string.IsNullOrEmpty(_credentialsFile))
        {
            opts = opts with { AuthOpts = NatsAuthOpts.Default with { CredsFile = _credentialsFile } };
        }
        else if (!string.IsNullOrEmpty(_token))
        {
            opts = opts with { AuthOpts = NatsAuthOpts.Default with { Token = _token } };
        }
        else if (!string.IsNullOrEmpty(_username) && !string.IsNullOrEmpty(_password))
        {
            opts = opts with { AuthOpts = NatsAuthOpts.Default with { Username = _username, Password = _password } };
        }

        _connection = new NatsConnection(opts);
        await _connection.ConnectAsync();

        _jetStream = new NatsJSContext(_connection);
    }

    public override void Stop()
    {
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            try { _connection?.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { /* ignore */ }
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0 || _jetStream == null)
            return;

        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0)
                continue;

            // Use the Surgewave topic as NATS subject, or extract from key if available
            var subject = record.Key != null 
                ? Encoding.UTF8.GetString(record.Key) 
                : $"{_streamName}.{record.Topic}";

            var lastException = default(Exception);
            for (var attempt = 0; attempt <= _retries; attempt++)
            {
                try
                {
                    using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    timeoutCts.CancelAfter(_publishTimeoutMs);

                    var ack = await _jetStream.PublishAsync(
                        subject,
                        record.Value,
                        cancellationToken: timeoutCts.Token);

                    // Check for publish errors
                    ack.EnsureSuccess();
                    lastException = null;
                    break;
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested && attempt < _retries)
                {
                    lastException = ex;
                    await Task.Delay(100 * (attempt + 1), cancellationToken);
                }
            }

            if (lastException != null)
            {
                throw new InvalidOperationException(
                    $"Failed to publish message to NATS after {_retries + 1} attempts", 
                    lastException);
            }
        }
    }
}
