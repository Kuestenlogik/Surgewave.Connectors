using System.Text;
using System.Text.Json;
using Renci.SshNet;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sftp;

/// <summary>
/// Task that uploads files to an SFTP server.
/// </summary>
public sealed class SftpSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _host = "";
    private int _port = SftpConnectorConfig.DefaultPort;
    private string _username = "";
    private string _password = "";
    private string _privateKeyPath = "";
    private string _privateKeyPassphrase = "";
    private string _privateKeyContent = "";
    private int _timeoutSeconds = SftpConnectorConfig.DefaultTimeoutSeconds;
    private string _outputPath = "/";
    private string _outputMode = SftpConnectorConfig.DefaultOutputMode;
    private string _fileNameField = "";
    private string _contentField = "";
    private bool _createDirectories = true;
    private bool _overwrite = true;
    private string _tempSuffix = SftpConnectorConfig.DefaultTempSuffix;

    private SftpClient? _client;
    private readonly HashSet<string> _createdDirectories = [];

    public override void Start(IDictionary<string, string> config)
    {
        _host = config[SftpConnectorConfig.HostConfig];
        _port = config.TryGetValue(SftpConnectorConfig.PortConfig, out var port)
            ? int.Parse(port) : SftpConnectorConfig.DefaultPort;
        _username = config[SftpConnectorConfig.UsernameConfig];
        _password = config.TryGetValue(SftpConnectorConfig.PasswordConfig, out var pass) ? pass : "";
        _privateKeyPath = config.TryGetValue(SftpConnectorConfig.PrivateKeyPathConfig, out var keyPath) ? keyPath : "";
        _privateKeyPassphrase = config.TryGetValue(SftpConnectorConfig.PrivateKeyPassphraseConfig, out var keyPass) ? keyPass : "";
        _privateKeyContent = config.TryGetValue(SftpConnectorConfig.PrivateKeyContentConfig, out var keyContent) ? keyContent : "";
        _timeoutSeconds = config.TryGetValue(SftpConnectorConfig.TimeoutSecondsConfig, out var timeout)
            ? int.Parse(timeout) : SftpConnectorConfig.DefaultTimeoutSeconds;

        _outputPath = config.TryGetValue(SftpConnectorConfig.OutputPathConfig, out var path) ? path : "/";
        _outputMode = config.TryGetValue(SftpConnectorConfig.OutputModeConfig, out var mode)
            ? mode : SftpConnectorConfig.DefaultOutputMode;
        _fileNameField = config.TryGetValue(SftpConnectorConfig.FileNameFieldConfig, out var fnf) ? fnf : "";
        _contentField = config.TryGetValue(SftpConnectorConfig.ContentFieldConfig, out var cf) ? cf : "";
        _createDirectories = !config.TryGetValue(SftpConnectorConfig.CreateDirectoriesConfig, out var create) ||
                             bool.Parse(create);
        _overwrite = !config.TryGetValue(SftpConnectorConfig.OverwriteConfig, out var overwrite) ||
                     bool.Parse(overwrite);
        _tempSuffix = config.TryGetValue(SftpConnectorConfig.TempSuffixConfig, out var suffix)
            ? suffix : SftpConnectorConfig.DefaultTempSuffix;

        // Connect
        _client = CreateClient();
        _client.Connect();
    }

    public override void Stop()
    {
        _client?.Dispose();
        _client = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _client?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        // Skip if no records to process
        if (records.Count == 0 || records.All(r => r.Value == null || r.Value.Length == 0))
            return Task.CompletedTask;

        if (_client == null || !_client.IsConnected)
        {
            _client?.Dispose();
            _client = CreateClient();
            _client.Connect();
        }

        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0)
                continue;

            try
            {
                var (fileName, content) = ParseRecord(record);

                if (string.IsNullOrWhiteSpace(fileName))
                    continue;

                // Resolve output path
                var fullPath = ResolvePath(_outputPath, record, fileName);

                // Create directories if needed
                if (_createDirectories)
                {
                    var directory = GetDirectoryPath(fullPath);
                    EnsureDirectoryExists(directory);
                }

                // Upload file
                if (_outputMode == SftpConnectorConfig.OutputModeAppend)
                {
                    AppendToFile(fullPath, content);
                }
                else
                {
                    WriteFile(fullPath, content);
                }
            }
            catch (JsonException)
            {
                // Skip invalid JSON records
            }
            catch (Renci.SshNet.Common.SshException)
            {
                // Connection issue - will retry on next batch
                _client?.Dispose();
                _client = null;
            }
        }

        return Task.CompletedTask;
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // Nothing to flush - files are written immediately
        return Task.CompletedTask;
    }

    private SftpClient CreateClient()
    {
        var connectionInfo = CreateConnectionInfo();
        var client = new SftpClient(connectionInfo);
        client.OperationTimeout = TimeSpan.FromSeconds(_timeoutSeconds);
        return client;
    }

    private ConnectionInfo CreateConnectionInfo()
    {
        var authMethods = new List<AuthenticationMethod>();

        // SSH key authentication
        if (!string.IsNullOrWhiteSpace(_privateKeyPath) && File.Exists(_privateKeyPath))
        {
            var keyFile = string.IsNullOrWhiteSpace(_privateKeyPassphrase)
                ? new PrivateKeyFile(_privateKeyPath)
                : new PrivateKeyFile(_privateKeyPath, _privateKeyPassphrase);
            authMethods.Add(new PrivateKeyAuthenticationMethod(_username, keyFile));
        }
        else if (!string.IsNullOrWhiteSpace(_privateKeyContent))
        {
            using var keyStream = new MemoryStream(Encoding.UTF8.GetBytes(_privateKeyContent));
            var keyFile = string.IsNullOrWhiteSpace(_privateKeyPassphrase)
                ? new PrivateKeyFile(keyStream)
                : new PrivateKeyFile(keyStream, _privateKeyPassphrase);
            authMethods.Add(new PrivateKeyAuthenticationMethod(_username, keyFile));
        }

        // Password authentication
        if (!string.IsNullOrWhiteSpace(_password))
        {
            authMethods.Add(new PasswordAuthenticationMethod(_username, _password));
        }

        if (authMethods.Count == 0)
        {
            throw new InvalidOperationException("No authentication method configured. Provide password or private key.");
        }

        return new ConnectionInfo(_host, _port, _username, [.. authMethods])
        {
            Timeout = TimeSpan.FromSeconds(_timeoutSeconds)
        };
    }

    private (string fileName, byte[] content) ParseRecord(SinkRecord record)
    {
        var rawValue = Encoding.UTF8.GetString(record.Value);

        // Try to parse as JSON
        try
        {
            using var doc = JsonDocument.Parse(rawValue);
            var root = doc.RootElement;

            // Get filename from field or generate
            var fileName = !string.IsNullOrWhiteSpace(_fileNameField) &&
                          root.TryGetProperty(_fileNameField, out var fn)
                ? fn.GetString() ?? ""
                : "";

            // Get content from field or use full value
            byte[] content;
            if (!string.IsNullOrWhiteSpace(_contentField) &&
                root.TryGetProperty(_contentField, out var contentProp))
            {
                var contentStr = contentProp.GetString() ?? "";

                // Try base64 decode
                try
                {
                    content = Convert.FromBase64String(contentStr);
                }
                catch (FormatException)
                {
                    content = Encoding.UTF8.GetBytes(contentStr);
                }
            }
            else
            {
                content = record.Value;
            }

            return (fileName, content);
        }
        catch (JsonException)
        {
            // Not JSON - use raw value
            return ("", record.Value);
        }
    }

    private string ResolvePath(string pathTemplate, SinkRecord record, string fileName)
    {
        var path = pathTemplate
            .Replace("${topic}", record.Topic)
            .Replace("${partition}", record.Partition.ToString())
            .Replace("${offset}", record.Offset.ToString())
            .Replace("${timestamp}", DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss"));

        if (record.Key != null)
        {
            var key = Encoding.UTF8.GetString(record.Key);
            // Sanitize key for filename
            key = SanitizeFileName(key);
            path = path.Replace("${key}", key);
        }
        else
        {
            path = path.Replace("${key}", record.Offset.ToString());
        }

        // If path is a directory and we have a filename, combine them
        if (!string.IsNullOrWhiteSpace(fileName))
        {
            if (path.EndsWith('/'))
            {
                path += fileName;
            }
            else if (!path.Contains('.'))
            {
                // Looks like a directory
                path = path + "/" + fileName;
            }
        }
        else if (!path.Contains('.'))
        {
            // No extension - generate filename
            path = path.TrimEnd('/') + $"/{record.Topic}_{record.Partition}_{record.Offset}.dat";
        }

        return path.Replace('\\', '/');
    }

    private static string SanitizeFileName(string name)
    {
        var invalid = new[] { '/', '\\', ':', '*', '?', '"', '<', '>', '|' };
        foreach (var c in invalid)
        {
            name = name.Replace(c, '_');
        }
        return name;
    }

    private static string GetDirectoryPath(string fullPath)
    {
        var lastSlash = fullPath.LastIndexOf('/');
        return lastSlash > 0 ? fullPath[..lastSlash] : "/";
    }

    private void EnsureDirectoryExists(string path)
    {
        if (_client == null || string.IsNullOrWhiteSpace(path) || path == "/")
            return;

        if (_createdDirectories.Contains(path))
            return;

        try
        {
            if (_client.Exists(path))
            {
                _createdDirectories.Add(path);
                return;
            }
        }
        catch
        {
            // Might not exist
        }

        // Create parent first
        var parent = GetDirectoryPath(path);
        if (parent != "/" && parent != path)
        {
            EnsureDirectoryExists(parent);
        }

        try
        {
            _client.CreateDirectory(path);
            _createdDirectories.Add(path);
        }
        catch
        {
            // Directory might already exist (race condition)
            _createdDirectories.Add(path);
        }
    }

    private void WriteFile(string path, byte[] content)
    {
        if (_client == null)
            return;

        var useTempFile = !string.IsNullOrWhiteSpace(_tempSuffix);
        var tempPath = useTempFile ? path + _tempSuffix : path;

        // Check if file exists and we shouldn't overwrite
        if (!_overwrite && _client.Exists(path))
            return;

        // Upload to temp file first
        using (var stream = new MemoryStream(content))
        {
            _client.UploadFile(stream, tempPath, _overwrite);
        }

        // Rename to final path
        if (useTempFile)
        {
            if (_client.Exists(path))
            {
                _client.DeleteFile(path);
            }
            _client.RenameFile(tempPath, path);
        }
    }

    private void AppendToFile(string path, byte[] content)
    {
        if (_client == null)
            return;

        try
        {
            using var stream = _client.AppendText(path);
            stream.Write(Encoding.UTF8.GetString(content));
            stream.WriteLine();
        }
        catch
        {
            // File doesn't exist - create it
            WriteFile(path, content);
        }
    }
}
