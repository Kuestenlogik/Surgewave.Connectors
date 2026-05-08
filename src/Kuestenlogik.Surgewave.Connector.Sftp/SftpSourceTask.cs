using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Renci.SshNet;
using Renci.SshNet.Sftp;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sftp;

internal static class JsonOptions
{
    public static readonly JsonSerializerOptions Default = new()
    {
        WriteIndented = false,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };
}

/// <summary>
/// Task that polls an SFTP server for files and produces their contents as records.
/// </summary>
public sealed class SftpSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _topic = "";
    private string _host = "";
    private int _port = SftpConnectorConfig.DefaultPort;
    private string _username = "";
    private string _password = "";
    private string _privateKeyPath = "";
    private string _privateKeyPassphrase = "";
    private string _privateKeyContent = "";
    private int _timeoutSeconds = SftpConnectorConfig.DefaultTimeoutSeconds;
    private string _remotePath = "/";
    private Regex? _filePattern;
    private bool _recursive;
    private int _pollIntervalMs = SftpConnectorConfig.DefaultPollIntervalMs;
    private bool _deleteAfterRead;
    private bool _moveAfterRead;
    private string _moveToPath = "";
    private bool _includeMetadata = true;
    private long _maxFileSizeBytes = SftpConnectorConfig.DefaultMaxFileSizeBytes;
    private long _minFileSizeBytes;
    private string _startFrom = SftpConnectorConfig.DefaultStartFrom;

    private SftpClient? _client;
    private DateTimeOffset _lastPollTime = DateTimeOffset.MinValue;
    private DateTime _lastModifiedThreshold = DateTime.MinValue;
    private readonly HashSet<string> _processedFiles = [];
    private readonly Dictionary<string, object> _sourcePartition = [];
    private bool _initialized;

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[SftpConnectorConfig.TopicConfig];
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

        _remotePath = config.TryGetValue(SftpConnectorConfig.RemotePathConfig, out var path) ? path : "/";
        _recursive = config.TryGetValue(SftpConnectorConfig.RecursiveConfig, out var recursive) && bool.Parse(recursive);
        _pollIntervalMs = config.TryGetValue(SftpConnectorConfig.PollIntervalMsConfig, out var poll)
            ? int.Parse(poll) : SftpConnectorConfig.DefaultPollIntervalMs;
        _deleteAfterRead = config.TryGetValue(SftpConnectorConfig.DeleteAfterReadConfig, out var delete) && bool.Parse(delete);
        _moveAfterRead = config.TryGetValue(SftpConnectorConfig.MoveAfterReadConfig, out var move) && bool.Parse(move);
        _moveToPath = config.TryGetValue(SftpConnectorConfig.MoveToPathConfig, out var moveTo) ? moveTo : "";
        _includeMetadata = !config.TryGetValue(SftpConnectorConfig.IncludeMetadataConfig, out var meta) || bool.Parse(meta);
        _maxFileSizeBytes = config.TryGetValue(SftpConnectorConfig.MaxFileSizeBytesConfig, out var maxSize)
            ? long.Parse(maxSize) : SftpConnectorConfig.DefaultMaxFileSizeBytes;
        _minFileSizeBytes = config.TryGetValue(SftpConnectorConfig.MinFileSizeBytesConfig, out var minSize)
            ? long.Parse(minSize) : 0;
        _startFrom = config.TryGetValue(SftpConnectorConfig.StartFromConfig, out var start)
            ? start : SftpConnectorConfig.DefaultStartFrom;

        // Compile file pattern
        if (config.TryGetValue(SftpConnectorConfig.FilePatternConfig, out var pattern) &&
            !string.IsNullOrWhiteSpace(pattern) && pattern != "*")
        {
            _filePattern = GlobToRegex(pattern);
        }

        _sourcePartition["host"] = _host;
        _sourcePartition["path"] = _remotePath;

        // Restore offset
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null)
        {
            if (storedOffset.TryGetValue(SftpConnectorConfig.OffsetLastModified, out var lastMod))
            {
                _lastModifiedThreshold = DateTime.FromBinary(Convert.ToInt64(lastMod));
            }
        }
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

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var elapsed = (now - _lastPollTime).TotalMilliseconds;

        if (elapsed < _pollIntervalMs)
        {
            var waitTime = (int)(_pollIntervalMs - elapsed);
            await Task.Delay(waitTime, cancellationToken);
        }

        try
        {
            // Connect if needed
            if (_client == null || !_client.IsConnected)
            {
                _client = CreateClient();
                _client.Connect();
            }

            // Initialize on first poll
            if (!_initialized)
            {
                _initialized = true;
                if (_startFrom == SftpConnectorConfig.StartFromLatest)
                {
                    // Mark current files as processed
                    var existingFiles = ListFiles(_remotePath, _recursive);
                    foreach (var file in existingFiles)
                    {
                        _processedFiles.Add(file.FullName);
                        if (file.LastWriteTime > _lastModifiedThreshold)
                        {
                            _lastModifiedThreshold = file.LastWriteTime;
                        }
                    }
                    _lastPollTime = DateTimeOffset.UtcNow;
                    return [];
                }
            }

            // List files
            var files = ListFiles(_remotePath, _recursive)
                .Where(f => !_processedFiles.Contains(f.FullName))
                .Where(f => f.Length >= _minFileSizeBytes && f.Length <= _maxFileSizeBytes)
                .Where(f => _filePattern == null || _filePattern.IsMatch(f.Name))
                .OrderBy(f => f.LastWriteTime)
                .ToList();

            var records = new List<SourceRecord>();

            foreach (var file in files)
            {
                try
                {
                    var content = ReadFile(file.FullName);
                    var record = CreateRecord(file, content);
                    records.Add(record);

                    // Post-process file
                    if (_deleteAfterRead)
                    {
                        _client.DeleteFile(file.FullName);
                    }
                    else if (_moveAfterRead && !string.IsNullOrWhiteSpace(_moveToPath))
                    {
                        var destPath = Path.Combine(_moveToPath, file.Name).Replace('\\', '/');
                        _client.RenameFile(file.FullName, destPath);
                    }

                    _processedFiles.Add(file.FullName);
                    if (file.LastWriteTime > _lastModifiedThreshold)
                    {
                        _lastModifiedThreshold = file.LastWriteTime;
                    }
                }
                catch (Exception)
                {
                    // Skip files that fail to read
                }
            }

            _lastPollTime = DateTimeOffset.UtcNow;
            return records;
        }
        catch (Renci.SshNet.Common.SshException)
        {
            _client?.Dispose();
            _client = null;
            await Task.Delay(5000, cancellationToken);
            return [];
        }
        catch (Exception)
        {
            await Task.Delay(5000, cancellationToken);
            return [];
        }
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

    private List<ISftpFile> ListFiles(string path, bool recursive)
    {
        var files = new List<ISftpFile>();

        if (_client == null)
            return files;

        try
        {
            var entries = _client.ListDirectory(path);

            foreach (var entry in entries)
            {
                if (entry.Name == "." || entry.Name == "..")
                    continue;

                if (entry.IsRegularFile)
                {
                    files.Add(entry);
                }
                else if (entry.IsDirectory && recursive)
                {
                    files.AddRange(ListFiles(entry.FullName, true));
                }
            }
        }
        catch
        {
            // Directory may not exist or permission denied
        }

        return files;
    }

    private byte[] ReadFile(string path)
    {
        if (_client == null)
            return [];

        using var stream = new MemoryStream();
        _client.DownloadFile(path, stream);
        return stream.ToArray();
    }

    private SourceRecord CreateRecord(ISftpFile file, byte[] content)
    {
        byte[] value;

        if (_includeMetadata)
        {
            var metadata = new Dictionary<string, object?>
            {
                ["path"] = file.FullName,
                ["name"] = file.Name,
                ["size"] = file.Length,
                ["lastModified"] = file.LastWriteTime.ToString("o"),
                ["lastAccessed"] = file.LastAccessTime.ToString("o"),
                ["permissions"] = file.Attributes.GetPermissionString(),
                ["owner"] = file.UserId,
                ["group"] = file.GroupId,
                ["content"] = Convert.ToBase64String(content)
            };

            value = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(metadata, JsonOptions.Default));
        }
        else
        {
            value = content;
        }

        var offset = new Dictionary<string, object>
        {
            [SftpConnectorConfig.OffsetLastModified] = file.LastWriteTime.ToBinary(),
            [SftpConnectorConfig.OffsetLastFileName] = file.FullName,
            [SftpConnectorConfig.OffsetLastPoll] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = offset,
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(file.FullName),
            Value = value,
            Timestamp = new DateTimeOffset(file.LastWriteTime)
        };
    }

    private static Regex GlobToRegex(string glob)
    {
        var pattern = "^" + Regex.Escape(glob)
            .Replace("\\*\\*", ".*")
            .Replace("\\*", "[^/]*")
            .Replace("\\?", ".") + "$";

        return new Regex(pattern, RegexOptions.Compiled | RegexOptions.IgnoreCase);
    }
}

internal static class SftpAttributesExtensions
{
    public static string GetPermissionString(this SftpFileAttributes attrs)
    {
        var perms = new char[9];
        perms[0] = (attrs.OwnerCanRead) ? 'r' : '-';
        perms[1] = (attrs.OwnerCanWrite) ? 'w' : '-';
        perms[2] = (attrs.OwnerCanExecute) ? 'x' : '-';
        perms[3] = (attrs.GroupCanRead) ? 'r' : '-';
        perms[4] = (attrs.GroupCanWrite) ? 'w' : '-';
        perms[5] = (attrs.GroupCanExecute) ? 'x' : '-';
        perms[6] = (attrs.OthersCanRead) ? 'r' : '-';
        perms[7] = (attrs.OthersCanWrite) ? 'w' : '-';
        perms[8] = (attrs.OthersCanExecute) ? 'x' : '-';
        return new string(perms);
    }
}
