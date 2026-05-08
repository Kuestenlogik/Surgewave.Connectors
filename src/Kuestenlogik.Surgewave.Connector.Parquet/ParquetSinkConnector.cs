using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Parquet;

/// <summary>
/// Sink connector that writes records to Apache Parquet files.
/// Supports configurable compression, row groups, and file rotation.
/// </summary>
public sealed class ParquetSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(ParquetSinkTask);

    private Dictionary<string, string> _config = new();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(ParquetConnectorConfig.Topics, ConfigType.String, "", Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
        .Define(ParquetConnectorConfig.OutputPath, ConfigType.String, "", Importance.High,
            "Output directory or file path for Parquet files", EditorHint.FilePath)
        .Define(ParquetConnectorConfig.OutputMode, ConfigType.String, ParquetConnectorConfig.DefaultOutputMode, Importance.Medium,
            "Output mode: 'append', 'overwrite', or 'rolling'", EditorHint.Select, options: ["append", "overwrite", "rolling"])
        .Define(ParquetConnectorConfig.MaxRecordsPerFile, ConfigType.Int, ParquetConnectorConfig.DefaultMaxRecordsPerFile, Importance.Medium,
            "Maximum records per file for rolling mode (0 = unlimited)")
        .Define(ParquetConnectorConfig.FileNamePattern, ConfigType.String, ParquetConnectorConfig.DefaultFileNamePattern, Importance.Medium,
            "File name pattern for rolling mode (supports ${topic}, ${timestamp}, ${partition})")
        .Define(ParquetConnectorConfig.CompressionCodec, ConfigType.String, ParquetConnectorConfig.DefaultCompressionCodec, Importance.Medium,
            "Compression codec: 'none', 'gzip', 'snappy', 'lz4', 'zstd', 'brotli'", EditorHint.Select, options: ["none", "snappy", "gzip", "lzo", "brotli", "zstd"])
        .Define(ParquetConnectorConfig.RowGroupSize, ConfigType.Int, ParquetConnectorConfig.DefaultRowGroupSize, Importance.Low,
            "Number of rows per row group");

    public override void Start(IDictionary<string, string> config)
    {
        _config = new Dictionary<string, string>(config);

        if (!config.TryGetValue(ParquetConnectorConfig.Topics, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Missing required config: {ParquetConnectorConfig.Topics}");

        if (!config.TryGetValue(ParquetConnectorConfig.OutputPath, out var outputPath) || string.IsNullOrEmpty(outputPath))
            throw new ArgumentException($"Missing required config: {ParquetConnectorConfig.OutputPath}");

        // Validate output mode
        var mode = config.TryGetValue(ParquetConnectorConfig.OutputMode, out var m) ? m : ParquetConnectorConfig.DefaultOutputMode;
        if (mode is not (ParquetConnectorConfig.OutputModeAppend or ParquetConnectorConfig.OutputModeOverwrite or ParquetConnectorConfig.OutputModeRolling))
            throw new ArgumentException($"Invalid output mode: {mode}. Must be 'append', 'overwrite', or 'rolling'");

        // Validate compression codec
        var codec = config.TryGetValue(ParquetConnectorConfig.CompressionCodec, out var c) ? c : ParquetConnectorConfig.DefaultCompressionCodec;
        if (codec is not (ParquetConnectorConfig.CompressionNone or ParquetConnectorConfig.CompressionGzip or
            ParquetConnectorConfig.CompressionSnappy or ParquetConnectorConfig.CompressionLz4 or
            ParquetConnectorConfig.CompressionZstd or ParquetConnectorConfig.CompressionBrotli))
            throw new ArgumentException($"Invalid compression codec: {codec}");
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task for file writing to avoid conflicts
        return [new Dictionary<string, string>(_config)];
    }
}
