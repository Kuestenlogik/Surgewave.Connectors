namespace Kuestenlogik.Surgewave.Connector.Script;

/// <summary>
/// Configuration constants for Script connectors.
/// </summary>
public static class ScriptConnectorConfig
{
    // Topics
    public const string Topics = "topics";
    public const string OutputTopic = "output.topic";

    // Script Configuration
    public const string ScriptPath = "script.path";
    public const string ScriptInline = "script.inline";
    public const string ScriptLanguage = "script.language";
    public const string DefaultScriptLanguage = "csharp";

    // Execution
    public const string TimeoutMs = "timeout.ms";
    public const int DefaultTimeoutMs = 30000;
    public const string ErrorHandling = "error.handling";
    public const string DefaultErrorHandling = "skip"; // skip, fail, deadletter
    public const string DeadLetterTopic = "dead.letter.topic";

    // Batching
    public const string BatchSize = "batch.size";
    public const int DefaultBatchSize = 1;
    public const string ProcessMode = "process.mode";
    public const string DefaultProcessMode = "record"; // record, batch

    // Imports
    public const string Imports = "imports";
    public const string DefaultImports = "System;System.Linq;System.Collections.Generic;System.Text;System.Text.Json;Kuestenlogik.Surgewave.Connector.Script";
    public const string References = "references";
}
