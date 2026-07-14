using Azure.Storage.Blobs;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace DmsMetricsCollector.Ingestion;

internal static class PgBouncerLogParser
{
    // Matches the body of the `log` field:
    // "YYYY-MM-DD HH:MM:SS.fff UTC [pid] LEVEL C-0xHEX: db/user@host:port message"
    // "YYYY-MM-DD HH:MM:SS.fff UTC [pid] LEVEL message"
    private static readonly Regex LogLineRegex = new(
        @"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ UTC \[(\d+)\] (LOG|WARNING|ERROR|FATAL|DEBUG|INFO|NOTICE)\s+(?:([CS])-0x([0-9a-fA-F]+):\s+(.+)|(.*))$",
        RegexOptions.Compiled);

    // Matches the connection header: "db/user@host:port message"
    // host may be: unix, unix(nnn), or an IP address
    private static readonly Regex ConnectionHeaderRegex = new(
        @"^([^/\s]+)/([^@\s]+)@([^:\s]+?)(?:\(\d+\))?:(\d+)\s+(.+)$",
        RegexOptions.Compiled);

    private static readonly Regex ShortMessageRegex = new(
        @"^([a-zA-Z][^:(]{0,59})(?:[:(]|$)",
        RegexOptions.Compiled);

    public static async Task<List<PgBouncerLogRow>> ReadRowsAsync(
        BlobContainerClient containerClient,
        BlobCursor blob,
        string containerName,
        string? defaultResourceId,
        string ingestionBatchId)
    {
        var content = await BlobReader.DownloadTextAsync(containerClient, blob).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(content))
            return [];

        var trimmed = content.TrimStart();
        if (trimmed.Length > 0 && (trimmed[0] == '{' || trimmed[0] == '['))
            return ParseJson(content, blob, containerName, defaultResourceId, ingestionBatchId);

        return [];
    }

    private static List<PgBouncerLogRow> ParseJson(
        string content,
        BlobCursor blob,
        string containerName,
        string? defaultResourceId,
        string ingestionBatchId)
    {
        var rows = new List<PgBouncerLogRow>();

        try
        {
            using var document = JsonDocument.Parse(content, new JsonDocumentOptions
            {
                AllowTrailingCommas = true,
                CommentHandling = JsonCommentHandling.Skip
            });
            AppendJsonRows(document.RootElement, rows, blob, containerName, defaultResourceId, ingestionBatchId);
            return rows;
        }
        catch (JsonException)
        {
            foreach (var fragment in JsonHelpers.SplitTopLevelJsonValues(content))
            {
                using var document = JsonDocument.Parse(fragment, new JsonDocumentOptions
                {
                    AllowTrailingCommas = true,
                    CommentHandling = JsonCommentHandling.Skip
                });
                AppendJsonRows(document.RootElement, rows, blob, containerName, defaultResourceId, ingestionBatchId);
            }
            return rows;
        }
    }

    private static void AppendJsonRows(
        JsonElement root,
        List<PgBouncerLogRow> rows,
        BlobCursor blob,
        string containerName,
        string? defaultResourceId,
        string ingestionBatchId)
    {
        foreach (var element in JsonHelpers.EnumerateRecords(root))
        {
            if (element.ValueKind == JsonValueKind.Object)
                rows.Add(MapJsonRow(element, blob, containerName, defaultResourceId, ingestionBatchId));
        }
    }

    private static PgBouncerLogRow MapJsonRow(
        JsonElement element,
        BlobCursor blob,
        string containerName,
        string? defaultResourceId,
        string ingestionBatchId)
    {
        var flattened = JsonHelpers.FlattenRawPayloadToDictionary(element);

        // PgBouncer diagnostic logs carry the raw log line in the "log" field
        var logText = JsonHelpers.ReadString(flattened, ["log", "message", "msg"]);

        ParseLogLine(logText,
            out var logLevel, out var processId, out var connectionRole,
            out var sessionId, out var databaseName, out var userName,
            out var clientAddr, out var clientPort, out var logMessage);

        return new PgBouncerLogRow(
            JsonHelpers.ReadString(element, ["resourceId", "resource_id", "resourceUri"]) ??
            JsonHelpers.ReadString(flattened, ["resourceId", "resource_id", "resourceUri"]) ??
            defaultResourceId ?? "unknown-resource",
            containerName,
            JsonHelpers.ReadTimestamp(element, ["time", "timeUtc", "time_utc", "timestamp"]) ??
            JsonHelpers.ReadTimestamp(flattened, ["time", "timeUtc", "time_utc", "timestamp"]) ??
            blob.LastModified,
            ingestionBatchId,
            blob.Name,
            DateTimeOffset.UtcNow,
            JsonSerializer.Serialize(flattened),
            JsonHelpers.ReadString(flattened, ["category", "Category"]),
            JsonHelpers.ReadString(flattened, ["operationName", "operation_name", "OperationName"]),
            JsonHelpers.ReadString(flattened, ["LogicalServerName", "logicalServerName", "logical_server_name"]),
            logLevel,
            processId,
            connectionRole,
            sessionId,
            databaseName,
            userName,
            clientAddr,
            clientPort,
            logMessage,
            ExtractShortLogMessage(logMessage));
    }

    private static void ParseLogLine(
        string? logText,
        out string? logLevel,
        out long? processId,
        out string? connectionRole,
        out string? sessionId,
        out string? databaseName,
        out string? userName,
        out string? clientAddr,
        out int? clientPort,
        out string? logMessage)
    {
        logLevel = null;
        processId = null;
        connectionRole = null;
        sessionId = null;
        databaseName = null;
        userName = null;
        clientAddr = null;
        clientPort = null;
        logMessage = logText;

        if (string.IsNullOrEmpty(logText))
            return;

        var m = LogLineRegex.Match(logText);
        if (!m.Success)
            return;

        if (long.TryParse(m.Groups[1].Value, out var pid))
            processId = pid;
        logLevel = m.Groups[2].Value;

        if (m.Groups[3].Success)
        {
            // Has C-0x.../S-0x... connection prefix
            connectionRole = m.Groups[3].Value;
            sessionId = "0x" + m.Groups[4].Value;

            var rest = m.Groups[5].Value;
            var connMatch = ConnectionHeaderRegex.Match(rest);
            if (connMatch.Success)
            {
                databaseName = connMatch.Groups[1].Value;
                userName = connMatch.Groups[2].Value;
                clientAddr = connMatch.Groups[3].Value;
                if (int.TryParse(connMatch.Groups[4].Value, out var port))
                    clientPort = port;
                logMessage = connMatch.Groups[5].Value;
            }
            else
            {
                logMessage = rest;
            }
        }
        else
        {
            // WARNING/ERROR/LOG without a connection pointer
            logMessage = m.Groups[6].Value;
        }
    }

    private static string? ExtractShortLogMessage(string? logMessage)
    {
        if (string.IsNullOrEmpty(logMessage))
            return null;
        var match = ShortMessageRegex.Match(logMessage);
        return match.Success ? match.Groups[1].Value.TrimEnd() : null;
    }
}
