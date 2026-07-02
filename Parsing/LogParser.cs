using Azure.Storage.Blobs;
using Microsoft.VisualBasic.FileIO;
using System.Text.Json;

namespace DmsMetricsCollector.Ingestion;

internal static class LogParser
{
    public static async Task<List<LogRawRow>> ReadRowsAsync(
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

        return ParseDelimited(content, blob, containerName, defaultResourceId, ingestionBatchId);
    }

    private static List<LogRawRow> ParseJson(
        string content,
        BlobCursor blob,
        string containerName,
        string? defaultResourceId,
        string ingestionBatchId)
    {
        var rows = new List<LogRawRow>();

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

    private static List<LogRawRow> ParseDelimited(
        string content,
        BlobCursor blob,
        string containerName,
        string? defaultResourceId,
        string ingestionBatchId)
    {
        using var parser = new TextFieldParser(new StringReader(content));
        parser.TextFieldType = FieldType.Delimited;
        parser.HasFieldsEnclosedInQuotes = true;
        parser.SetDelimiters(content.Contains('\t') && !content.Contains(',') ? "\t" : ",");

        var headers = parser.ReadFields();
        if (headers is null || headers.Length == 0)
            return [];

        var rows = new List<LogRawRow>();
        while (!parser.EndOfData)
        {
            var fields = parser.ReadFields();
            if (fields is null || fields.Length == 0)
                continue;

            var values = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
            var count = Math.Min(headers.Length, fields.Length);
            for (var i = 0; i < count; i++)
                values[headers[i]] = string.IsNullOrWhiteSpace(fields[i]) ? null : fields[i];

            rows.Add(MapDelimitedRow(values, blob, containerName, defaultResourceId, ingestionBatchId));
        }

        return rows;
    }

    private static void AppendJsonRows(
        JsonElement root,
        List<LogRawRow> rows,
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

    private static LogRawRow MapJsonRow(
        JsonElement element,
        BlobCursor blob,
        string containerName,
        string? defaultResourceId,
        string ingestionBatchId)
    {
        var flattened = JsonHelpers.FlattenRawPayloadToDictionary(element);

        return new LogRawRow(
            JsonHelpers.ReadString(element, ["resourceId", "resource_id", "resourceUri", "resource_uri", "resource"]) ??
            JsonHelpers.ReadString(flattened, ["resourceId", "resource_id", "resourceUri", "resource_uri", "resource"]) ??
            defaultResourceId ??
            "unknown-resource",
            containerName,
            JsonHelpers.ReadTimestamp(element, ["timeUtc", "time_utc", "timestamp", "time", "eventTime", "event_time", "collectedAt", "collected_at"]) ??
            JsonHelpers.ReadTimestamp(flattened, ["timeUtc", "time_utc", "timestamp", "time", "eventTime", "event_time", "collectedAt", "collected_at"]) ??
            blob.LastModified,
            ingestionBatchId,
            blob.Name,
            DateTimeOffset.UtcNow,
            JsonSerializer.Serialize(flattened),
            JsonHelpers.ReadString(flattened, ["category", "Category", "properties.category", "properties.Category"]),
            JsonHelpers.ReadString(flattened, ["operationName", "operation_name", "operation", "OperationName", "properties.operationName", "properties.OperationName"]),
            JsonHelpers.ReadString(flattened, ["level", "severity", "logLevel", "log_level", "properties.level", "properties.severity", "properties.logLevel", "properties.log_level"]),
            JsonHelpers.ReadString(flattened, ["errorSeverity", "error_severity", "severityText", "properties.errorSeverity", "properties.error_severity", "properties.severityText"]),
            JsonHelpers.ReadString(flattened, ["sqlState", "sql_state", "sqlstate", "sql_state_code", "properties.sqlState", "properties.sql_state", "properties.sqlstate", "properties.sql_state_code"]),
            JsonHelpers.ReadLong(flattened, ["processId", "process_id", "pid", "properties.processId", "properties.process_id", "properties.pid"]),
            JsonHelpers.ReadString(flattened, ["sessionId", "session_id", "properties.sessionId", "properties.session_id"]),
            JsonHelpers.ReadString(flattened, ["database", "database_name", "db", "dbname", "properties.database", "properties.database_name", "properties.db", "properties.dbname"]),
            JsonHelpers.ReadString(flattened, ["user", "user_name", "username", "properties.user", "properties.user_name", "properties.username"]),
            JsonHelpers.ReadString(flattened, ["applicationName", "application_name", "app", "properties.applicationName", "properties.application_name", "properties.app"]),
            JsonHelpers.ReadString(flattened, ["clientAddr", "client_addr", "clientIp", "client_ip", "remoteAddr", "remote_addr", "properties.clientAddr", "properties.client_addr", "properties.clientIp", "properties.client_ip", "properties.remoteAddr", "properties.remote_addr"]),
            JsonHelpers.ReadInt(flattened, ["clientPort", "client_port", "properties.clientPort", "properties.client_port"]),
            JsonHelpers.ReadString(flattened, ["message", "msg", "logMessage", "log_message", "properties.message", "properties.msg", "properties.logMessage", "properties.log_message"]));
    }

    private static LogRawRow MapDelimitedRow(
        IReadOnlyDictionary<string, string?> values,
        BlobCursor blob,
        string containerName,
        string? defaultResourceId,
        string ingestionBatchId)
    {
        return new LogRawRow(
            JsonHelpers.ReadString(values, ["resourceId", "resource_id", "resourceUri", "resource_uri", "resource"]) ?? defaultResourceId ?? "unknown-resource",
            containerName,
            JsonHelpers.ReadTimestamp(values, ["timeUtc", "time_utc", "timestamp", "time", "eventTime", "event_time", "collectedAt", "collected_at"]) ?? blob.LastModified,
            ingestionBatchId,
            blob.Name,
            DateTimeOffset.UtcNow,
            JsonSerializer.Serialize(values),
            JsonHelpers.ReadString(values, ["category", "Category", "properties.category", "properties.Category"]),
            JsonHelpers.ReadString(values, ["operationName", "operation_name", "operation", "OperationName", "properties.operationName", "properties.OperationName"]),
            JsonHelpers.ReadString(values, ["level", "severity", "logLevel", "log_level", "properties.level", "properties.severity", "properties.logLevel", "properties.log_level"]),
            JsonHelpers.ReadString(values, ["errorSeverity", "error_severity", "severityText", "properties.errorSeverity", "properties.error_severity", "properties.severityText"]),
            JsonHelpers.ReadString(values, ["sqlState", "sql_state", "sqlstate", "sql_state_code", "properties.sqlState", "properties.sql_state", "properties.sqlstate", "properties.sql_state_code"]),
            JsonHelpers.ReadLong(values, ["processId", "process_id", "pid", "properties.processId", "properties.process_id", "properties.pid"]),
            JsonHelpers.ReadString(values, ["sessionId", "session_id", "properties.sessionId", "properties.session_id"]),
            JsonHelpers.ReadString(values, ["database", "database_name", "db", "dbname", "properties.database", "properties.database_name", "properties.db", "properties.dbname"]),
            JsonHelpers.ReadString(values, ["user", "user_name", "username", "properties.user", "properties.user_name", "properties.username"]),
            JsonHelpers.ReadString(values, ["applicationName", "application_name", "app", "properties.applicationName", "properties.application_name", "properties.app"]),
            JsonHelpers.ReadString(values, ["clientAddr", "client_addr", "clientIp", "client_ip", "remoteAddr", "remote_addr", "properties.clientAddr", "properties.client_addr", "properties.clientIp", "properties.client_ip", "properties.remoteAddr", "properties.remote_addr"]),
            JsonHelpers.ReadInt(values, ["clientPort", "client_port", "properties.clientPort", "properties.client_port"]),
            JsonHelpers.ReadString(values, ["message", "msg", "logMessage", "log_message", "properties.message", "properties.msg", "properties.logMessage", "properties.log_message"]));
    }
}
