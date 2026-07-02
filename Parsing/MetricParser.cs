using Azure.Storage.Blobs;
using Microsoft.VisualBasic.FileIO;
using System.Text.Json;

namespace DmsMetricsCollector.Ingestion;

internal static class MetricParser
{
    public static async Task<List<MetricRow>> ReadRowsAsync(
        BlobContainerClient containerClient,
        BlobCursor blob,
        string? defaultResourceId)
    {
        var content = await BlobReader.DownloadTextAsync(containerClient, blob).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(content))
            return [];

        var trimmed = content.TrimStart();
        if (trimmed.Length > 0 && (trimmed[0] == '{' || trimmed[0] == '['))
            return ParseJson(content, blob, defaultResourceId);

        return ParseDelimited(content, blob, defaultResourceId);
    }

    private static List<MetricRow> ParseJson(string content, BlobCursor blob, string? defaultResourceId)
    {
        var rows = new List<MetricRow>();

        try
        {
            using var document = JsonDocument.Parse(content, new JsonDocumentOptions
            {
                AllowTrailingCommas = true,
                CommentHandling = JsonCommentHandling.Skip
            });

            AppendJsonRows(document.RootElement, rows, blob, defaultResourceId);
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

                AppendJsonRows(document.RootElement, rows, blob, defaultResourceId);
            }

            return rows;
        }
    }

    private static List<MetricRow> ParseDelimited(string content, BlobCursor blob, string? defaultResourceId)
    {
        using var parser = new TextFieldParser(new StringReader(content));
        parser.TextFieldType = FieldType.Delimited;
        parser.HasFieldsEnclosedInQuotes = true;
        parser.SetDelimiters(content.Contains('\t') && !content.Contains(',') ? "\t" : ",");

        var headers = parser.ReadFields();
        if (headers is null || headers.Length == 0)
            return [];

        var rows = new List<MetricRow>();
        while (!parser.EndOfData)
        {
            var fields = parser.ReadFields();
            if (fields is null || fields.Length == 0)
                continue;

            var values = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
            var count = Math.Min(headers.Length, fields.Length);
            for (var i = 0; i < count; i++)
                values[headers[i]] = string.IsNullOrWhiteSpace(fields[i]) ? null : fields[i];

            rows.Add(MapDelimitedRow(values, blob, defaultResourceId));
        }

        return rows;
    }

    private static void AppendJsonRows(JsonElement root, List<MetricRow> rows, BlobCursor blob, string? defaultResourceId)
    {
        foreach (var element in JsonHelpers.EnumerateRecords(root))
        {
            if (element.ValueKind == JsonValueKind.Object)
                rows.Add(MapJsonRow(element, blob, defaultResourceId));
        }
    }

    private static MetricRow MapJsonRow(JsonElement element, BlobCursor blob, string? defaultResourceId)
    {
        return new MetricRow(
            JsonHelpers.ReadString(element, ["resourceId", "resource_id", "resourceUri", "resource_uri", "resource"]) ?? defaultResourceId ?? "unknown-resource",
            JsonHelpers.ReadString(element, ["metricId", "metric_id", "metricName", "metric_name", "metric", "name"]) ?? "unknown-metric",
            JsonHelpers.ReadDimensionKey(element),
            JsonHelpers.ReadTimestamp(element, ["timeUtc", "time_utc", "timestamp", "time", "collectedAt", "collected_at", "startTime", "start_time"]) ?? blob.LastModified,
            JsonHelpers.ReadString(element, ["value", "metricValue", "metric_value", "average", "avg", "total", "sum", "count", "minimum", "min", "maximum", "max"]),
            JsonHelpers.ReadString(element, ["unit"]),
            JsonHelpers.ReadDouble(element, ["average", "avg"]),
            JsonHelpers.ReadDouble(element, ["minimum", "min"]),
            JsonHelpers.ReadDouble(element, ["maximum", "max"]),
            JsonHelpers.ReadDouble(element, ["total", "sum"]),
            JsonHelpers.ReadDouble(element, ["count"]),
            blob.Name,
            DateTimeOffset.UtcNow,
            JsonHelpers.FlattenRawPayload(element));
    }

    private static MetricRow MapDelimitedRow(IReadOnlyDictionary<string, string?> values, BlobCursor blob, string? defaultResourceId)
    {
        return new MetricRow(
            JsonHelpers.ReadString(values, ["resourceId", "resource_id", "resourceUri", "resource_uri", "resource"]) ?? defaultResourceId ?? "unknown-resource",
            JsonHelpers.ReadString(values, ["metricId", "metric_id", "metricName", "metric_name", "metric", "name"]) ?? "unknown-metric",
            JsonHelpers.ReadDimensionKey(values),
            JsonHelpers.ReadTimestamp(values, ["timeUtc", "time_utc", "timestamp", "time", "collectedAt", "collected_at", "startTime", "start_time"]) ?? blob.LastModified,
            JsonHelpers.ReadString(values, ["value", "metricValue", "metric_value", "average", "avg", "total", "sum", "count", "minimum", "min", "maximum", "max"]),
            JsonHelpers.ReadString(values, ["unit"]),
            JsonHelpers.ReadDouble(values, ["average", "avg"]),
            JsonHelpers.ReadDouble(values, ["minimum", "min"]),
            JsonHelpers.ReadDouble(values, ["maximum", "max"]),
            JsonHelpers.ReadDouble(values, ["total", "sum"]),
            JsonHelpers.ReadDouble(values, ["count"]),
            blob.Name,
            DateTimeOffset.UtcNow,
            JsonSerializer.Serialize(values));
    }
}
