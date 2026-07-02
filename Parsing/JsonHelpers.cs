using System.Globalization;
using System.Text.Json;

namespace DmsMetricsCollector.Ingestion;

internal static class JsonHelpers
{
    public static IEnumerable<JsonElement> EnumerateRecords(JsonElement root)
    {
        if (root.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in root.EnumerateArray())
                yield return item;

            yield break;
        }

        if (root.ValueKind != JsonValueKind.Object)
            yield break;

        foreach (var propertyName in new[] { "records", "rows", "values", "data", "metrics", "timeseries", "value" })
        {
            if (!TryGetProperty(root, propertyName, out var child) || child.ValueKind != JsonValueKind.Array)
                continue;

            foreach (var item in child.EnumerateArray())
                yield return item;

            yield break;
        }

        yield return root;
    }

    public static IEnumerable<string> SplitTopLevelJsonValues(string content)
    {
        var start = -1;
        var depth = 0;
        var inString = false;
        var isEscaped = false;

        for (var i = 0; i < content.Length; i++)
        {
            var ch = content[i];

            if (inString)
            {
                if (isEscaped)
                {
                    isEscaped = false;
                    continue;
                }

                if (ch == '\\')
                {
                    isEscaped = true;
                    continue;
                }

                if (ch == '"')
                    inString = false;

                continue;
            }

            if (ch == '"')
            {
                inString = true;
                continue;
            }

            if (start < 0)
            {
                if (ch == '{' || ch == '[')
                {
                    start = i;
                    depth = 1;
                }

                continue;
            }

            if (ch == '{' || ch == '[')
            {
                depth++;
                continue;
            }

            if (ch == '}' || ch == ']')
            {
                depth--;
                if (depth == 0)
                {
                    yield return content.Substring(start, i - start + 1);
                    start = -1;
                }
            }
        }
    }

    public static string? ReadDimensionKey(JsonElement element)
    {
        if (TryGetProperty(element, "dimension_key", out var direct) && direct.ValueKind == JsonValueKind.String)
            return direct.GetString();

        if (TryGetProperty(element, "dimensions", out var dimensions) && dimensions.ValueKind == JsonValueKind.Object)
        {
            var parts = dimensions.EnumerateObject()
                .OrderBy(property => property.Name, StringComparer.OrdinalIgnoreCase)
                .Select(property => $"{property.Name}={ScalarToString(property.Value)}");
            return string.Join(';', parts);
        }

        return null;
    }

    public static string? ReadDimensionKey(IReadOnlyDictionary<string, string?> values)
    {
        if (values.TryGetValue("dimension_key", out var direct) && !string.IsNullOrWhiteSpace(direct))
            return direct;

        if (values.TryGetValue("dimensions", out var dimensionJson) && !string.IsNullOrWhiteSpace(dimensionJson))
            return dimensionJson;

        return null;
    }

    public static string? ReadString(JsonElement element, IReadOnlyList<string> candidateNames)
    {
        if (!TryReadProperty(element, candidateNames, out var child))
            return null;

        return ScalarToString(child);
    }

    public static string? ReadString(IReadOnlyDictionary<string, string?> values, IReadOnlyList<string> candidateNames)
    {
        foreach (var candidateName in candidateNames)
        {
            foreach (var pair in values)
            {
                if (string.Equals(pair.Key, candidateName, StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(pair.Value))
                    return pair.Value;
            }
        }

        return null;
    }

    public static DateTimeOffset? ReadTimestamp(JsonElement element, IReadOnlyList<string> candidateNames)
    {
        if (!TryReadProperty(element, candidateNames, out var child))
            return null;

        if (child.ValueKind == JsonValueKind.String && DateTimeOffset.TryParse(child.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
            return parsed;

        if (child.ValueKind == JsonValueKind.Number && child.TryGetInt64(out var unixSeconds))
            return DateTimeOffset.FromUnixTimeSeconds(unixSeconds);

        return null;
    }

    public static DateTimeOffset? ReadTimestamp(IReadOnlyDictionary<string, string?> values, IReadOnlyList<string> candidateNames)
    {
        var raw = ReadString(values, candidateNames);
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        if (DateTimeOffset.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
            return parsed;

        if (long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var unixSeconds))
            return DateTimeOffset.FromUnixTimeSeconds(unixSeconds);

        return null;
    }

    public static double? ReadDouble(JsonElement element, IReadOnlyList<string> candidateNames)
    {
        if (!TryReadProperty(element, candidateNames, out var child))
            return null;

        if (child.ValueKind == JsonValueKind.Number && child.TryGetDouble(out var numericValue))
            return numericValue;

        if (child.ValueKind == JsonValueKind.String &&
            double.TryParse(child.GetString(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsedStringValue))
            return parsedStringValue;

        return null;
    }

    public static double? ReadDouble(IReadOnlyDictionary<string, string?> values, IReadOnlyList<string> candidateNames)
    {
        var raw = ReadString(values, candidateNames);
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        return double.TryParse(raw, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
    }

    public static long? ReadLong(IReadOnlyDictionary<string, string?> values, IReadOnlyList<string> candidateNames)
    {
        var raw = ReadString(values, candidateNames);
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        return long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
    }

    public static int? ReadInt(IReadOnlyDictionary<string, string?> values, IReadOnlyList<string> candidateNames)
    {
        var raw = ReadString(values, candidateNames);
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
    }

    public static string ScalarToString(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString() ?? string.Empty,
            JsonValueKind.Number => element.GetRawText(),
            JsonValueKind.True => bool.TrueString,
            JsonValueKind.False => bool.FalseString,
            _ => element.GetRawText()
        };
    }

    public static string FlattenRawPayload(JsonElement element)
    {
        return JsonSerializer.Serialize(FlattenRawPayloadToDictionary(element));
    }

    public static Dictionary<string, string?> FlattenRawPayloadToDictionary(JsonElement element)
    {
        var flattened = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        FlattenElement(element, string.Empty, flattened);
        return flattened;
    }

    private static void FlattenElement(JsonElement element, string path, IDictionary<string, string?> destination)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    var childPath = string.IsNullOrEmpty(path) ? property.Name : $"{path}.{property.Name}";
                    FlattenElement(property.Value, childPath, destination);
                }
                break;

            case JsonValueKind.Array:
                var index = 0;
                foreach (var item in element.EnumerateArray())
                {
                    var childPath = $"{path}[{index}]";
                    FlattenElement(item, childPath, destination);
                    index++;
                }
                break;

            case JsonValueKind.Null:
            case JsonValueKind.Undefined:
                destination[path] = null;
                break;

            default:
                destination[path] = ScalarToString(element);
                break;
        }
    }

    private static bool TryReadProperty(JsonElement element, IReadOnlyList<string> candidateNames, out JsonElement child)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            child = default;
            return false;
        }

        foreach (var property in element.EnumerateObject())
        {
            foreach (var candidateName in candidateNames)
            {
                if (string.Equals(property.Name, candidateName, StringComparison.OrdinalIgnoreCase))
                {
                    child = property.Value;
                    return true;
                }
            }
        }

        child = default;
        return false;
    }

    private static bool TryGetProperty(JsonElement element, string candidateName, out JsonElement child)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            child = default;
            return false;
        }

        foreach (var property in element.EnumerateObject())
        {
            if (string.Equals(property.Name, candidateName, StringComparison.OrdinalIgnoreCase))
            {
                child = property.Value;
                return true;
            }
        }

        child = default;
        return false;
    }
}
