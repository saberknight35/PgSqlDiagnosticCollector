using System.Globalization;
using System.Text.Json;

namespace DmsMetricsCollector.Ingestion;

internal sealed record IngestionOptions(
    string StorageConnectionString,
    string ContainerName,
    string? BlobPrefix,
    string PostgresConnectionString,
    string WatermarkPath,
    string? DefaultResourceId,
    IngestionKind Kind,
    TimeSpan StabilizationLag,
    string PostgreSqlLogsContainerName,
    string PgBouncerLogsContainerName,
    string? PostgreSqlLogsBlobPrefix,
    string? PgBouncerLogsBlobPrefix,
    DateTimeOffset? FromUtc)
{
    public static IngestionOptions Parse(string[] args)
    {
        var parsedArgs = ParseArgs(args);
        var configFile = LoadConfigFile();

        var kindRaw = GetOptional(parsedArgs, configFile, "kind", "DMS_INGESTION_KIND") ?? "metrics";
        var kind = ParseKind(kindRaw);

        var watermarkPath = GetOptional(parsedArgs, configFile, "watermark", "DMS_INGESTION_WATERMARK_PATH")
            ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "DmsMetricsCollector", "ingestion-watermark.json");

        var stabilizationLag = ParseLag(GetOptional(parsedArgs, configFile, "lag", "DMS_INGESTION_STABILIZATION_LAG_MINUTES"));

        var storageConnectionString =
            GetOptional(parsedArgs, configFile, "storage", "DMS_INGESTION_STORAGE_CONNECTION_STRING")
            ?? BuildStorageConnectionString(
                GetOptional(parsedArgs, configFile, "storageAccountName", "DMS_INGESTION_STORAGE_ACCOUNT_NAME"),
                GetOptional(parsedArgs, configFile, "storageAccountKey", "DMS_INGESTION_STORAGE_ACCOUNT_KEY"))
            ?? throw new InvalidOperationException(
                "Missing storage configuration. Provide Storage.ConnectionString or both Storage.AccountName and Storage.AccountKey in ingestion.config.json.");

        var fromUtc = ParseFromUtc(GetOptional(parsedArgs, configFile, "from", "DMS_INGESTION_FROM_UTC"));

        return new IngestionOptions(
            storageConnectionString,
            GetRequired(parsedArgs, configFile, "container", "DMS_INGESTION_CONTAINER_NAME", "insights-metrics-pt1m"),
            GetOptional(parsedArgs, configFile, "prefix", "DMS_INGESTION_BLOB_PREFIX"),
            GetRequired(parsedArgs, configFile, "postgres", "DMS_INGESTION_POSTGRES_CONNECTION_STRING"),
            watermarkPath,
            GetOptional(parsedArgs, configFile, "resourceId", "DMS_INGESTION_DEFAULT_RESOURCE_ID"),
            kind,
            stabilizationLag,
            GetOptional(parsedArgs, configFile, "postgresLogsContainer", "DMS_INGESTION_POSTGRESQL_LOGS_CONTAINER_NAME") ?? "insights-logs-postgresqllogs",
            GetOptional(parsedArgs, configFile, "pgbouncerLogsContainer", "DMS_INGESTION_PGBOUNCER_LOGS_CONTAINER_NAME") ?? "insights-logs-postgresqlflexpgbouncer",
            GetOptional(parsedArgs, configFile, "postgresLogsPrefix", "DMS_INGESTION_POSTGRESQL_LOGS_BLOB_PREFIX"),
            GetOptional(parsedArgs, configFile, "pgbouncerLogsPrefix", "DMS_INGESTION_PGBOUNCER_LOGS_BLOB_PREFIX"),
            fromUtc);
    }

    public IngestionOptions WithPipeline(IngestionKind kind, string containerName, string? blobPrefix, string watermarkPath)
    {
        return this with
        {
            Kind = kind,
            ContainerName = containerName,
            BlobPrefix = blobPrefix,
            WatermarkPath = watermarkPath
        };
    }

    public string GetWatermarkPathFor(IngestionKind kind)
    {
        var directory = Path.GetDirectoryName(WatermarkPath);
        if (string.IsNullOrWhiteSpace(directory))
            directory = Environment.CurrentDirectory;

        var baseName = Path.GetFileNameWithoutExtension(WatermarkPath);
        if (string.IsNullOrWhiteSpace(baseName))
            baseName = "ingestion-watermark";

        var extension = Path.GetExtension(WatermarkPath);
        if (string.IsNullOrWhiteSpace(extension))
            extension = ".json";

        var suffix = kind switch
        {
            IngestionKind.PostgreSqlServerLogs => "postgresqllogs",
            IngestionKind.PgBouncerLogs => "pgbouncerlogs",
            _ => "metrics"
        };

        return Path.Combine(directory, $"{baseName}-{suffix}{extension}");
    }

    private static IngestionKind ParseKind(string raw)
    {
        return raw.Trim().ToLowerInvariant() switch
        {
            "metrics" => IngestionKind.Metrics,
            "postgresqllogs" => IngestionKind.PostgreSqlServerLogs,
            "pgbouncerlogs" => IngestionKind.PgBouncerLogs,
            "alllogs" => IngestionKind.AllLogs,
            _ => throw new InvalidOperationException("Unsupported ingestion kind. Use metrics, postgresqllogs, pgbouncerlogs, or alllogs.")
        };
    }

    private static TimeSpan ParseLag(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return TimeSpan.FromMinutes(5);

        if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var minutes) && minutes >= 0)
            return TimeSpan.FromMinutes(minutes);

        throw new InvalidOperationException("Invalid stabilization lag. Set DMS_INGESTION_STABILIZATION_LAG_MINUTES to a non-negative integer.");
    }

    private static Dictionary<string, string> LoadConfigFile()
    {
        var candidates = new[]
        {
            Path.Combine(AppContext.BaseDirectory, "ingestion.config.json"),
            Path.Combine(Environment.CurrentDirectory, "ingestion.config.json")
        };

        foreach (var path in candidates)
        {
            if (!File.Exists(path))
                continue;

            using var stream = File.OpenRead(path);
            var doc = JsonDocument.Parse(stream);
            var root = doc.RootElement;
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            SetIfPresent(result, "storage",               root, "Storage", "ConnectionString");
            SetIfPresent(result, "storageAccountName",     root, "Storage", "AccountName");
            SetIfPresent(result, "storageAccountKey",      root, "Storage", "AccountKey");
            SetIfPresent(result, "container",             root, "Storage", "Metrics", "ContainerName");
            SetIfPresent(result, "prefix",                root, "Storage", "Metrics", "BlobPrefix");
            SetIfPresent(result, "postgresLogsContainer", root, "Storage", "PostgreSqlLogs", "ContainerName");
            SetIfPresent(result, "postgresLogsPrefix",    root, "Storage", "PostgreSqlLogs", "BlobPrefix");
            SetIfPresent(result, "pgbouncerLogsContainer",root, "Storage", "PgBouncerLogs", "ContainerName");
            SetIfPresent(result, "pgbouncerLogsPrefix",   root, "Storage", "PgBouncerLogs", "BlobPrefix");
            SetIfPresent(result, "postgres",              root, "Destination", "ConnectionString");
            SetIfPresent(result, "watermark",             root, "Destination", "WatermarkPath");
            SetIfPresent(result, "kind",                  root, "Ingestion", "Kind");
            SetIfPresent(result, "lag",                   root, "Ingestion", "StabilizationLagMinutes");
            SetIfPresent(result, "resourceId",            root, "Ingestion", "DefaultResourceId");
            SetIfPresent(result, "from",                  root, "Ingestion", "FromUtc");

            return result;
        }

        return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    }

    private static string? BuildStorageConnectionString(string? accountName, string? accountKey)
    {
        if (string.IsNullOrWhiteSpace(accountName) || string.IsNullOrWhiteSpace(accountKey))
            return null;

        return $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net";
    }

    private static void SetIfPresent(Dictionary<string, string> result, string key, JsonElement root, params string[] path)
    {
        var current = root;
        foreach (var segment in path)
        {
            if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(segment, out current))
                return;
        }

        var value = current.ValueKind switch
        {
            JsonValueKind.String => current.GetString(),
            JsonValueKind.Number => current.GetRawText(),
            _ => null
        };

        if (!string.IsNullOrWhiteSpace(value))
            result[key] = value!;
    }

    private static Dictionary<string, string> ParseArgs(string[] args)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < args.Length; i++)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal))
                continue;

            var key = args[i][2..];
            var value = i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal)
                ? args[++i]
                : "true";

            result[key] = value;
        }

        return result;
    }

    private static DateTimeOffset? ParseFromUtc(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        var trimmed = raw.Trim();

        if (DateTimeOffset.TryParseExact(trimmed, "yyyy-MM-dd HH", CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var withHour))
            return withHour;

        if (DateTimeOffset.TryParseExact(trimmed, "yyyy-MM-dd", CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dateOnly))
            return dateOnly;

        throw new InvalidOperationException(
            $"Invalid 'from' value '{raw}'. Use format 'yyyy-MM-dd' or 'yyyy-MM-dd HH' (UTC).");
    }

    private static string GetRequired(IReadOnlyDictionary<string, string> args, IReadOnlyDictionary<string, string> config, string argName, string envName, string? defaultValue = null)
    {
        if (args.TryGetValue(argName, out var argValue) && !string.IsNullOrWhiteSpace(argValue))
            return argValue;

        var envValue = Environment.GetEnvironmentVariable(envName);
        if (!string.IsNullOrWhiteSpace(envValue))
            return envValue;

        if (config.TryGetValue(argName, out var cfgValue) && !string.IsNullOrWhiteSpace(cfgValue))
            return cfgValue;

        if (!string.IsNullOrWhiteSpace(defaultValue))
            return defaultValue;

        throw new InvalidOperationException($"Missing required ingestion setting. Provide --{argName}, set {envName}, or fill in ingestion.config.json.");
    }

    private static string? GetOptional(IReadOnlyDictionary<string, string> args, IReadOnlyDictionary<string, string> config, string argName, string envName)
    {
        if (args.TryGetValue(argName, out var argValue) && !string.IsNullOrWhiteSpace(argValue))
            return argValue;

        var envValue = Environment.GetEnvironmentVariable(envName);
        if (!string.IsNullOrWhiteSpace(envValue))
            return envValue;

        if (config.TryGetValue(argName, out var cfgValue) && !string.IsNullOrWhiteSpace(cfgValue))
            return cfgValue;

        return null;
    }
}
