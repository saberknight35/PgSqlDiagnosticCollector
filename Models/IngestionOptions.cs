using System.Globalization;

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
    string? PgBouncerLogsBlobPrefix)
{
    public static IngestionOptions Parse(string[] args)
    {
        var parsedArgs = ParseArgs(args);

        var kindRaw = GetOptional(parsedArgs, "kind", "DMS_INGESTION_KIND") ?? "metrics";
        var kind = ParseKind(kindRaw);

        var watermarkPath = GetOptional(parsedArgs, "watermark", "DMS_INGESTION_WATERMARK_PATH")
            ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "DmsMetricsCollector", "ingestion-watermark.json");

        var stabilizationLag = ParseLag(GetOptional(parsedArgs, "lag", "DMS_INGESTION_STABILIZATION_LAG_MINUTES"));

        return new IngestionOptions(
            GetRequired(parsedArgs, "storage", "DMS_INGESTION_STORAGE_CONNECTION_STRING"),
            GetRequired(parsedArgs, "container", "DMS_INGESTION_CONTAINER_NAME", "insights-metrics-pt1m"),
            GetOptional(parsedArgs, "prefix", "DMS_INGESTION_BLOB_PREFIX"),
            GetRequired(parsedArgs, "postgres", "DMS_INGESTION_POSTGRES_CONNECTION_STRING"),
            watermarkPath,
            GetOptional(parsedArgs, "resourceId", "DMS_INGESTION_DEFAULT_RESOURCE_ID"),
            kind,
            stabilizationLag,
            GetOptional(parsedArgs, "postgresLogsContainer", "DMS_INGESTION_POSTGRESQL_LOGS_CONTAINER_NAME") ?? "insights-logs-postgresqllogs",
            GetOptional(parsedArgs, "pgbouncerLogsContainer", "DMS_INGESTION_PGBOUNCER_LOGS_CONTAINER_NAME") ?? "insights-logs-postgresqlflexpgbouncer",
            GetOptional(parsedArgs, "postgresLogsPrefix", "DMS_INGESTION_POSTGRESQL_LOGS_BLOB_PREFIX"),
            GetOptional(parsedArgs, "pgbouncerLogsPrefix", "DMS_INGESTION_PGBOUNCER_LOGS_BLOB_PREFIX"));
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

    private static string GetRequired(IReadOnlyDictionary<string, string> args, string argName, string envName, string? defaultValue = null)
    {
        if (args.TryGetValue(argName, out var argValue) && !string.IsNullOrWhiteSpace(argValue))
            return argValue;

        var envValue = Environment.GetEnvironmentVariable(envName);
        if (!string.IsNullOrWhiteSpace(envValue))
            return envValue;

        if (!string.IsNullOrWhiteSpace(defaultValue))
            return defaultValue;

        throw new InvalidOperationException($"Missing required ingestion setting. Provide --{argName} or set {envName}.");
    }

    private static string? GetOptional(IReadOnlyDictionary<string, string> args, string argName, string envName)
    {
        if (args.TryGetValue(argName, out var argValue) && !string.IsNullOrWhiteSpace(argValue))
            return argValue;

        var envValue = Environment.GetEnvironmentVariable(envName);
        return string.IsNullOrWhiteSpace(envValue) ? null : envValue;
    }
}
