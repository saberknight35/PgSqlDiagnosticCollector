namespace DmsMetricsCollector.Ingestion;

internal sealed record PgBouncerLogRow(
    string ResourceId,
    string ContainerName,
    DateTimeOffset TimeUtc,
    string IngestionBatchId,
    string BlobName,
    DateTimeOffset IngestedAt,
    string RawPayloadJson,
    string? LogCategory,
    string? OperationName,
    string? LogicalServerName,
    string? LogLevel,
    long? ProcessId,
    string? ConnectionRole,
    string? SessionId,
    string? DatabaseName,
    string? UserName,
    string? ClientAddr,
    int? ClientPort,
    string? LogMessage,
    string? ShortLogMessage);
