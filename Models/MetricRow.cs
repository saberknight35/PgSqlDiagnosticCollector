namespace DmsMetricsCollector.Ingestion;

internal sealed record MetricRow(
    string ResourceId,
    string MetricId,
    string? DimensionKey,
    DateTimeOffset TimeUtc,
    string? MetricValue,
    string? Unit,
    double? MetricAverage,
    double? MetricMinimum,
    double? MetricMaximum,
    double? MetricTotal,
    double? MetricCount,
    string BlobName,
    DateTimeOffset IngestedAt,
    string RawPayload);
