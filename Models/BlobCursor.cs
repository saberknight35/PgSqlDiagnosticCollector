namespace DmsMetricsCollector.Ingestion;

internal sealed record BlobCursor(string Name, DateTimeOffset LastModified);
