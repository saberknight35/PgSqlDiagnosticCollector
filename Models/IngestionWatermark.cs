namespace DmsMetricsCollector.Ingestion;

internal sealed record IngestionWatermark(DateTimeOffset TimestampUtc, string BlobName)
{
    public static IngestionWatermark Empty { get; } = new(DateTimeOffset.MinValue, string.Empty);
}
