using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace DmsMetricsCollector.Ingestion;

internal static class BlobLister
{
    public static async Task<List<BlobCursor>> ListEligibleBlobsAsync(
        BlobContainerClient containerClient,
        string? prefix,
        IngestionWatermark watermark,
        TimeSpan stabilizationLag)
    {
        var blobs = new List<BlobCursor>();
        var latestAllowedTimestamp = DateTimeOffset.UtcNow - stabilizationLag;

        await foreach (var blob in containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, prefix))
        {
            var lastModified = blob.Properties.LastModified ?? DateTimeOffset.MinValue;

            if (stabilizationLag > TimeSpan.Zero && lastModified > latestAllowedTimestamp)
                continue;

            if (lastModified < watermark.TimestampUtc)
                continue;

            if (lastModified == watermark.TimestampUtc && string.CompareOrdinal(blob.Name, watermark.BlobName) <= 0)
                continue;

            blobs.Add(new BlobCursor(blob.Name, lastModified));
        }

        blobs.Sort(static (left, right) =>
        {
            var compare = left.LastModified.CompareTo(right.LastModified);
            return compare != 0 ? compare : string.CompareOrdinal(left.Name, right.Name);
        });

        return blobs;
    }
}
