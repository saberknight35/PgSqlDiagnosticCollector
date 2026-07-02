using Azure.Storage.Blobs;

namespace DmsMetricsCollector.Ingestion;

internal static class BlobReader
{
    public static async Task<string> DownloadTextAsync(BlobContainerClient containerClient, BlobCursor blob)
    {
        var blobClient = containerClient.GetBlobClient(blob.Name);
        var download = await blobClient.DownloadStreamingAsync().ConfigureAwait(false);

        await using var contentStream = download.Value.Content;
        if (contentStream is null)
            return string.Empty;

        using var reader = new StreamReader(contentStream, detectEncodingFromByteOrderMarks: true);
        return await reader.ReadToEndAsync().ConfigureAwait(false);
    }
}
