using System.Globalization;
using System.Text.Json;

namespace DmsMetricsCollector.Ingestion;

internal sealed class WatermarkStore
{
    private readonly string _path;

    public WatermarkStore(string path)
    {
        _path = path;
    }

    public async Task<IngestionWatermark> ReadAsync()
    {
        if (!File.Exists(_path))
            return IngestionWatermark.Empty;

        var json = await File.ReadAllTextAsync(_path).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(json))
            return IngestionWatermark.Empty;

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        var timestamp = root.TryGetProperty("timestampUtc", out var timestampElement) &&
                        timestampElement.ValueKind == JsonValueKind.String &&
                        DateTimeOffset.TryParse(timestampElement.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsedTimestamp)
            ? parsedTimestamp
            : DateTimeOffset.MinValue;

        var blobName = root.TryGetProperty("blobName", out var blobNameElement) && blobNameElement.ValueKind == JsonValueKind.String
            ? blobNameElement.GetString() ?? string.Empty
            : string.Empty;

        return new IngestionWatermark(timestamp, blobName);
    }

    public async Task WriteAsync(IngestionWatermark watermark)
    {
        var directory = Path.GetDirectoryName(_path);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        var json = JsonSerializer.Serialize(new
        {
            timestampUtc = watermark.TimestampUtc,
            blobName = watermark.BlobName
        }, new JsonSerializerOptions { WriteIndented = true });

        var tempPath = _path + ".tmp";
        await File.WriteAllTextAsync(tempPath, json).ConfigureAwait(false);
        File.Copy(tempPath, _path, true);
        File.Delete(tempPath);
    }
}
