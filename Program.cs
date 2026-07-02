using Azure;
using Azure.Storage.Blobs;

namespace DmsMetricsCollector.Ingestion;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        Console.WriteLine($"[{DateTimeOffset.UtcNow:O}] DMS ingestion run started.");

        try
        {
            var options = IngestionOptions.Parse(args);

            if (options.Kind == IngestionKind.AllLogs)
            {
                var postgresLogOptions = options.WithPipeline(
                    IngestionKind.PostgreSqlServerLogs,
                    options.PostgreSqlLogsContainerName,
                    options.PostgreSqlLogsBlobPrefix,
                    options.GetWatermarkPathFor(IngestionKind.PostgreSqlServerLogs));

                var pgbouncerLogOptions = options.WithPipeline(
                    IngestionKind.PgBouncerLogs,
                    options.PgBouncerLogsContainerName,
                    options.PgBouncerLogsBlobPrefix,
                    options.GetWatermarkPathFor(IngestionKind.PgBouncerLogs));

                var loadedPostgresLogs = await RunSinglePipelineAsync(postgresLogOptions).ConfigureAwait(false);
                var loadedPgBouncerLogs = await RunSinglePipelineAsync(pgbouncerLogOptions).ConfigureAwait(false);

                Console.WriteLine($"[{DateTimeOffset.UtcNow:O}] Ingestion run completed. Rows loaded: {loadedPostgresLogs + loadedPgBouncerLogs}.");
                return 0;
            }

            var loadedRows = await RunSinglePipelineAsync(options).ConfigureAwait(false);
            Console.WriteLine($"[{DateTimeOffset.UtcNow:O}] Ingestion run completed. Rows loaded: {loadedRows}.");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[{DateTimeOffset.UtcNow:O}] Ingestion failed: {ex}");
            return 1;
        }
    }

    private static async Task<int> RunSinglePipelineAsync(IngestionOptions options)
    {
        var watermarkStore = new WatermarkStore(options.WatermarkPath);
        var watermark = await watermarkStore.ReadAsync().ConfigureAwait(false);

        var containerClient = new BlobContainerClient(options.StorageConnectionString, options.ContainerName);

        DestinationTable destination;
        try
        {
            destination = await SqlDestination.ResolveAsync(options.PostgresConnectionString, options.Kind).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[{DateTimeOffset.UtcNow:O}] Failed resolving destination table for {options.Kind}: {ex.Message}");
            return 0;
        }

        await destination.EnsureRequiredColumnsAsync(options.Kind).ConfigureAwait(false);

        List<BlobCursor> blobs;
        try
        {
            blobs = await BlobLister.ListEligibleBlobsAsync(containerClient, options.BlobPrefix, watermark, options.StabilizationLag).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            Console.WriteLine($"[{DateTimeOffset.UtcNow:O}] Container '{options.ContainerName}' not found. Skipping '{options.Kind}'.");
            return 0;
        }

        if (blobs.Count == 0)
        {
            Console.WriteLine($"[{DateTimeOffset.UtcNow:O}] No eligible blobs found in '{options.ContainerName}'.");
            return 0;
        }

        var totalRows = 0;
        var ingestionBatchId = Guid.NewGuid().ToString("N");

        foreach (var blob in blobs)
        {
            if (options.Kind == IngestionKind.Metrics)
            {
                var metricRows = await MetricParser.ReadRowsAsync(containerClient, blob, options.DefaultResourceId).ConfigureAwait(false);
                if (metricRows.Count == 0)
                {
                    await watermarkStore.WriteAsync(new IngestionWatermark(blob.LastModified, blob.Name)).ConfigureAwait(false);
                    continue;
                }

                var insertedMetricRows = await MetricsBulkInserter.InsertAsync(options.PostgresConnectionString, destination.FullName, metricRows).ConfigureAwait(false);
                totalRows += insertedMetricRows;
                await watermarkStore.WriteAsync(new IngestionWatermark(blob.LastModified, blob.Name)).ConfigureAwait(false);

                Console.WriteLine($"[{DateTimeOffset.UtcNow:O}] Loaded {insertedMetricRows}/{metricRows.Count} metric rows from {blob.Name}.");
                continue;
            }

            var logRows = await LogParser.ReadRowsAsync(
                containerClient,
                blob,
                options.ContainerName,
                options.DefaultResourceId,
                ingestionBatchId).ConfigureAwait(false);

            if (logRows.Count == 0)
            {
                await watermarkStore.WriteAsync(new IngestionWatermark(blob.LastModified, blob.Name)).ConfigureAwait(false);
                continue;
            }

            var insertedLogRows = await LogsBulkInserter.InsertAsync(options.PostgresConnectionString, destination.FullName, logRows).ConfigureAwait(false);
            totalRows += insertedLogRows;
            await watermarkStore.WriteAsync(new IngestionWatermark(blob.LastModified, blob.Name)).ConfigureAwait(false);

            Console.WriteLine($"[{DateTimeOffset.UtcNow:O}] Loaded {insertedLogRows}/{logRows.Count} log rows from {blob.Name}.");
        }

        return totalRows;
    }
}