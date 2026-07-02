using Npgsql;
using NpgsqlTypes;

namespace DmsMetricsCollector.Ingestion;

internal static class MetricsBulkInserter
{
    public static async Task<int> InsertAsync(string postgresConnectionString, string destinationTable, IReadOnlyList<MetricRow> rows)
    {
        if (rows.Count == 0)
            return 0;

        await using var connection = new NpgsqlConnection(postgresConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        const string createTempSql = """
            CREATE TEMP TABLE tmp_ingest_metrics (
                resource_id TEXT NOT NULL,
                metric_id TEXT NOT NULL,
                dimension_key TEXT NULL,
                time_utc TIMESTAMPTZ NOT NULL,
                metric_value TEXT NULL,
                unit TEXT NULL,
                metric_avg DOUBLE PRECISION NULL,
                metric_min DOUBLE PRECISION NULL,
                metric_max DOUBLE PRECISION NULL,
                metric_total DOUBLE PRECISION NULL,
                metric_count DOUBLE PRECISION NULL,
                blob_name TEXT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                raw_payload TEXT NULL
            ) ON COMMIT DROP;
            """;

        await using (var createTempCmd = new NpgsqlCommand(createTempSql, connection))
        {
            await createTempCmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        using (var importer = connection.BeginBinaryImport("COPY tmp_ingest_metrics (resource_id, metric_id, dimension_key, time_utc, metric_value, unit, metric_avg, metric_min, metric_max, metric_total, metric_count, blob_name, ingested_at, raw_payload) FROM STDIN (FORMAT BINARY)"))
        {
            foreach (var row in rows)
            {
                importer.StartRow();
                importer.Write(row.ResourceId, NpgsqlDbType.Text);
                importer.Write(row.MetricId, NpgsqlDbType.Text);
                importer.Write(row.DimensionKey, NpgsqlDbType.Text);
                importer.Write(row.TimeUtc.UtcDateTime, NpgsqlDbType.TimestampTz);
                importer.Write(row.MetricValue, NpgsqlDbType.Text);
                importer.Write(row.Unit, NpgsqlDbType.Text);
                importer.Write(row.MetricAverage, NpgsqlDbType.Double);
                importer.Write(row.MetricMinimum, NpgsqlDbType.Double);
                importer.Write(row.MetricMaximum, NpgsqlDbType.Double);
                importer.Write(row.MetricTotal, NpgsqlDbType.Double);
                importer.Write(row.MetricCount, NpgsqlDbType.Double);
                importer.Write(row.BlobName, NpgsqlDbType.Text);
                importer.Write(row.IngestedAt.UtcDateTime, NpgsqlDbType.TimestampTz);
                importer.Write(row.RawPayload, NpgsqlDbType.Text);
            }

            importer.Complete();
        }

        var insertSql = $"""
            WITH candidates AS (
                SELECT DISTINCT ON (
                    s.resource_id,
                    s.metric_id,
                    COALESCE(s.dimension_key, ''),
                    s.time_utc
                )
                    s.resource_id,
                    s.metric_id,
                    s.dimension_key,
                    s.time_utc,
                    s.metric_value,
                    s.unit,
                    s.metric_avg,
                    s.metric_min,
                    s.metric_max,
                    s.metric_total,
                    s.metric_count,
                    s.blob_name,
                    s.ingested_at,
                    s.raw_payload
                FROM tmp_ingest_metrics s
                ORDER BY
                    s.resource_id,
                    s.metric_id,
                    COALESCE(s.dimension_key, ''),
                    s.time_utc,
                    s.ingested_at
            ),
            inserted AS (
                INSERT INTO {destinationTable} (
                    resource_id,
                    metric_id,
                    dimension_key,
                    time_utc,
                    metric_value,
                    unit,
                    metric_avg,
                    metric_min,
                    metric_max,
                    metric_total,
                    metric_count,
                    blob_name,
                    ingested_at,
                    raw_payload
                )
                SELECT
                    c.resource_id,
                    c.metric_id,
                    c.dimension_key,
                    c.time_utc,
                    c.metric_value,
                    c.unit,
                    c.metric_avg,
                    c.metric_min,
                    c.metric_max,
                    c.metric_total,
                    c.metric_count,
                    c.blob_name,
                    c.ingested_at,
                    c.raw_payload
                FROM candidates c
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {destinationTable} t
                    WHERE t.resource_id = c.resource_id
                      AND t.metric_id = c.metric_id
                      AND COALESCE(t.dimension_key, '') = COALESCE(c.dimension_key, '')
                      AND t.time_utc = c.time_utc
                )
                RETURNING 1
            )
            SELECT COUNT(*)::INT FROM inserted;
            """;

        await using var insertCmd = new NpgsqlCommand(insertSql, connection);
        var inserted = await insertCmd.ExecuteScalarAsync().ConfigureAwait(false);
        return inserted is int count ? count : 0;
    }
}
