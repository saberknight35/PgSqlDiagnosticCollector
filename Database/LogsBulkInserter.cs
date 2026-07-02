using Npgsql;
using NpgsqlTypes;

namespace DmsMetricsCollector.Ingestion;

internal static class LogsBulkInserter
{
    public static async Task<int> InsertAsync(string postgresConnectionString, string destinationTable, IReadOnlyList<LogRawRow> rows)
    {
        if (rows.Count == 0)
            return 0;

        await using var connection = new NpgsqlConnection(postgresConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        const string createTempSql = """
            CREATE TEMP TABLE tmp_ingest_logs (
                resource_id TEXT NOT NULL,
                container_name TEXT NOT NULL,
                time_utc TIMESTAMPTZ NOT NULL,
                ingestion_batch_id TEXT NOT NULL,
                blob_name TEXT NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                raw_payload_json TEXT NULL,
                log_category TEXT NULL,
                operation_name TEXT NULL,
                logical_server_name TEXT NULL,
                log_level TEXT NULL,
                error_severity TEXT NULL,
                sql_state TEXT NULL,
                process_id BIGINT NULL,
                session_id TEXT NULL,
                database_name TEXT NULL,
                user_name TEXT NULL,
                application_name TEXT NULL,
                client_addr TEXT NULL,
                client_port INTEGER NULL,
                log_message TEXT NULL,
                short_log_message TEXT NULL
            ) ON COMMIT DROP;
            """;

        await using (var createTempCmd = new NpgsqlCommand(createTempSql, connection))
        {
            await createTempCmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        using (var importer = connection.BeginBinaryImport("COPY tmp_ingest_logs (resource_id, container_name, time_utc, ingestion_batch_id, blob_name, ingested_at, raw_payload_json, log_category, operation_name, logical_server_name, log_level, error_severity, sql_state, process_id, session_id, database_name, user_name, application_name, client_addr, client_port, log_message, short_log_message) FROM STDIN (FORMAT BINARY)"))
        {
            foreach (var row in rows)
            {
                importer.StartRow();
                importer.Write(row.ResourceId, NpgsqlDbType.Text);
                importer.Write(row.ContainerName, NpgsqlDbType.Text);
                importer.Write(row.TimeUtc.UtcDateTime, NpgsqlDbType.TimestampTz);
                importer.Write(row.IngestionBatchId, NpgsqlDbType.Text);
                importer.Write(row.BlobName, NpgsqlDbType.Text);
                importer.Write(row.IngestedAt.UtcDateTime, NpgsqlDbType.TimestampTz);
                importer.Write(row.RawPayloadJson, NpgsqlDbType.Text);
                importer.Write(row.LogCategory, NpgsqlDbType.Text);
                importer.Write(row.OperationName, NpgsqlDbType.Text);
                importer.Write(row.LogicalServerName, NpgsqlDbType.Text);
                importer.Write(row.LogLevel, NpgsqlDbType.Text);
                importer.Write(row.ErrorSeverity, NpgsqlDbType.Text);
                importer.Write(row.SqlState, NpgsqlDbType.Text);
                importer.Write(row.ProcessId, NpgsqlDbType.Bigint);
                importer.Write(row.SessionId, NpgsqlDbType.Text);
                importer.Write(row.DatabaseName, NpgsqlDbType.Text);
                importer.Write(row.UserName, NpgsqlDbType.Text);
                importer.Write(row.ApplicationName, NpgsqlDbType.Text);
                importer.Write(row.ClientAddr, NpgsqlDbType.Text);
                importer.Write(row.ClientPort, NpgsqlDbType.Integer);
                importer.Write(row.LogMessage, NpgsqlDbType.Text);
                importer.Write(row.ShortLogMessage, NpgsqlDbType.Text);
            }

            importer.Complete();
        }

        var insertSql = $"""
            WITH candidates AS (
                SELECT DISTINCT ON (
                    s.container_name,
                    s.blob_name,
                    s.time_utc,
                    COALESCE(s.raw_payload_json, '')
                )
                    s.resource_id,
                    s.container_name,
                    s.time_utc,
                    s.ingestion_batch_id,
                    s.blob_name,
                    s.ingested_at,
                    s.raw_payload_json,
                    s.log_category,
                    s.operation_name,
                    s.logical_server_name,
                    s.log_level,
                    s.error_severity,
                    s.sql_state,
                    s.process_id,
                    s.session_id,
                    s.database_name,
                    s.user_name,
                    s.application_name,
                    s.client_addr,
                    s.client_port,
                    s.log_message,
                    s.short_log_message
                FROM tmp_ingest_logs s
                ORDER BY
                    s.container_name,
                    s.blob_name,
                    s.time_utc,
                    COALESCE(s.raw_payload_json, ''),
                    s.ingested_at
            ),
            inserted AS (
                INSERT INTO {destinationTable} (
                    resource_id,
                    container_name,
                    time_utc,
                    ingestion_batch_id,
                    blob_name,
                    ingested_at,
                    raw_payload_json,
                    log_category,
                    operation_name,
                    logical_server_name,
                    log_level,
                    error_severity,
                    sql_state,
                    process_id,
                    session_id,
                    database_name,
                    user_name,
                    application_name,
                    client_addr,
                    client_port,
                    log_message,
                    short_log_message
                )
                SELECT
                    c.resource_id,
                    c.container_name,
                    c.time_utc,
                    c.ingestion_batch_id,
                    c.blob_name,
                    c.ingested_at,
                    c.raw_payload_json,
                    c.log_category,
                    c.operation_name,
                    c.logical_server_name,
                    c.log_level,
                    c.error_severity,
                    c.sql_state,
                    c.process_id,
                    c.session_id,
                    c.database_name,
                    c.user_name,
                    c.application_name,
                    c.client_addr,
                    c.client_port,
                    c.log_message,
                    c.short_log_message
                FROM candidates c
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {destinationTable} t
                    WHERE t.container_name = c.container_name
                      AND t.blob_name = c.blob_name
                      AND t.time_utc = c.time_utc
                      AND COALESCE(t.raw_payload_json, '') = COALESCE(c.raw_payload_json, '')
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
