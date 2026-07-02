using Npgsql;

namespace DmsMetricsCollector.Ingestion;

internal sealed record DestinationTable(string SqlConnectionString, string SchemaName, string TableName)
{
    public string FullName => $"\"{SchemaName}\".\"{TableName}\"";

    public async Task EnsureRequiredColumnsAsync(IngestionKind kind)
    {
        await using var connection = new NpgsqlConnection(SqlConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        var quotedSchema = QuoteIdentifier(SchemaName);
        var quotedTable = QuoteIdentifier(TableName);
        var tableRef = $"{quotedSchema}.{quotedTable}";

        var ddl = kind switch
        {
            IngestionKind.Metrics => $"""
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS metric_avg DOUBLE PRECISION;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS metric_min DOUBLE PRECISION;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS metric_max DOUBLE PRECISION;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS metric_total DOUBLE PRECISION;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS metric_count DOUBLE PRECISION;
                """,
            _ => $"""
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS container_name TEXT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS ingestion_batch_id TEXT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS raw_payload_json TEXT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS log_category TEXT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS operation_name TEXT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS log_level TEXT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS error_severity TEXT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS sql_state TEXT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS process_id BIGINT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS session_id TEXT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS database_name TEXT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS user_name TEXT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS application_name TEXT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS client_addr TEXT;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS client_port INTEGER;
                ALTER TABLE {tableRef} ADD COLUMN IF NOT EXISTS log_message TEXT;
                """
        };

        await using var command = new NpgsqlCommand(ddl, connection);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }
}

internal static class SqlDestination
{
    public static async Task<DestinationTable> ResolveAsync(string sqlConnectionString, IngestionKind kind)
    {
        await using var connection = new NpgsqlConnection(sqlConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        var desiredTableName = kind switch
        {
            IngestionKind.Metrics => "azure_metric_raw",
            IngestionKind.PostgreSqlServerLogs => "postgresql_server_log_raw",
            IngestionKind.PgBouncerLogs => "pgbouncer_log_raw",
            _ => throw new InvalidOperationException("alllogs is not a concrete destination")
        };

        const string lookupSql = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_name = @table_name
            ORDER BY CASE WHEN table_schema = 'public' THEN 0 ELSE 1 END, table_schema
            LIMIT 1;
            """;

        await using (var command = new NpgsqlCommand(lookupSql, connection))
        {
            command.Parameters.AddWithValue("table_name", desiredTableName);
            await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
            if (await reader.ReadAsync().ConfigureAwait(false))
                return new DestinationTable(sqlConnectionString, reader.GetString(0), reader.GetString(1));
        }

        await EnsureDefaultTableAsync(connection, kind).ConfigureAwait(false);

        await using (var retryCommand = new NpgsqlCommand(lookupSql, connection))
        {
            retryCommand.Parameters.AddWithValue("table_name", desiredTableName);
            await using var retryReader = await retryCommand.ExecuteReaderAsync().ConfigureAwait(false);
            if (await retryReader.ReadAsync().ConfigureAwait(false))
                return new DestinationTable(sqlConnectionString, retryReader.GetString(0), retryReader.GetString(1));
        }

        throw new InvalidOperationException($"Unable to locate or create {desiredTableName} in the consolidation database.");
    }

    private static async Task EnsureDefaultTableAsync(NpgsqlConnection connection, IngestionKind kind)
    {
        var ddl = kind switch
        {
            IngestionKind.Metrics => """
                CREATE TABLE IF NOT EXISTS public.azure_metric_raw
                (
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
                );
                """,
            IngestionKind.PostgreSqlServerLogs => """
                CREATE TABLE IF NOT EXISTS public.postgresql_server_log_raw
                (
                    resource_id TEXT NOT NULL,
                    container_name TEXT NOT NULL,
                    time_utc TIMESTAMPTZ NOT NULL,
                    ingestion_batch_id TEXT NOT NULL,
                    blob_name TEXT NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    raw_payload_json TEXT NULL,
                    log_category TEXT NULL,
                    operation_name TEXT NULL,
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
                    log_message TEXT NULL
                );
                """,
            IngestionKind.PgBouncerLogs => """
                CREATE TABLE IF NOT EXISTS public.pgbouncer_log_raw
                (
                    resource_id TEXT NOT NULL,
                    container_name TEXT NOT NULL,
                    time_utc TIMESTAMPTZ NOT NULL,
                    ingestion_batch_id TEXT NOT NULL,
                    blob_name TEXT NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    raw_payload_json TEXT NULL,
                    log_category TEXT NULL,
                    operation_name TEXT NULL,
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
                    log_message TEXT NULL
                );
                """,
            _ => throw new InvalidOperationException("alllogs is not a concrete destination")
        };

        await using var command = new NpgsqlCommand(ddl, connection);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }
}
