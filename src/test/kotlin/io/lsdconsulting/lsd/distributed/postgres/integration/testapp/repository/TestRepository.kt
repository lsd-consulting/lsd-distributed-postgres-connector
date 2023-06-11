package io.lsdconsulting.lsd.distributed.postgres.integration.testapp.repository

import org.springframework.stereotype.Component
import javax.sql.DataSource

private const val PREPARE_DATABASE = """
    CREATE SCHEMA lsd;
    CREATE TABLE IF NOT EXISTS lsd.intercepted_interactions (
    id integer PRIMARY KEY generated always as identity, 
    trace_id VARCHAR(30), 
    body TEXT,
    request_headers TEXT,
    response_headers TEXT,
    service_name VARCHAR(30),
    target VARCHAR(30),
    path VARCHAR(100),
    http_status VARCHAR(35),
    http_method VARCHAR(7),
    interaction_type VARCHAR(8),
    profile VARCHAR(20),
    elapsed_time NUMERIC(20),
    created_at timestamp with time zone
);
"""

@Component
class TestRepository {
    fun createTable(dataSource: DataSource) {
        dataSource.connection.use { con ->
            con.prepareStatement(PREPARE_DATABASE).use { pst ->
                pst.executeUpdate()
            }
        }
    }
}
