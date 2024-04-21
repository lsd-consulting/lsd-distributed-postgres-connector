CREATE TABLE IF NOT EXISTS intercepted_interactions
(
    id               integer PRIMARY KEY generated always as identity,
    trace_id         TEXT,
    body             TEXT,
    request_headers  TEXT,
    response_headers TEXT,
    service_name     TEXT,
    target           TEXT,
    path             TEXT,
    http_status      TEXT,
    http_method      TEXT,
    interaction_type TEXT,
    profile          TEXT,
    elapsed_time     NUMERIC(20),
    created_at       timestamp with time zone
);
CREATE INDEX IF NOT EXISTS admin_query1_idx ON intercepted_interactions (created_at);
CREATE INDEX IF NOT EXISTS admin_query2_idx ON intercepted_interactions (trace_id);
