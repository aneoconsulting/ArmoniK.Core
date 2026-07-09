-- Tasks table
CREATE TABLE IF NOT EXISTS tasks (
    session_id          TEXT NOT NULL,
    task_id             TEXT PRIMARY KEY,
    owner_pod_id        TEXT NOT NULL DEFAULT '',
    owner_pod_name      TEXT NOT NULL DEFAULT '',
    payload_id          TEXT NOT NULL,
    parent_task_ids     TEXT[] NOT NULL DEFAULT '{}',
    data_dependencies   TEXT[] NOT NULL DEFAULT '{}',
    expected_output_ids TEXT[] NOT NULL DEFAULT '{}',
    initial_task_id     TEXT NOT NULL,
    created_by          TEXT NOT NULL DEFAULT '',
    retry_of_ids        TEXT[] NOT NULL DEFAULT '{}',
    status              INTEGER NOT NULL,
    status_message      TEXT NOT NULL DEFAULT '',
    options_options          JSONB NOT NULL DEFAULT '{}',
    options_max_duration     BIGINT NOT NULL DEFAULT 0,
    options_max_retries      INTEGER NOT NULL DEFAULT 0,
    options_priority         INTEGER NOT NULL DEFAULT 0,
    options_partition_id     TEXT NOT NULL DEFAULT '',
    options_app_name         TEXT NOT NULL DEFAULT '',
    options_app_version      TEXT NOT NULL DEFAULT '',
    options_app_namespace    TEXT NOT NULL DEFAULT '',
    options_app_service      TEXT NOT NULL DEFAULT '',
    options_engine_type      TEXT NOT NULL DEFAULT '',
    creation_date       TIMESTAMP NOT NULL,
    submitted_date      TIMESTAMP,
    start_date          TIMESTAMP,
    end_date            TIMESTAMP,
    reception_date      TIMESTAMP,
    acquisition_date    TIMESTAMP,
    processed_date      TIMESTAMP,
    fetched_date        TIMESTAMP,
    pod_ttl             TIMESTAMP,
    processing_to_end_duration BIGINT,
    creation_to_end_duration   BIGINT,
    received_to_end_duration   BIGINT,
    output_status       INTEGER NOT NULL DEFAULT 0,
    output_error        TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_tasks_session_id ON tasks(session_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_partition_status ON tasks(options_partition_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_owner_pod_id ON tasks(owner_pod_id);
CREATE INDEX IF NOT EXISTS idx_tasks_initial_task_id ON tasks(initial_task_id);
CREATE INDEX IF NOT EXISTS idx_tasks_created_by ON tasks(created_by);
CREATE INDEX IF NOT EXISTS idx_tasks_creation_date ON tasks(creation_date);

-- Association table for RemainingDataDependencies
CREATE TABLE IF NOT EXISTS task_remaining_dependencies (
    task_id       TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    dependency_id TEXT NOT NULL,
    PRIMARY KEY (task_id, dependency_id)
);

CREATE INDEX IF NOT EXISTS idx_trd_dependency ON task_remaining_dependencies(dependency_id);

-- Sessions table
CREATE TABLE IF NOT EXISTS sessions (
    session_id        TEXT PRIMARY KEY,
    status            INTEGER NOT NULL,
    client_submission BOOLEAN NOT NULL DEFAULT TRUE,
    worker_submission BOOLEAN NOT NULL DEFAULT TRUE,
    creation_date     TIMESTAMP NOT NULL,
    cancellation_date TIMESTAMP,
    closure_date      TIMESTAMP,
    purge_date        TIMESTAMP,
    deletion_date     TIMESTAMP,
    deletion_ttl      TIMESTAMP,
    duration          BIGINT,
    partition_ids     TEXT[] NOT NULL DEFAULT '{}',
    options_options          JSONB NOT NULL DEFAULT '{}',
    options_max_duration     BIGINT NOT NULL DEFAULT 0,
    options_max_retries      INTEGER NOT NULL DEFAULT 0,
    options_priority         INTEGER NOT NULL DEFAULT 0,
    options_partition_id     TEXT NOT NULL DEFAULT '',
    options_app_name         TEXT NOT NULL DEFAULT '',
    options_app_version      TEXT NOT NULL DEFAULT '',
    options_app_namespace    TEXT NOT NULL DEFAULT '',
    options_app_service      TEXT NOT NULL DEFAULT '',
    options_engine_type      TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
CREATE INDEX IF NOT EXISTS idx_sessions_creation_date ON sessions(creation_date);

-- Results table
CREATE TABLE IF NOT EXISTS results (
    session_id      TEXT NOT NULL,
    result_id       TEXT PRIMARY KEY,
    name            TEXT NOT NULL DEFAULT '',
    created_by      TEXT NOT NULL DEFAULT '',
    completed_by    TEXT NOT NULL DEFAULT '',
    owner_task_id   TEXT NOT NULL DEFAULT '',
    status          INTEGER NOT NULL,
    dependent_tasks TEXT[] NOT NULL DEFAULT '{}',
    creation_date   TIMESTAMP NOT NULL,
    completion_date TIMESTAMP,
    size            BIGINT NOT NULL DEFAULT 0,
    opaque_id       BYTEA NOT NULL DEFAULT ''::BYTEA,
    manual_deletion BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_results_session_id ON results(session_id);
CREATE INDEX IF NOT EXISTS idx_results_owner_task_id ON results(owner_task_id);
CREATE INDEX IF NOT EXISTS idx_results_created_by ON results(created_by);
CREATE INDEX IF NOT EXISTS idx_results_creation_date ON results(creation_date);

-- Partitions table
CREATE TABLE IF NOT EXISTS partitions (
    partition_id         TEXT PRIMARY KEY,
    parent_partition_ids TEXT[] NOT NULL DEFAULT '{}',
    pod_reserved         INTEGER NOT NULL DEFAULT 0,
    pod_max              INTEGER NOT NULL DEFAULT 0,
    preemption_pct       INTEGER NOT NULL DEFAULT 0,
    priority             INTEGER NOT NULL DEFAULT 0,
    pod_configuration    JSONB
);

-- Auth tables
CREATE TABLE IF NOT EXISTS auth_data (
    auth_id     INTEGER PRIMARY KEY,
    user_id     INTEGER NOT NULL,
    cn          TEXT NOT NULL,
    fingerprint TEXT
);

CREATE TABLE IF NOT EXISTS user_data (
    user_id  INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    roles    INTEGER[] NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS role_data (
    role_id     INTEGER PRIMARY KEY,
    role_name   TEXT NOT NULL,
    permissions TEXT[] NOT NULL DEFAULT '{}'
);

-- Logical replication via pgoutput (requires wal_level = logical).
-- On AWS RDS / Aurora PostgreSQL: set rds.logical_replication = 1 in the DB parameter group and reboot.
-- The database user must have the REPLICATION attribute (or rds_replication role on RDS).
-- REPLICA IDENTITY is left at DEFAULT (primary key only): the old row values are not included
-- in WAL UPDATE messages, so watchers cannot suppress spurious events or recover previous
-- field values. This matches the behaviour of the MongoDB adaptor.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'armonik_pub') THEN
    CREATE PUBLICATION armonik_pub FOR TABLE tasks, results;
  END IF;
END;
$$;
