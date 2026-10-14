-- Authorization: who a session belongs to and who it was created under

ALTER TABLE sessions ADD COLUMN IF NOT EXISTS owner         TEXT;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS created_under TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS shared_with   TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS delegated_to  TEXT[] NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_sessions_owner ON sessions(owner);
CREATE INDEX IF NOT EXISTS idx_sessions_created_under ON sessions USING GIN (created_under);
CREATE INDEX IF NOT EXISTS idx_sessions_shared_with ON sessions USING GIN (shared_with);
CREATE INDEX IF NOT EXISTS idx_sessions_delegated_to ON sessions USING GIN (delegated_to);

-- Grants and permission sets of the access model

CREATE TABLE IF NOT EXISTS grants (
    grant_id       TEXT PRIMARY KEY,
    principal      TEXT NOT NULL,
    permission_set TEXT NOT NULL,
    condition      TEXT,
    origin         INTEGER NOT NULL,
    creation_date  TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS permission_sets (
    name        TEXT PRIMARY KEY,
    version     INTEGER NOT NULL,
    permissions TEXT[] NOT NULL DEFAULT '{}',
    origin      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS permission_set_versions (
    name        TEXT NOT NULL,
    version     INTEGER NOT NULL,
    permissions TEXT[] NOT NULL DEFAULT '{}',
    origin      INTEGER NOT NULL,
    PRIMARY KEY (name, version)
);

-- Revision of the configuration last reconciled, and generation of the grants and permission sets: the last change
-- that began, and the last that ended

CREATE TABLE IF NOT EXISTS authorization_state (
    id       INTEGER PRIMARY KEY,
    revision INTEGER NOT NULL DEFAULT 0,
    begun    BIGINT NOT NULL DEFAULT 0,
    ended    BIGINT NOT NULL DEFAULT 0
);

-- The legacy user, role and certificate tables are replaced by the grants of the access model

DROP TABLE IF EXISTS auth_data;
DROP TABLE IF EXISTS user_data;
DROP TABLE IF EXISTS role_data;
