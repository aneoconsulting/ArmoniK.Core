-- Authorization: who a session belongs to and who it was created under

ALTER TABLE sessions ADD COLUMN IF NOT EXISTS owner         TEXT;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS created_under TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS shared_with   TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS delegated_to  TEXT[] NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_sessions_owner ON sessions(owner);
CREATE INDEX IF NOT EXISTS idx_sessions_created_under ON sessions USING GIN (created_under);
CREATE INDEX IF NOT EXISTS idx_sessions_shared_with ON sessions USING GIN (shared_with);
CREATE INDEX IF NOT EXISTS idx_sessions_delegated_to ON sessions USING GIN (delegated_to);
