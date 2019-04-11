ALTER TABLE task_state
ADD COLUMN IF NOT EXISTS pending BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE task_state
SET pending = TRUE
WHERE name IN ('ts_new', 'ts_held', 'ts_waiting', 'ts_ready', 'ts_running', 'ts_failed');

INSERT INTO task_state (name, holdable, pending)
VALUES ('ts_cleaned', FALSE, FALSE)
ON CONFLICT (name) DO NOTHING;
