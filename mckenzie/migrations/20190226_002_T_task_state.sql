ALTER TABLE task_state
ADD COLUMN IF NOT EXISTS holdable BOOLEAN NOT NULL DEFAULT FALSE;

INSERT INTO task_state (name, holdable)
VALUES
	('ts_held', FALSE),
	('ts_waiting', TRUE),
	('ts_ready', TRUE)
ON CONFLICT (name) DO NOTHING;
