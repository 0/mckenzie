CREATE TABLE IF NOT EXISTS task_state
(
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	holdable BOOLEAN NOT NULL,
	pending BOOLEAN NOT NULL
);


INSERT INTO task_state (name, holdable, pending)
VALUES
	('ts_new', FALSE, TRUE),
	('ts_held', FALSE, TRUE),
	('ts_waiting', TRUE, TRUE),
	('ts_ready', TRUE, TRUE),
	('ts_running', FALSE, TRUE),
	('ts_done', FALSE, FALSE),
	('ts_failed', FALSE, TRUE),
	('ts_cleaned', FALSE, FALSE),
	('ts_cancelled', FALSE, TRUE)
ON CONFLICT (name) DO NOTHING;
