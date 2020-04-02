CREATE TABLE IF NOT EXISTS worker_state
(
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	job_exists BOOLEAN NOT NULL DEFAULT FALSE
);


INSERT INTO worker_state (name, job_exists)
VALUES
	('ws_queued', TRUE),
	('ws_cancelled', FALSE),
	('ws_running', TRUE),
	('ws_done', FALSE),
	('ws_failed', FALSE)
ON CONFLICT (name) DO NOTHING;
