CREATE TABLE IF NOT EXISTS worker_state
(
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	-- Workers in the state should have a Slurm job that is either pending (PD)
	-- or running (R).
	job_exists BOOLEAN NOT NULL DEFAULT FALSE,
	-- Workers in the state should have a Slurm job that is running (R).
	job_running BOOLEAN NOT NULL DEFAULT FALSE,
	-- job_running -> job_exists
	CONSTRAINT running_exists CHECK (NOT job_running OR job_exists)
);


INSERT INTO worker_state (name)
VALUES
	('ws_cancelled'),
	('ws_queued'),
	('ws_running'),
	('ws_failed'),
	('ws_quitting'),
	('ws_done')
ON CONFLICT (name) DO NOTHING;

UPDATE worker_state
SET job_exists = TRUE
WHERE name IN ('ws_queued', 'ws_running', 'ws_quitting');

UPDATE worker_state
SET job_running = TRUE
WHERE name IN ('ws_running', 'ws_quitting');
