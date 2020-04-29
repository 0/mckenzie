CREATE TABLE IF NOT EXISTS worker_state
(
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	-- Workers in the state should have a Slurm job that is either pending (PD)
	-- or running (R).
	job_exists BOOLEAN NOT NULL DEFAULT FALSE,
	-- Workers in the state should have a Slurm job that is running (R).
	job_running BOOLEAN NOT NULL DEFAULT FALSE,
	-- job_running -> job_exists
	CONSTRAINT running_exists CHECK (NOT job_running OR job_exists)
);
