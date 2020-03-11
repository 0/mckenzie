CREATE TABLE IF NOT EXISTS worker
(
	id INTEGER PRIMARY KEY,
	state_id INTEGER NOT NULL REFERENCES worker_state,
	num_cores INTEGER NOT NULL,
	time_limit INTERVAL NOT NULL,
	mem_limit_mb INTEGER NOT NULL,
	quitting BOOLEAN NOT NULL DEFAULT FALSE,
	node TEXT,
	time_start TIMESTAMP WITH TIME ZONE,
	heartbeat TIMESTAMP WITH TIME ZONE,
	time_end TIMESTAMP WITH TIME ZONE,
	num_tasks INTEGER NOT NULL DEFAULT 0,
	num_tasks_active INTEGER NOT NULL DEFAULT 0,
	cur_mem_usage_mb INTEGER,
	failure_acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
	CONSTRAINT num_tasks CHECK (num_tasks_active <= num_tasks)
);


DROP TRIGGER IF EXISTS aftins_worker
ON worker;

CREATE CONSTRAINT TRIGGER aftins_worker
AFTER INSERT
ON worker
INITIALLY DEFERRED
FOR EACH ROW
EXECUTE PROCEDURE aftins_worker();


DROP TRIGGER IF EXISTS befupd_worker
ON worker;

CREATE TRIGGER befupd_worker
BEFORE UPDATE
ON worker
FOR EACH ROW
EXECUTE PROCEDURE befupd_worker();
