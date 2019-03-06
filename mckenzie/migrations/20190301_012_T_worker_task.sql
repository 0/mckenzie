CREATE TABLE IF NOT EXISTS worker_task
(
	id SERIAL PRIMARY KEY,
	worker_id INTEGER NOT NULL REFERENCES worker,
	task_id INTEGER NOT NULL REFERENCES task,
	active BOOLEAN NOT NULL DEFAULT TRUE,
	time_active TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
	time_inactive TIMESTAMP WITH TIME ZONE,
	CONSTRAINT one_active EXCLUDE USING btree (task_id WITH =) WHERE (active)
);


DROP TRIGGER IF EXISTS aftins_worker_task
ON worker_task;

CREATE TRIGGER aftins_worker_task
AFTER INSERT
ON worker_task
FOR EACH ROW
EXECUTE PROCEDURE aftins_worker_task();


DROP TRIGGER IF EXISTS befupd_worker_task
ON worker_task;

CREATE TRIGGER befupd_worker_task
BEFORE UPDATE
ON worker_task
FOR EACH ROW
EXECUTE PROCEDURE befupd_worker_task();


DROP TRIGGER IF EXISTS aftupd_worker_task
ON worker_task;

CREATE TRIGGER aftupd_worker_task
AFTER UPDATE
ON worker_task
FOR EACH ROW
EXECUTE PROCEDURE aftupd_worker_task();
