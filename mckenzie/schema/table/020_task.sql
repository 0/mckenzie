CREATE TABLE IF NOT EXISTS task
(
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL,
	state_id INTEGER NOT NULL REFERENCES task_state,
	priority INTEGER NOT NULL,
	time_limit INTERVAL NOT NULL,
	mem_limit_mb INTEGER NOT NULL,
	claimed_by INTEGER,
	claimed_since TIMESTAMP WITH TIME ZONE,
	num_dependencies INTEGER NOT NULL DEFAULT 0,
	num_dependencies_satisfied INTEGER NOT NULL DEFAULT 0,
	elapsed_time INTERVAL,
	max_mem_mb INTEGER,
	CONSTRAINT name_spaces CHECK (name NOT LIKE '%% %%'),
	CONSTRAINT dependencies_bound
		CHECK (0 <= num_dependencies
				AND 0 <= num_dependencies_satisfied
				AND num_dependencies_satisfied <= num_dependencies)
);


CREATE UNIQUE INDEX IF NOT EXISTS task_name_pattern_ops
ON task (name text_pattern_ops);

CREATE INDEX IF NOT EXISTS task_state_id
ON task (state_id);


DROP TRIGGER IF EXISTS aftins_task
ON task;

CREATE CONSTRAINT TRIGGER aftins_task
AFTER INSERT
ON task
INITIALLY DEFERRED
FOR EACH ROW
EXECUTE PROCEDURE aftins_task();


DROP TRIGGER IF EXISTS befupd_task
ON task;

CREATE TRIGGER befupd_task
BEFORE UPDATE
ON task
FOR EACH ROW
EXECUTE PROCEDURE befupd_task();


DROP TRIGGER IF EXISTS aftupd_task
ON task;

CREATE TRIGGER aftupd_task
AFTER UPDATE
ON task
FOR EACH ROW
EXECUTE PROCEDURE aftupd_task();
