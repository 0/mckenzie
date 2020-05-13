CREATE TABLE IF NOT EXISTS task_history
(
	id SERIAL PRIMARY KEY,
	task_id INTEGER NOT NULL REFERENCES task,
	state_id INTEGER NOT NULL REFERENCES task_state,
	time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
	reason_id INTEGER NOT NULL REFERENCES task_reason,
	worker_id INTEGER REFERENCES worker
);


CREATE INDEX IF NOT EXISTS task_history_task_id
ON task_history (task_id);

CREATE INDEX IF NOT EXISTS task_history_time
ON task_history (time);


DROP TRIGGER IF EXISTS aftins_task_history
ON task_history;

CREATE TRIGGER aftins_task_history
AFTER INSERT
ON task_history
FOR EACH ROW
EXECUTE PROCEDURE aftins_task_history();
