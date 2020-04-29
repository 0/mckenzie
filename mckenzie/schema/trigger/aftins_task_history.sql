{{{X from .task import TaskState}}}


CREATE OR REPLACE FUNCTION aftins_task_history()
RETURNS trigger AS $$
BEGIN
	UPDATE task
	SET state_id = NEW.state_id
	WHERE id = NEW.task_id;

	-- If the task just started running, link it to a worker.
	IF NEW.state_id = {{{V TaskState.ts_running}}} THEN
		INSERT INTO worker_task (worker_id, task_id)
		VALUES (NEW.worker_id, NEW.task_id);
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
