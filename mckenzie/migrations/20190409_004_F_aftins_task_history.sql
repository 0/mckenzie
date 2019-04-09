CREATE OR REPLACE FUNCTION aftins_task_history()
RETURNS trigger AS $$
DECLARE
	_running_id INTEGER;
	_done_id INTEGER;
BEGIN
	SELECT id INTO _running_id
	FROM task_state
	WHERE name = 'ts_running';

	SELECT id INTO _done_id
	FROM task_state
	WHERE name = 'ts_done';

	UPDATE task
	SET state_id = NEW.state_id
	WHERE id = NEW.task_id;

	-- If the task just started running, link it to a worker.
	IF NEW.state_id = _running_id THEN
		INSERT INTO worker_task (worker_id, task_id)
		VALUES (NEW.worker_id, NEW.task_id);
	END IF;

	-- Clear values that only make sense for a done task.
	IF NEW.state_id != _done_id THEN
		UPDATE task
		SET partial_cleaned = FALSE
		WHERE id = NEW.task_id;
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
