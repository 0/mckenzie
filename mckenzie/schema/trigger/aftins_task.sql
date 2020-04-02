CREATE OR REPLACE FUNCTION aftins_task()
RETURNS trigger AS $$
DECLARE
	_hist_state_id INTEGER;
BEGIN
	SELECT state_id INTO _hist_state_id
	FROM task_history
	WHERE task_id = NEW.id
	LIMIT 1;

	-- Ensure at least one row in task_history for each row in task.
	IF _hist_state_id IS NULL THEN
		RAISE EXCEPTION 'Task not found in task_history.';
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
