CREATE OR REPLACE FUNCTION aftins_task()
RETURNS trigger AS $$
DECLARE
	_hist_present INTEGER;
BEGIN
	SELECT 1 INTO _hist_present
	FROM task_history
	WHERE task_id = NEW.id
	LIMIT 1;

	-- Ensure at least one row in task_history for each row in task.
	IF _hist_present IS NULL THEN
		RAISE EXCEPTION 'Task not found in task_history.';
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
