CREATE OR REPLACE FUNCTION aftins_task_dependency()
RETURNS trigger AS $$
DECLARE
	_done_id INTEGER;
	_dependency_state_id INTEGER;
	_num_pending INTEGER := 1;
BEGIN
	SELECT id INTO _done_id
	FROM task_state
	WHERE name = 'ts_done';

	SELECT state_id INTO _dependency_state_id
	FROM task
	WHERE id = NEW.dependency_id;

	IF _dependency_state_id = _done_id THEN
		_num_pending = 0;
	END IF;

	UPDATE task
	SET
		num_dependencies = num_dependencies + 1,
		num_dependencies_pending = num_dependencies_pending + _num_pending
	WHERE id = NEW.task_id;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
