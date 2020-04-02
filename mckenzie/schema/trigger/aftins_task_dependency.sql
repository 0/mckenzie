CREATE OR REPLACE FUNCTION aftins_task_dependency()
RETURNS trigger AS $$
DECLARE
	_cleaned_id INTEGER;
	_dependency_state_id INTEGER;
	_dependency_pending BOOLEAN;
	_num_pending INTEGER := 0;
	_num_cleaned INTEGER := 0;
BEGIN
	SELECT id INTO _cleaned_id
	FROM task_state
	WHERE name = 'ts_cleaned';

	SELECT state_id INTO _dependency_state_id
	FROM task
	WHERE id = NEW.dependency_id;

	SELECT pending INTO _dependency_pending
	FROM task_state
	WHERE id = _dependency_state_id;

	IF _dependency_pending THEN
		_num_pending = 1;
	ELSIF NOT NEW.soft AND _dependency_state_id = _cleaned_id THEN
		_num_cleaned = 1;
	END IF;

	UPDATE task
	SET
		num_dependencies = num_dependencies + 1,
		num_dependencies_pending = num_dependencies_pending + _num_pending,
		num_dependencies_cleaned = num_dependencies_cleaned + _num_cleaned
	WHERE id = NEW.task_id;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
