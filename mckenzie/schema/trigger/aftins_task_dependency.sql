CREATE OR REPLACE FUNCTION aftins_task_dependency()
RETURNS trigger AS $$
DECLARE
	_dependency_satisfied BOOLEAN;
	_dependency_soft_satisfied BOOLEAN;
	_num_satisfied INTEGER;
BEGIN
	SELECT ts.satisfies_dependency, ts.satisfies_soft_dependency
	INTO STRICT _dependency_satisfied, _dependency_soft_satisfied
	FROM task t
	JOIN task_state ts ON ts.id = t.state_id
	WHERE t.id = NEW.dependency_id;

	IF _dependency_satisfied OR (NEW.soft AND _dependency_soft_satisfied) THEN
		_num_satisfied = 1;
	ELSE
		_num_satisfied = 0;
	END IF;

	UPDATE task
	SET
		num_dependencies = num_dependencies + 1,
		num_dependencies_satisfied = num_dependencies_satisfied + _num_satisfied
	WHERE id = NEW.task_id;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
