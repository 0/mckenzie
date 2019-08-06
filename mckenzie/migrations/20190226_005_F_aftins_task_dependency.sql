CREATE OR REPLACE FUNCTION aftins_task_dependency()
RETURNS trigger AS $$
BEGIN
	UPDATE task
	SET
		num_dependencies = num_dependencies + 1,
		num_dependencies_pending = num_dependencies_pending + 1
	WHERE id = NEW.task_id;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
