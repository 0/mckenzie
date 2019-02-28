CREATE OR REPLACE FUNCTION aftins_task_history()
RETURNS trigger AS $$
BEGIN
	UPDATE task
	SET state_id = NEW.state_id
	WHERE id = NEW.task_id;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
