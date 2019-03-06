CREATE OR REPLACE FUNCTION aftins_worker_task()
RETURNS trigger AS $$
BEGIN
	UPDATE worker
	SET
		num_tasks = num_tasks + 1,
		num_tasks_active = num_tasks_active + 1
	WHERE id = NEW.worker_id;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
