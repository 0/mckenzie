CREATE OR REPLACE FUNCTION aftupd_worker_task()
RETURNS trigger AS $$
BEGIN
	UPDATE worker
	SET num_tasks_active = num_tasks_active - 1
	WHERE id = NEW.worker_id;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
