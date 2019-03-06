CREATE OR REPLACE FUNCTION befupd_worker()
RETURNS trigger AS $$
DECLARE
	_running_id INTEGER;
BEGIN
	SELECT id INTO _running_id
	FROM worker_state
	WHERE name = 'ws_running';

	IF NEW.state_id != _running_id AND NEW.num_tasks_active > 0 THEN
		RAISE EXCEPTION 'Only running worker can have active tasks.';
	END IF;

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
