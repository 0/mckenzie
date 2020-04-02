CREATE OR REPLACE FUNCTION befupd_worker()
RETURNS trigger AS $$
DECLARE
	_running_id INTEGER;
	_failed_id INTEGER;
BEGIN
	SELECT id INTO _running_id
	FROM worker_state
	WHERE name = 'ws_running';

	SELECT id INTO _failed_id
	FROM worker_state
	WHERE name = 'ws_failed';

	IF NEW.state_id != _running_id AND NEW.num_tasks_active > 0 THEN
		RAISE EXCEPTION 'Only running worker can have active tasks.';
	END IF;

	IF NEW.state_id != _failed_id AND NEW.failure_acknowledged THEN
		RAISE EXCEPTION 'Only failed worker can have acknowledged failure.';
	END IF;

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
