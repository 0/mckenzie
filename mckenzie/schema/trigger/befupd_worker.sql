CREATE OR REPLACE FUNCTION befupd_worker()
RETURNS trigger AS $$
DECLARE
	_state_transition INTEGER;
	_job_running BOOLEAN;
BEGIN
	SELECT id INTO _state_transition
	FROM worker_state_transition
	WHERE from_state_id = OLD.state_id
	AND to_state_id = NEW.state_id;

	SELECT job_running INTO STRICT _job_running
	FROM worker_state
	WHERE id = NEW.state_id;

	IF OLD.state_id != NEW.state_id AND _state_transition IS NULL THEN
		RAISE EXCEPTION 'Invalid state transition.';
	END IF;

	IF NOT _job_running AND NEW.num_tasks_active > 0 THEN
		RAISE EXCEPTION 'Only running worker can have active tasks.';
	END IF;

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
