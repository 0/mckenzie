CREATE OR REPLACE FUNCTION aftins_worker_history()
RETURNS trigger AS $$
DECLARE
	_running_id INTEGER;
BEGIN
	SELECT id INTO _running_id
	FROM worker_state
	WHERE name = 'ws_running';

	UPDATE worker
	SET state_id = NEW.state_id
	WHERE id = NEW.worker_id;

	-- Clear stats that only make sense for a running worker.
	IF NEW.state_id != _running_id THEN
		UPDATE worker
		SET
			quitting = FALSE,
			cur_mem_usage_mb = NULL
		WHERE id = NEW.worker_id;
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
