CREATE OR REPLACE FUNCTION aftins_worker_history()
RETURNS trigger AS $$
DECLARE
	_job_running BOOLEAN;
BEGIN
	SELECT job_running INTO STRICT _job_running
	FROM worker_state
	WHERE id = NEW.state_id;

	UPDATE worker
	SET state_id = NEW.state_id
	WHERE id = NEW.worker_id;

	-- Clear stats that only make sense for a running worker.
	IF NOT _job_running THEN
		UPDATE worker
		SET cur_mem_usage_mb = NULL
		WHERE id = NEW.worker_id;
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
