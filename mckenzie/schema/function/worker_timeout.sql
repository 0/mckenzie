CREATE OR REPLACE FUNCTION worker_timeout(
	_worker worker,
	_threshold INTERVAL
)
RETURNS BOOLEAN AS $$
DECLARE
	_job_running BOOLEAN;
BEGIN
	SELECT job_running INTO STRICT _job_running
	FROM worker_state
	WHERE id = _worker.state_id;

	IF NOT _job_running THEN
		RETURN NULL;
	END IF;

	RETURN (
		_worker.heartbeat IS NULL
		OR (NOW() - _worker.heartbeat) > _threshold
	);
END;
$$ LANGUAGE plpgsql;
