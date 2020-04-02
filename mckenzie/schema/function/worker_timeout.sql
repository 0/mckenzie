CREATE OR REPLACE FUNCTION worker_timeout(
	_worker worker,
	_threshold INTERVAL
)
RETURNS BOOLEAN AS $$
DECLARE
	_running_id INTEGER;
BEGIN
	SELECT id INTO _running_id
	FROM worker_state
	WHERE name = 'ws_running';

	IF _worker.state_id != _running_id THEN
		RETURN NULL;
	END IF;

	RETURN (
		_worker.heartbeat IS NULL
		OR (NOW() - _worker.heartbeat) > _threshold
	);
END;
$$ LANGUAGE plpgsql;
