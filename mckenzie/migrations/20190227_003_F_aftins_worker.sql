CREATE OR REPLACE FUNCTION aftins_worker()
RETURNS trigger AS $$
DECLARE
	_hist_state_id INTEGER;
BEGIN
	SELECT state_id INTO _hist_state_id
	FROM worker_history
	WHERE worker_id = NEW.id
	LIMIT 1;

	-- Ensure at least one row in worker_history for each row in worker.
	IF _hist_state_id IS NULL THEN
		RAISE EXCEPTION 'Worker not found in worker_history.';
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
