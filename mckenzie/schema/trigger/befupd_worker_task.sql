CREATE OR REPLACE FUNCTION befupd_worker_task()
RETURNS trigger AS $$
BEGIN
	IF NOT (OLD.active AND NOT NEW.active) THEN
		RAISE EXCEPTION 'Can only deactivate task.';
	END IF;

	NEW.time_inactive = NOW();

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
