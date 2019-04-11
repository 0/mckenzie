CREATE OR REPLACE FUNCTION befupd_task()
RETURNS trigger AS $$
DECLARE
	_new_pending BOOLEAN;
BEGIN
	SELECT pending INTO _new_pending
	FROM task_state
	WHERE id = NEW.state_id;

	IF _new_pending AND NEW.synthesized THEN
		RAISE EXCEPTION 'Pending task may not be synthesized.';
	END IF;

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
