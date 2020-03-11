CREATE OR REPLACE FUNCTION befupd_task()
RETURNS trigger AS $$
DECLARE
	_state_transition INTEGER;
BEGIN
	SELECT id INTO _state_transition
	FROM task_state_transition
	WHERE from_state_id = OLD.state_id
	AND to_state_id = NEW.state_id;

	IF OLD.state_id != NEW.state_id AND _state_transition IS NULL THEN
		RAISE EXCEPTION 'Invalid state transition.';
	END IF;

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
