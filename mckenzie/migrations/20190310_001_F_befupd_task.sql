CREATE OR REPLACE FUNCTION befupd_task()
RETURNS trigger AS $$
DECLARE
	_done_id INTEGER;
BEGIN
	SELECT id INTO _done_id
	FROM task_state
	WHERE name = 'ts_done';

	IF NEW.state_id != _done_id AND NEW.synthesized THEN
		RAISE EXCEPTION 'Only done task may be synthesized.';
	END IF;

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
