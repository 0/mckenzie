CREATE OR REPLACE FUNCTION aftupd_worker()
RETURNS trigger AS $$
DECLARE
	_note_quitting_id INTEGER;
BEGIN
	SELECT id INTO _note_quitting_id
	FROM worker_note
	WHERE name = 'wn_quitting';

	IF NOT OLD.quitting and NEW.quitting THEN
		INSERT INTO worker_note_history (worker_id, note_id, note_args)
		VALUES (NEW.id, _note_quitting_id, ARRAY[]::TEXT[]);
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
