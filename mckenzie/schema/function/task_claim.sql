CREATE OR REPLACE FUNCTION task_claim(
	_task_id INTEGER
)
RETURNS BOOLEAN AS $$
DECLARE
	_ident INTEGER := current_setting('mck.ident');
	_success BOOLEAN;
BEGIN
	UPDATE task t
	SET
		claimed_by = _ident,
		claimed_since = CASE WHEN t.claimed_since IS NULL THEN NOW() ELSE t.claimed_since END
	WHERE t.id = _task_id
	AND (
		t.claimed_by IS NULL
		OR t.claimed_by = _ident
	)
	RETURNING TRUE INTO _success;

	RETURN _success IS NOT NULL;
END;
$$ LANGUAGE plpgsql;
