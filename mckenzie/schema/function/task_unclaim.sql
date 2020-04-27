CREATE OR REPLACE FUNCTION task_unclaim(
	_task_id INTEGER,
	_force BOOLEAN = FALSE
)
RETURNS BOOLEAN AS $$
DECLARE
	-- Allow no ident to be set if the change will be forced.
	_ident INTEGER := current_setting('mck.ident', _force);
	_success BOOLEAN;
BEGIN
	UPDATE task t
	SET
		claimed_by = NULL,
		claimed_since = NULL
	WHERE t.id = _task_id
	AND (
		_force
		OR t.claimed_by IS NULL
		OR t.claimed_by = _ident
	)
	RETURNING TRUE INTO _success;

	RETURN _success IS NOT NULL;
END;
$$ LANGUAGE plpgsql;
