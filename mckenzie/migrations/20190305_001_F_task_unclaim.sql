CREATE OR REPLACE FUNCTION task_unclaim(
	_task_id INTEGER,
	_claimed_by INTEGER,
	_force BOOLEAN = FALSE
)
RETURNS BOOLEAN AS $$
DECLARE
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
		OR t.claimed_by = _claimed_by
	)
	RETURNING TRUE INTO _success;

	RETURN _success IS NOT NULL;
END;
$$ LANGUAGE plpgsql;
