CREATE OR REPLACE FUNCTION task_claim(
	_task_id INTEGER,
	_claimed_by INTEGER
)
RETURNS BOOLEAN AS $$
DECLARE
	_success BOOLEAN;
BEGIN
	UPDATE task t
	SET
		claimed_by = _claimed_by,
		claimed_since = CASE WHEN t.claimed_since IS NULL THEN NOW() ELSE t.claimed_since END
	WHERE t.id = _task_id
	AND (
		t.claimed_by IS NULL
		OR t.claimed_by = _claimed_by
	)
	RETURNING TRUE INTO _success;

	RETURN _success IS NOT NULL;
END;
$$ LANGUAGE plpgsql;
