CREATE OR REPLACE FUNCTION aftupd_task()
RETURNS trigger AS $$
DECLARE
	_waiting_id INTEGER;
	_ready_id INTEGER;
	_reason_id INTEGER;
	_claim_success BOOLEAN;
BEGIN
	SELECT id INTO _waiting_id
	FROM task_state
	WHERE name = 'ts_waiting';

	SELECT id INTO _ready_id
	FROM task_state
	WHERE name = 'ts_ready';

	SELECT id INTO _reason_id
	FROM task_reason
	WHERE name = 'tr_ready';

	-- If the task is in "waiting" and has no pending dependencies, move it to
	-- "ready". We make sure not to touch the task if it's claimed. When it is
	-- later unclaimed, this trigger will run again automatically, and we will
	-- make another attempt if it's still relevant.
	IF NEW.state_id = _waiting_id AND NEW.num_dependencies_pending = 0
			AND NEW.claimed_by IS NULL THEN

		INSERT INTO task_history (task_id, state_id, reason_id)
		VALUES (NEW.id, _ready_id, _reason_id);
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
