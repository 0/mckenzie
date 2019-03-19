CREATE OR REPLACE FUNCTION aftupd_task()
RETURNS trigger AS $$
DECLARE
	_waiting_id INTEGER;
	_ready_id INTEGER;
	_running_id INTEGER;
	_done_id INTEGER;
	_reason_waiting_id INTEGER;
	_reason_ready_id INTEGER;
	_note_time_id INTEGER;
	_note_mem_id INTEGER;
	_dependent_task_id INTEGER;
BEGIN
	SELECT id INTO _waiting_id
	FROM task_state
	WHERE name = 'ts_waiting';

	SELECT id INTO _ready_id
	FROM task_state
	WHERE name = 'ts_ready';

	SELECT id INTO _running_id
	FROM task_state
	WHERE name = 'ts_running';

	SELECT id INTO _done_id
	FROM task_state
	WHERE name = 'ts_done';

	SELECT id INTO _reason_waiting_id
	FROM task_reason
	WHERE name = 'tr_waiting_dep';

	SELECT id INTO _reason_ready_id
	FROM task_reason
	WHERE name = 'tr_ready';

	SELECT id INTO _note_time_id
	FROM task_note
	WHERE name = 'tn_change_time';

	SELECT id INTO _note_mem_id
	FROM task_note
	WHERE name = 'tn_change_mem';

	-- If the task is in "waiting" and has no pending dependencies, move it to
	-- "ready", and vice versa. We make sure not to touch the task if it's
	-- claimed. When it is later unclaimed, this trigger will run again
	-- automatically, and we will make another attempt if it's still relevant.
	IF NEW.state_id = _waiting_id AND NEW.num_dependencies_pending = 0
			AND NEW.claimed_by IS NULL THEN

		INSERT INTO task_history (task_id, state_id, reason_id)
		VALUES (NEW.id, _ready_id, _reason_ready_id);
	ELSIF NEW.state_id = _ready_id AND NEW.num_dependencies_pending != 0
			AND NEW.claimed_by IS NULL THEN

		INSERT INTO task_history (task_id, state_id, reason_id)
		VALUES (NEW.id, _waiting_id, _reason_waiting_id);
	END IF;

	-- If the task has stopped running, ensure it's not active.
	IF OLD.state_id = _running_id AND NEW.state_id != _running_id THEN
		UPDATE worker_task
		SET active = FALSE
		WHERE task_id = NEW.id
		AND active;
	END IF;

	-- If this task has finished or been reset, update all the tasks that
	-- depend on it.
	IF OLD.state_id != _done_id AND NEW.state_id = _done_id THEN
		FOR _dependent_task_id IN
				SELECT task_id
				FROM task_dependency
				WHERE dependency_id = NEW.id
				LOOP

			UPDATE task
			SET num_dependencies_pending = num_dependencies_pending - 1
			WHERE id = _dependent_task_id;
		END LOOP;
	ELSIF OLD.state_id = _done_id AND NEW.state_id != _done_id THEN
		FOR _dependent_task_id IN
				SELECT task_id
				FROM task_dependency
				WHERE dependency_id = NEW.id
				LOOP

			UPDATE task
			SET num_dependencies_pending = num_dependencies_pending + 1
			WHERE id = _dependent_task_id;
		END LOOP;
	END IF;

	-- If the time or memory limits have been changed, make a note of this.
	IF OLD.time_limit != NEW.time_limit THEN
		INSERT INTO task_note_history (task_id, note_id, note_args)
		VALUES (NEW.id, _note_time_id, ARRAY[OLD.time_limit, NEW.time_limit]);
	END IF;

	IF OLD.mem_limit_mb != NEW.mem_limit_mb THEN
		INSERT INTO task_note_history (task_id, note_id, note_args)
		VALUES (NEW.id, _note_mem_id, ARRAY[OLD.mem_limit_mb, NEW.mem_limit_mb]);
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
