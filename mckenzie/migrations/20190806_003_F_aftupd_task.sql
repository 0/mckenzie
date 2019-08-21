CREATE OR REPLACE FUNCTION aftupd_task()
RETURNS trigger AS $$
DECLARE
	_waiting_id INTEGER;
	_ready_id INTEGER;
	_running_id INTEGER;
	_cleaned_id INTEGER;
	_reason_waiting_id INTEGER;
	_reason_ready_id INTEGER;
	_note_time_id INTEGER;
	_note_mem_id INTEGER;
	_note_partial_clean_id INTEGER;
	_note_marked_for_clean_id INTEGER;
	_note_unmarked_for_clean_id INTEGER;
	_old_pending BOOLEAN;
	_new_pending BOOLEAN;
	_num_dependencies_not_done INTEGER;
	_pending_diff INTEGER := 0;
	_cleaned_diff INTEGER := 0;
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

	SELECT id INTO _cleaned_id
	FROM task_state
	WHERE name = 'ts_cleaned';

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

	SELECT id INTO _note_partial_clean_id
	FROM task_note
	WHERE name = 'tn_partial_clean';

	SELECT id INTO _note_marked_for_clean_id
	FROM task_note
	WHERE name = 'tn_marked_for_clean';

	SELECT id INTO _note_unmarked_for_clean_id
	FROM task_note
	WHERE name = 'tn_unmarked_for_clean';

	SELECT pending INTO _old_pending
	FROM task_state
	WHERE id = OLD.state_id;

	SELECT pending INTO _new_pending
	FROM task_state
	WHERE id = NEW.state_id;

	_num_dependencies_not_done := NEW.num_dependencies_pending + NEW.num_dependencies_cleaned;

	-- If the task is in "waiting" and has only "done" dependencies, move it to
	-- "ready", and vice versa. We make sure not to touch the task if it's
	-- claimed. When it is later unclaimed, this trigger will run again
	-- automatically, and we will make another attempt if it's still relevant.
	IF NEW.state_id = _waiting_id AND _num_dependencies_not_done = 0
			AND NEW.claimed_by IS NULL THEN

		INSERT INTO task_history (task_id, state_id, reason_id)
		VALUES (NEW.id, _ready_id, _reason_ready_id);
	ELSIF NEW.state_id = _ready_id AND _num_dependencies_not_done != 0
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

	-- If this task has started or ceased being pending or cleaned, update all
	-- the tasks that depend on it.
	IF _old_pending AND NOT _new_pending THEN
		_pending_diff := -1;
	ELSIF NOT _old_pending AND _new_pending THEN
		_pending_diff := +1;
	END IF;

	IF OLD.state_id = _cleaned_id AND NEW.state_id != _cleaned_id THEN
		_cleaned_diff := -1;
	ELSIF OLD.state_id != _cleaned_id AND NEW.state_id = _cleaned_id THEN
		_cleaned_diff := +1;
	END IF;

	IF _pending_diff != 0 THEN
		-- TASK_DEPENDENCY_ACCESS
		PERFORM pg_advisory_xact_lock_shared(1001);

		FOR _dependent_task_id IN
				SELECT task_id
				FROM task_dependency
				WHERE dependency_id = NEW.id
				LOOP

			UPDATE task
			SET
				num_dependencies_pending = num_dependencies_pending + _pending_diff
			WHERE id = _dependent_task_id;
		END LOOP;
	END IF;

	IF _cleaned_diff != 0 THEN
		-- TASK_DEPENDENCY_ACCESS
		PERFORM pg_advisory_xact_lock_shared(1001);

		FOR _dependent_task_id IN
				SELECT task_id
				FROM task_dependency
				WHERE dependency_id = NEW.id
				AND NOT soft
				LOOP

			UPDATE task
			SET
				num_dependencies_cleaned = num_dependencies_cleaned + _cleaned_diff
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

	-- If the task has been partially cleaned, make a note of this.
	IF NOT OLD.partial_cleaned AND NEW.partial_cleaned THEN
		INSERT INTO task_note_history (task_id, note_id, note_args)
		VALUES (NEW.id, _note_partial_clean_id, ARRAY[]::TEXT[]);
	END IF;

	-- If the task has been marked or unmarked for clean, make a note of this.
	IF NOT OLD.marked_for_clean AND NEW.marked_for_clean THEN
		INSERT INTO task_note_history (task_id, note_id, note_args)
		VALUES (NEW.id, _note_marked_for_clean_id, ARRAY[]::TEXT[]);
	ELSIF OLD.marked_for_clean AND NOT NEW.marked_for_clean THEN
		INSERT INTO task_note_history (task_id, note_id, note_args)
		VALUES (NEW.id, _note_unmarked_for_clean_id, ARRAY[]::TEXT[]);
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
