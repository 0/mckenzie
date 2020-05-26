{{{X from .task import TaskState, TaskReason}}}


CREATE OR REPLACE FUNCTION aftupd_task()
RETURNS trigger AS $$
DECLARE
	_satisfies_dependency_diff INTEGER;
	_satisfies_soft_dependency_diff INTEGER;
	_dependent_task_id INTEGER;
BEGIN
	SELECT satisfies_dependency_diff INTO _satisfies_dependency_diff
	FROM task_state_transition
	WHERE from_state_id = OLD.state_id
	AND to_state_id = NEW.state_id;

	SELECT satisfies_soft_dependency_diff INTO _satisfies_soft_dependency_diff
	FROM task_state_transition
	WHERE from_state_id = OLD.state_id
	AND to_state_id = NEW.state_id;

	-- If the task is in "waiting" and has no unsatisfied dependencies, move it
	-- to "ready", and vice versa. We make sure not to touch the task if it's
	-- claimed. When it is later unclaimed, this trigger will run again
	-- automatically, and we will make another attempt if it's still relevant.
	IF NEW.state_id = {{{V TaskState.ts_waiting}}}
			AND NEW.num_dependencies_satisfied = NEW.num_dependencies
			AND NEW.claimed_by IS NULL THEN

		INSERT INTO task_history (task_id, state_id, reason_id)
		VALUES (NEW.id, {{{V TaskState.ts_ready}}}, {{{V TaskReason.tr_ready}}});
	ELSIF NEW.state_id = {{{V TaskState.ts_ready}}}
			AND NEW.num_dependencies_satisfied != NEW.num_dependencies
			AND NEW.claimed_by IS NULL THEN

		INSERT INTO task_history (task_id, state_id, reason_id)
		VALUES (NEW.id, {{{V TaskState.ts_waiting}}}, {{{V TaskReason.tr_waiting_dep}}});
	END IF;

	-- If the task has stopped running, mark it as inactive.
	IF OLD.state_id = {{{V TaskState.ts_running}}}
			AND NEW.state_id != {{{V TaskState.ts_running}}} THEN

		UPDATE worker_task
		SET active = FALSE
		WHERE task_id = NEW.id
		AND active;
	END IF;

	-- If this task has started or ceased satisfying dependencies, update all
	-- the tasks that depend on it.
	IF _satisfies_dependency_diff IS NOT NULL
			AND _satisfies_dependency_diff != 0 THEN

		-- TASK_DEPENDENCY_ACCESS
		PERFORM pg_advisory_xact_lock_shared(1001, (NEW.id & ((1::bigint << 31) - 1))::integer);

		FOR _dependent_task_id IN
				SELECT task_id
				FROM task_dependency
				WHERE dependency_id = NEW.id
				AND NOT soft
				LOOP

			UPDATE task
			SET num_dependencies_satisfied = num_dependencies_satisfied + _satisfies_dependency_diff
			WHERE id = _dependent_task_id;
		END LOOP;
	END IF;

	IF _satisfies_soft_dependency_diff IS NOT NULL
			AND _satisfies_soft_dependency_diff != 0 THEN

		-- TASK_DEPENDENCY_ACCESS
		PERFORM pg_advisory_xact_lock_shared(1001, (NEW.id & ((1::bigint << 31) - 1))::integer);

		FOR _dependent_task_id IN
				SELECT task_id
				FROM task_dependency
				WHERE dependency_id = NEW.id
				AND soft
				LOOP

			UPDATE task
			SET num_dependencies_satisfied = num_dependencies_satisfied + _satisfies_soft_dependency_diff
			WHERE id = _dependent_task_id;
		END LOOP;
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
