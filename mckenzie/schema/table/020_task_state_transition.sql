CREATE TABLE IF NOT EXISTS task_state_transition
(
	id SERIAL PRIMARY KEY,
	from_state_id INTEGER NOT NULL REFERENCES task_state,
	to_state_id INTEGER NOT NULL REFERENCES task_state,
	-- Transition has no external requirements or associated actions.
	free_transition BOOLEAN NOT NULL,
	UNIQUE (from_state_id, to_state_id),
	CONSTRAINT different_states CHECK (from_state_id != to_state_id)
);


INSERT INTO task_state_transition (from_state_id, to_state_id, free_transition)
VALUES
	((SELECT id FROM task_state WHERE name = 'ts_held'),
		(SELECT id FROM task_state WHERE name = 'ts_cancelled'),
		TRUE),
	((SELECT id FROM task_state WHERE name = 'ts_cancelled'),
		(SELECT id FROM task_state WHERE name = 'ts_held'),
		TRUE),
	((SELECT id FROM task_state WHERE name = 'ts_held'),
		(SELECT id FROM task_state WHERE name = 'ts_waiting'),
		TRUE),
	((SELECT id FROM task_state WHERE name = 'ts_waiting'),
		(SELECT id FROM task_state WHERE name = 'ts_held'),
		TRUE),
	((SELECT id FROM task_state WHERE name = 'ts_waiting'),
		(SELECT id FROM task_state WHERE name = 'ts_ready'),
		-- Task must have no incomplete dependencies.
		FALSE),
	((SELECT id FROM task_state WHERE name = 'ts_ready'),
		(SELECT id FROM task_state WHERE name = 'ts_waiting'),
		TRUE),
	((SELECT id FROM task_state WHERE name = 'ts_ready'),
		(SELECT id FROM task_state WHERE name = 'ts_running'),
		-- Task must be chosen by worker.
		FALSE),
	((SELECT id FROM task_state WHERE name = 'ts_running'),
		(SELECT id FROM task_state WHERE name = 'ts_failed'),
		-- Task must fail.
		FALSE),
	((SELECT id FROM task_state WHERE name = 'ts_failed'),
		(SELECT id FROM task_state WHERE name = 'ts_waiting'),
		-- Task must be cleaned.
		FALSE),
	((SELECT id FROM task_state WHERE name = 'ts_running'),
		(SELECT id FROM task_state WHERE name = 'ts_done'),
		-- Task must succeed.
		FALSE),
	((SELECT id FROM task_state WHERE name = 'ts_done'),
		(SELECT id FROM task_state WHERE name = 'ts_synthesized'),
		-- Task must be synthesized.
		FALSE),
	((SELECT id FROM task_state WHERE name = 'ts_synthesized'),
		(SELECT id FROM task_state WHERE name = 'ts_cleanable'),
		TRUE),
	((SELECT id FROM task_state WHERE name = 'ts_cleanable'),
		(SELECT id FROM task_state WHERE name = 'ts_synthesized'),
		TRUE),
	((SELECT id FROM task_state WHERE name = 'ts_cleanable'),
		(SELECT id FROM task_state WHERE name = 'ts_cleaning'),
		-- Could result in hard dependencies no longer being satisfied.
		FALSE),
	((SELECT id FROM task_state WHERE name = 'ts_cleaning'),
		(SELECT id FROM task_state WHERE name = 'ts_cleaned'),
		-- Task must be cleaned.
		FALSE),
	((SELECT id FROM task_state WHERE name = 'ts_cleaned'),
		(SELECT id FROM task_state WHERE name = 'ts_waiting'),
		-- Task must be unsynthesized. Could result in soft dependencies no
		-- longer being satisfied.
		FALSE)
ON CONFLICT (from_state_id, to_state_id) DO NOTHING;


-- Add endpoints for all possible paths that involve only free transitions.
WITH RECURSIVE paths(from_state_id, to_state_id, free_transition) AS (
	SELECT from_state_id, to_state_id, free_transition
	FROM task_state_transition
	WHERE free_transition
UNION
	SELECT paths.from_state_id, tst.to_state_id, TRUE
	FROM paths
	JOIN task_state_transition tst ON tst.from_state_id = paths.to_state_id
	WHERE tst.free_transition
	AND paths.from_state_id != tst.to_state_id
)
INSERT INTO task_state_transition (from_state_id, to_state_id, free_transition)
SELECT * FROM paths
ON CONFLICT (from_state_id, to_state_id) DO NOTHING;
