{{{X from .task import TaskState}}}


CREATE TABLE IF NOT EXISTS task_state_transition
(
	id SERIAL PRIMARY KEY,
	from_state_id INTEGER NOT NULL REFERENCES task_state,
	to_state_id INTEGER NOT NULL REFERENCES task_state,
	-- Transition has no external requirements or associated actions.
	free_transition BOOLEAN NOT NULL,
	-- Change in "satisfies_dependency" between states.
	satisfies_dependency_diff INTEGER NOT NULL DEFAULT 0,
	-- Change in "satisfies_soft_dependency" between states.
	satisfies_soft_dependency_diff INTEGER NOT NULL DEFAULT 0,
	UNIQUE (from_state_id, to_state_id),
	CONSTRAINT different_states CHECK (from_state_id != to_state_id)
);


INSERT INTO task_state_transition (from_state_id, to_state_id, free_transition)
VALUES
	({{{V TaskState.ts_waiting}}}, {{{V TaskState.ts_held}}}, TRUE),
	-- Task must have no unsatisfied dependencies.
	({{{V TaskState.ts_waiting}}}, {{{V TaskState.ts_ready}}}, FALSE),
	({{{V TaskState.ts_held}}}, {{{V TaskState.ts_cancelled}}}, TRUE),
	({{{V TaskState.ts_held}}}, {{{V TaskState.ts_waiting}}}, TRUE),
	({{{V TaskState.ts_cancelled}}}, {{{V TaskState.ts_held}}}, TRUE),
	({{{V TaskState.ts_ready}}}, {{{V TaskState.ts_waiting}}}, TRUE),
	-- Task must be chosen by worker.
	({{{V TaskState.ts_ready}}}, {{{V TaskState.ts_running}}}, FALSE),
	-- Task must fail.
	({{{V TaskState.ts_running}}}, {{{V TaskState.ts_failed}}}, FALSE),
	-- Task must be cleaned and scrubbed.
	({{{V TaskState.ts_failed}}}, {{{V TaskState.ts_waiting}}}, FALSE),
	-- Task must succeed.
	({{{V TaskState.ts_running}}}, {{{V TaskState.ts_done}}}, FALSE),
	-- Task must be cleaned and scrubbed. Could result in dependencies no
	-- longer being satisfied.
	({{{V TaskState.ts_done}}}, {{{V TaskState.ts_waiting}}}, FALSE),
	-- Task must be synthesized.
	({{{V TaskState.ts_done}}}, {{{V TaskState.ts_synthesized}}}, FALSE),
	({{{V TaskState.ts_synthesized}}}, {{{V TaskState.ts_cleanable}}}, TRUE),
	({{{V TaskState.ts_cleanable}}}, {{{V TaskState.ts_synthesized}}}, TRUE),
	-- Could result in hard dependencies no longer being satisfied.
	({{{V TaskState.ts_cleanable}}}, {{{V TaskState.ts_cleaning}}}, FALSE),
	-- Task must be cleaned.
	({{{V TaskState.ts_cleaning}}}, {{{V TaskState.ts_cleaned}}}, FALSE),
	-- Task must be unsynthesized and scrubbed. Could result in soft
	-- dependencies no longer being satisfied.
	({{{V TaskState.ts_cleaned}}}, {{{V TaskState.ts_waiting}}}, FALSE)
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


-- Populate dependency changes.
UPDATE task_state_transition tst
SET satisfies_dependency_diff
	= (SELECT ts.satisfies_dependency FROM task_state ts WHERE ts.id = tst.to_state_id)::INTEGER
		- (SELECT ts.satisfies_dependency FROM task_state ts WHERE ts.id = tst.from_state_id)::INTEGER;

UPDATE task_state_transition tst
SET satisfies_soft_dependency_diff
	= (SELECT ts.satisfies_soft_dependency FROM task_state ts WHERE ts.id = tst.to_state_id)::INTEGER
		- (SELECT ts.satisfies_soft_dependency FROM task_state ts WHERE ts.id = tst.from_state_id)::INTEGER;
