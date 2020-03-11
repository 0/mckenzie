CREATE TABLE IF NOT EXISTS task_state
(
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	-- Tasks in the state must be handled manually.
	exceptional BOOLEAN NOT NULL DEFAULT FALSE,
	-- Tasks in the state have run successfully and not been cleaned.
	satisfies_dependency BOOLEAN NOT NULL DEFAULT FALSE,
	-- Tasks in the state have run successfully, but may have been cleaned.
	satisfies_soft_dependency BOOLEAN NOT NULL DEFAULT FALSE,
	-- Tasks in the state are expected to complete successfully in the future.
	pending BOOLEAN NOT NULL DEFAULT TRUE,
	-- Tasks in the state have not yet run successfully.
	incomplete BOOLEAN NOT NULL DEFAULT TRUE
);


INSERT INTO task_state (name)
VALUES
	('ts_cancelled'),
	('ts_held'),
	('ts_waiting'),
	('ts_ready'),
	('ts_running'),
	('ts_failed'),
	('ts_done'),
	('ts_synthesized'),
	('ts_cleanable'),
	('ts_cleaning'),
	('ts_cleaned')
ON CONFLICT (name) DO NOTHING;

UPDATE task_state
SET exceptional = TRUE
WHERE name IN ('ts_cancelled', 'ts_held', 'ts_failed');

UPDATE task_state
SET
	satisfies_dependency = TRUE,
	satisfies_soft_dependency = TRUE
WHERE name IN ('ts_done', 'ts_synthesized', 'ts_cleanable');

UPDATE task_state
SET satisfies_soft_dependency = TRUE
WHERE name IN ('ts_cleaning', 'ts_cleaned');

UPDATE task_state
SET pending = FALSE
WHERE name = 'ts_cancelled'
OR satisfies_soft_dependency;

UPDATE task_state
SET incomplete = FALSE
WHERE satisfies_soft_dependency;


ALTER TABLE task_state
-- satisfies_dependency -> satisfies_soft_dependency
ADD CONSTRAINT hard_is_soft CHECK (NOT satisfies_dependency OR satisfies_soft_dependency),
-- pending -> incomplete
ADD CONSTRAINT pending_is_incomplete CHECK (NOT pending OR incomplete),
-- satisfies_soft_dependency -> NOT incomplete
ADD CONSTRAINT soft_is_not_incomplete CHECK (NOT (satisfies_soft_dependency AND incomplete));
