CREATE TABLE IF NOT EXISTS task_state
(
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	-- Tasks in the state are not expected to leave in a hurry.
	terminal BOOLEAN NOT NULL DEFAULT FALSE,
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
