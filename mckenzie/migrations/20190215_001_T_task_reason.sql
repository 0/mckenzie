CREATE TABLE IF NOT EXISTS task_reason
(
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	description TEXT NOT NULL
);

INSERT INTO task_reason (name, description)
VALUES ('tr_task_add_new', 'Minted by "task add".')
ON CONFLICT (name) DO NOTHING;
