CREATE TABLE IF NOT EXISTS task_note
(
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	description_format TEXT NOT NULL,
	arg_types TEXT[] NOT NULL
);

INSERT INTO task_note (name, description_format, arg_types)
VALUES
	('tn_change_time', 'Changed time limit from {0} to {1}.', ARRAY['INTERVAL', 'INTERVAL']),
	('tn_change_mem', 'Changed memory limit from {0} MB to {1} MB.', ARRAY['INTEGER', 'INTEGER'])
ON CONFLICT (name) DO NOTHING;
