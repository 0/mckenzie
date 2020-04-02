CREATE TABLE IF NOT EXISTS worker_note
(
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	description_format TEXT NOT NULL,
	arg_types TEXT[] NOT NULL
);


INSERT INTO worker_note (name, description_format, arg_types)
VALUES ('wn_quitting', 'Quitting.', ARRAY[]::TEXT[])
ON CONFLICT (name) DO NOTHING;
