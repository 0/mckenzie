CREATE TABLE IF NOT EXISTS worker_reason
(
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	description TEXT NOT NULL
);

INSERT INTO worker_reason (name, description)
VALUES
	('wr_worker_spawn', 'Minted by "worker spawn".'),
	('wr_start', 'Started running.'),
	('wr_success', 'Exited normally.'),
	('wr_success_abort', 'Aborted.'),
	('wr_failure', 'Exited unexpectedly.'),
	('wr_worker_quit_cancelled', 'Cancelled by "worker quit".')
ON CONFLICT (name) DO NOTHING;
