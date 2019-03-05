INSERT INTO worker_reason (name, description)
VALUES
	('wr_worker_clean_queued', 'Found "queued" by "worker clean".'),
	('wr_worker_clean_running', 'Found "running" by "worker clean".')
ON CONFLICT (name) DO NOTHING;
