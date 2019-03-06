INSERT INTO task_state (name, holdable)
VALUES
	('ts_running', FALSE),
	('ts_done', FALSE),
	('ts_failed', FALSE)
ON CONFLICT (name) DO NOTHING;
