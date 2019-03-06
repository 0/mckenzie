INSERT INTO task_reason (name, description)
VALUES
	('tr_running', 'Picked up by worker.'),
	('tr_success', 'Successfully finished.'),
	('tr_failure_exit_code', 'Exited with non-zero status.'),
	('tr_failure_string', 'Ended with invalid string.'),
	('tr_failure_abort', 'Worker exited before task finished.')
ON CONFLICT (name) DO NOTHING;
