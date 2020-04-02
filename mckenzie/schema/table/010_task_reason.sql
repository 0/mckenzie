CREATE TABLE IF NOT EXISTS task_reason
(
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	description TEXT NOT NULL
);


INSERT INTO task_reason (name, description)
VALUES
	('tr_task_add_new', 'Minted by "task add".'),
	('tr_task_add_waiting', 'Processed by "task add".'),
	('tr_task_add_held', 'Held by "task add".'),
	('tr_task_hold', 'Held by "task hold".'),
	('tr_task_release', 'Released by "task release".'),
	('tr_ready', 'No pending dependencies.'),
	('tr_running', 'Picked up by worker.'),
	('tr_success', 'Successfully finished.'),
	('tr_failure_exit_code', 'Exited with non-zero status.'),
	('tr_failure_string', 'Ended with invalid string.'),
	('tr_failure_abort', 'Worker exited before task finished.'),
	('tr_failure_worker_clean', 'Found by "worker clean".'),
	('tr_task_reset_failed', 'Reset by "task reset-failed".'),
	('tr_task_rerun', 'Reset by "task rerun".'),
	('tr_waiting_dep', 'New pending dependencies.'),
	('tr_failure_run', 'Failed to run task.'),
	('tr_failure_memory', 'Exceeded memory limit.'),
	('tr_task_clean', 'Cleaned by "task clean".'),
	('tr_task_cancel', 'Cancelled by "task cancel".'),
	('tr_task_uncancel', 'Uncancelled by "task uncancel".'),
	('tr_limit_retry', 'Retrying with extended limit.')
ON CONFLICT (name) DO NOTHING;
