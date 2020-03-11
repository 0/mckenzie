CREATE TABLE IF NOT EXISTS task_reason
(
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	description TEXT NOT NULL
);


INSERT INTO task_reason (name, description)
VALUES
	('tr_task_add', 'Minted by "task add".'),
	('tr_task_cancel', 'Cancelled by "task cancel".'),
	('tr_task_uncancel', 'Uncancelled by "task uncancel".'),
	('tr_task_hold', 'Held by "task hold".'),
	('tr_task_release', 'Released by "task release".'),
	('tr_ready', 'No incomplete dependencies.'),
	('tr_waiting_dep', 'New incomplete dependencies.'),
	('tr_running', 'Picked up by worker.'),
	('tr_failure_exit_code', 'Exited with non-zero status.'),
	('tr_failure_string', 'Ended with invalid string.'),
	('tr_failure_abort', 'Worker exited before task finished.'),
	('tr_failure_worker_clean', 'Found by "worker clean".'),
	('tr_failure_run', 'Failed to run task.'),
	('tr_failure_memory', 'Exceeded memory limit.'),
	('tr_limit_retry', 'Automatically reset with extended limit.'),
	('tr_task_reset_failed', 'Reset by "task reset-failed".'),
	('tr_success', 'Successfully finished running.'),
	('tr_task_synthesize', 'Synthesized by "task synthesize".'),
	('tr_task_cleanablize', 'Marked as cleanable by "task cleanablize".'),
	('tr_task_uncleanablize', 'Unmarked as cleanable by "task uncleanablize".'),
	('tr_task_clean_cleaning', 'Prepared for cleaning by "task clean".'),
	('tr_task_clean_cleaned', 'Cleaned by "task clean".'),
	('tr_task_rerun_synthesize', 'Synthesized by "task rerun".'),
	('tr_task_rerun_cleanablize', 'Marked as cleanable by "task rerun".'),
	('tr_task_rerun_cleaning', 'Prepared for cleaning by "task rerun".'),
	('tr_task_rerun_cleaned', 'Cleaned by "task rerun".'),
	('tr_task_rerun_reset', 'Reset by "task rerun".')
ON CONFLICT (name) DO NOTHING;
