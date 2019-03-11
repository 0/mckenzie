INSERT INTO task_reason (name, description)
VALUES
	('tr_task_rerun', 'Reset by "task rerun".'),
	('tr_waiting_dep', 'New pending dependencies.')
ON CONFLICT (name) DO NOTHING;
