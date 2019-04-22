INSERT INTO task_reason (name, description)
VALUES
	('tr_task_cancel', 'Cancelled by "task cancel".'),
	('tr_task_uncancel', 'Uncancelled by "task uncancel".')
ON CONFLICT (name) DO NOTHING;
