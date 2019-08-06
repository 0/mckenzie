INSERT INTO task_reason (name, description)
VALUES
	('tr_database_migration', 'Updated by database migration.'),
	('tr_task_add_waiting', 'Processed by "task add".'),
	('tr_task_add_held', 'Held by "task add".'),
	('tr_task_hold', 'Held by "task hold".'),
	('tr_task_release', 'Released by "task release".'),
	('tr_ready', 'No pending dependencies.')
ON CONFLICT (name) DO NOTHING;
