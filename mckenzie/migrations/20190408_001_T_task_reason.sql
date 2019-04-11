INSERT INTO task_reason (name, description)
VALUES ('tr_task_clean', 'Cleaned by "task clean".')
ON CONFLICT (name) DO NOTHING;
