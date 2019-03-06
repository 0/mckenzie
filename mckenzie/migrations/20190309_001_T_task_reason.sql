INSERT INTO task_reason (name, description)
VALUES ('tr_task_reset_failed', 'Reset by "task reset-failed".')
ON CONFLICT (name) DO NOTHING;
