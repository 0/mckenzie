INSERT INTO task_reason (name, description)
VALUES ('tr_failure_run', 'Failed to run task.')
ON CONFLICT (name) DO NOTHING;
