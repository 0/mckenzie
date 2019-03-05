INSERT INTO task_reason (name, description)
VALUES ('tr_failure_worker_clean', 'Found by "worker clean".')
ON CONFLICT (name) DO NOTHING;
