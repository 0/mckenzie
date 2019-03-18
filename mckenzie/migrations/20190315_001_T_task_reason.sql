INSERT INTO task_reason (name, description)
VALUES ('tr_failure_memory', 'Exceeded memory limit.')
ON CONFLICT (name) DO NOTHING;
