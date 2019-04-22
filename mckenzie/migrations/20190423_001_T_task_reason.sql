INSERT INTO task_reason (name, description)
VALUES ('tr_limit_retry', 'Retrying with extended limit.')
ON CONFLICT (name) DO NOTHING;
