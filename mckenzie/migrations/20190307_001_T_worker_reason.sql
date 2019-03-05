INSERT INTO worker_reason (name, description)
VALUES ('wr_success_idle', 'Ran out of tasks.')
ON CONFLICT (name) DO NOTHING;
