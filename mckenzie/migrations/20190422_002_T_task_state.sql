INSERT INTO task_state (name, holdable, pending)
VALUES ('ts_cancelled', FALSE, TRUE)
ON CONFLICT (name) DO NOTHING;
