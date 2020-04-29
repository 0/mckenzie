UPDATE worker_state
SET job_exists = TRUE
WHERE name IN ('ws_queued', 'ws_running', 'ws_quitting');

UPDATE worker_state
SET job_running = TRUE
WHERE name IN ('ws_running', 'ws_quitting');
