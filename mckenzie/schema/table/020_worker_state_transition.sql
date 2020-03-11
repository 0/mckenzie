CREATE TABLE IF NOT EXISTS worker_state_transition
(
	id SERIAL PRIMARY KEY,
	from_state_id INTEGER NOT NULL REFERENCES worker_state,
	to_state_id INTEGER NOT NULL REFERENCES worker_state,
	UNIQUE (from_state_id, to_state_id),
	CONSTRAINT different_states CHECK (from_state_id != to_state_id)
);


INSERT INTO worker_state_transition (from_state_id, to_state_id)
VALUES
	((SELECT id FROM worker_state WHERE name = 'ws_queued'),
		(SELECT id FROM worker_state WHERE name = 'ws_cancelled')),
	((SELECT id FROM worker_state WHERE name = 'ws_queued'),
		(SELECT id FROM worker_state WHERE name = 'ws_failed')),
	((SELECT id FROM worker_state WHERE name = 'ws_queued'),
		(SELECT id FROM worker_state WHERE name = 'ws_running')),
	((SELECT id FROM worker_state WHERE name = 'ws_running'),
		(SELECT id FROM worker_state WHERE name = 'ws_failed')),
	((SELECT id FROM worker_state WHERE name = 'ws_running'),
		(SELECT id FROM worker_state WHERE name = 'ws_quitting')),
	((SELECT id FROM worker_state WHERE name = 'ws_quitting'),
		(SELECT id FROM worker_state WHERE name = 'ws_failed')),
	((SELECT id FROM worker_state WHERE name = 'ws_quitting'),
		(SELECT id FROM worker_state WHERE name = 'ws_done')),
	((SELECT id FROM worker_state WHERE name = 'ws_failed'),
		(SELECT id FROM worker_state WHERE name = 'ws_done'))
ON CONFLICT (from_state_id, to_state_id) DO NOTHING;
