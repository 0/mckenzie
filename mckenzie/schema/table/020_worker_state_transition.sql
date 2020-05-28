{{{X from .worker import WorkerState}}}


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
	({{{V WorkerState.ws_queued}}}, {{{V WorkerState.ws_cancelled}}}),
	({{{V WorkerState.ws_queued}}}, {{{V WorkerState.ws_failed}}}),
	({{{V WorkerState.ws_queued}}}, {{{V WorkerState.ws_running}}}),
	({{{V WorkerState.ws_running}}}, {{{V WorkerState.ws_failed}}}),
	({{{V WorkerState.ws_running}}}, {{{V WorkerState.ws_quitting}}}),
	({{{V WorkerState.ws_quitting}}}, {{{V WorkerState.ws_running}}}),
	({{{V WorkerState.ws_quitting}}}, {{{V WorkerState.ws_failed}}}),
	({{{V WorkerState.ws_quitting}}}, {{{V WorkerState.ws_done}}}),
	({{{V WorkerState.ws_failed}}}, {{{V WorkerState.ws_done}}})
ON CONFLICT (from_state_id, to_state_id) DO NOTHING;
