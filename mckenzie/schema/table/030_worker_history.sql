CREATE TABLE IF NOT EXISTS worker_history
(
	id SERIAL PRIMARY KEY,
	worker_id INTEGER NOT NULL REFERENCES worker,
	state_id INTEGER NOT NULL REFERENCES worker_state,
	time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
	reason_id INTEGER NOT NULL REFERENCES worker_reason
);


CREATE INDEX IF NOT EXISTS worker_history_worker_id
ON worker_history (worker_id);


DROP TRIGGER IF EXISTS aftins_worker_history
ON worker_history;

CREATE TRIGGER aftins_worker_history
AFTER INSERT
ON worker_history
FOR EACH ROW
EXECUTE PROCEDURE aftins_worker_history();
