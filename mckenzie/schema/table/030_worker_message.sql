CREATE TABLE IF NOT EXISTS worker_message
(
	id SERIAL PRIMARY KEY,
	worker_id INTEGER NOT NULL REFERENCES worker,
	time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
	type_id INTEGER NOT NULL,
	args JSONB NOT NULL,
	read_at TIMESTAMP WITH TIME ZONE
);


CREATE INDEX IF NOT EXISTS worker_message_worker_id_unread
ON worker_message (worker_id) WHERE read_at IS NULL;
