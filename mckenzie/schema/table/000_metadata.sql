CREATE TABLE IF NOT EXISTS metadata
(
	id SERIAL PRIMARY KEY,
	key TEXT NOT NULL UNIQUE,
	value TEXT NOT NULL
);

INSERT INTO metadata (key, value)
VALUES ('schema_version', '{{{V Database.SCHEMA_VERSION}}}');
