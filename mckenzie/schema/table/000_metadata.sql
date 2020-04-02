CREATE TABLE metadata
(
	id SERIAL PRIMARY KEY,
	key TEXT UNIQUE NOT NULL,
	value TEXT NOT NULL
);

INSERT INTO metadata (key, value)
VALUES ('schema_version', '1');
