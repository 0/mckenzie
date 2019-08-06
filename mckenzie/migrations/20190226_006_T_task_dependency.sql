CREATE TABLE IF NOT EXISTS task_dependency
(
	id SERIAL PRIMARY KEY,
	task_id INTEGER NOT NULL REFERENCES task,
	dependency_id INTEGER NOT NULL REFERENCES task,
	UNIQUE (task_id, dependency_id),
	CONSTRAINT self_dependency CHECK (task_id != dependency_id)
);


DROP TRIGGER IF EXISTS aftins_task_dependency
ON task_dependency;

CREATE TRIGGER aftins_task_dependency
AFTER INSERT
ON task_dependency
FOR EACH ROW
EXECUTE PROCEDURE aftins_task_dependency();
