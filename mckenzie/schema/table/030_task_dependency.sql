CREATE TABLE IF NOT EXISTS task_dependency
(
	id SERIAL PRIMARY KEY,
	task_id INTEGER NOT NULL REFERENCES task,
	dependency_id INTEGER NOT NULL REFERENCES task,
	soft BOOLEAN NOT NULL DEFAULT FALSE,
	UNIQUE (task_id, dependency_id),
	CONSTRAINT self_dependency CHECK (task_id != dependency_id)
);


-- For the LEFT JOIN in the cleanable_tasks subquery for the "task clean"
-- command, when selecting a task in ts_cleanable.
CREATE INDEX IF NOT EXISTS task_dependency_dependency_id_soft
ON task_dependency (dependency_id, soft);


DROP TRIGGER IF EXISTS aftins_task_dependency
ON task_dependency;

CREATE TRIGGER aftins_task_dependency
AFTER INSERT
ON task_dependency
FOR EACH ROW
EXECUTE PROCEDURE aftins_task_dependency();
