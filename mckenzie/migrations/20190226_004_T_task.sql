ALTER TABLE task
DROP CONSTRAINT IF EXISTS num_dependencies;

ALTER TABLE task
ADD COLUMN IF NOT EXISTS num_dependencies INTEGER NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS num_dependencies_pending INTEGER NOT NULL DEFAULT 0,
ADD CONSTRAINT num_dependencies CHECK (num_dependencies_pending <= num_dependencies);


DROP TRIGGER IF EXISTS aftupd_task
ON task;

CREATE TRIGGER aftupd_task
AFTER UPDATE
ON task
FOR EACH ROW
EXECUTE PROCEDURE aftupd_task();
