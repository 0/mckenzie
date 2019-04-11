ALTER TABLE task
DROP CONSTRAINT IF EXISTS num_dependencies;

ALTER TABLE task
ADD COLUMN IF NOT EXISTS num_dependencies_cleaned INTEGER NOT NULL DEFAULT 0,
ADD CONSTRAINT num_dependencies CHECK (num_dependencies_pending + num_dependencies_cleaned <= num_dependencies);
