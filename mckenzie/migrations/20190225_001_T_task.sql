ALTER TABLE task
DROP CONSTRAINT IF EXISTS name_spaces;

ALTER TABLE task
ADD CONSTRAINT name_spaces CHECK (name NOT LIKE '% %');
