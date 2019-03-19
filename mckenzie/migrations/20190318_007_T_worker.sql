DROP TRIGGER IF EXISTS aftupd_worker
ON worker;

CREATE TRIGGER aftupd_worker
AFTER UPDATE
ON worker
FOR EACH ROW
EXECUTE PROCEDURE aftupd_worker();
