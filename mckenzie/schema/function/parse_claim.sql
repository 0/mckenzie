CREATE OR REPLACE FUNCTION parse_claim(
	_claimed_by INTEGER
)
RETURNS TEXT AS $$
BEGIN
	IF _claimed_by IS NULL THEN
		RETURN NULL;
	ELSE
		RETURN parse_claim_type(_claimed_by) || ' ' || parse_claim_id(_claimed_by);
	END IF;
END;
$$ LANGUAGE plpgsql;
