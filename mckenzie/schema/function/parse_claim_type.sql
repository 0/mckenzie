CREATE OR REPLACE FUNCTION parse_claim_type(
	_claimed_by INTEGER
)
RETURNS TEXT AS $$
DECLARE
	_agent_type_id INTEGER := (_claimed_by & (((1 << 8) - 1) << 24)) >> 24;
BEGIN
	IF _claimed_by IS NULL THEN
		RETURN NULL;
	ELSIF _agent_type_id = 1 THEN
		RETURN 'manager';
	ELSIF _agent_type_id = 2 THEN
		RETURN 'worker';
	ELSE
		RAISE EXCEPTION 'Unrecognized agent type in claim.';
	END IF;
END;
$$ LANGUAGE plpgsql;
