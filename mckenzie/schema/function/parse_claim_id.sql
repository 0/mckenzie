CREATE OR REPLACE FUNCTION parse_claim_id(
	_claimed_by INTEGER
)
RETURNS INTEGER AS $$
DECLARE
	_agent_type_id INTEGER := (_claimed_by & (((1 << 8) - 1) << 24)) >> 24;
BEGIN
	IF _claimed_by IS NULL THEN
		RETURN NULL;
	ELSIF _agent_type_id = 1 THEN
		RETURN _claimed_by & ((1 << 16) - 1);
	ELSIF _agent_type_id = 2 THEN
		RETURN _claimed_by & ((1 << 24) - 1);
	ELSE
		RAISE EXCEPTION 'Unrecognized agent type in claim.';
	END IF;
END;
$$ LANGUAGE plpgsql;
