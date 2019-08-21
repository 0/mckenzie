INSERT INTO task_note (name, description_format, arg_types)
VALUES
	('tn_marked_for_clean', 'Marked for clean.', ARRAY[]::TEXT[]),
	('tn_unmarked_for_clean', 'Unmarked for clean.', ARRAY[]::TEXT[])
ON CONFLICT (name) DO NOTHING;
