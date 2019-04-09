INSERT INTO task_note (name, description_format, arg_types)
VALUES ('tn_partial_clean', 'Partially cleaned.', ARRAY[]::TEXT[])
ON CONFLICT (name) DO NOTHING;
