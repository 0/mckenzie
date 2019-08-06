tasks = tx.execute('''
        SELECT id
        FROM task
        WHERE state_id = %s
        ''', (ts.rlookup('ts_new'),))

for task_id, in tasks:
    tx.execute('''
            INSERT INTO task_history (task_id, state_id, reason_id)
            VALUES (%s, %s, %s)
            ''', (task_id, ts.rlookup('ts_waiting'),
                  tr.rlookup('tr_database_migration')))
