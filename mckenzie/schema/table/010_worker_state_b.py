from .worker import WorkerState


for state in sorted(WorkerState):
    tx.execute('''
        INSERT INTO worker_state (id, name)
        VALUES (%s, %s);
        ''', (state.value, state.name))
