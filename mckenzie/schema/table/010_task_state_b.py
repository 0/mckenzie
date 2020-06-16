from .task import TaskState


for state in sorted(TaskState):
    tx.execute('''
        INSERT INTO task_state (id, name)
        VALUES (%s, %s);
        ''', (state.value, state.name))
