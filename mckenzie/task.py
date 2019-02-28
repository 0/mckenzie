from datetime import timedelta
import logging

from .base import DatabaseView, Manager
from .util import print_table


logger = logging.getLogger(__name__)


class TaskState(DatabaseView):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Mapping from IDs to names.
        self._dict_f = {}
        # Mapping from names to IDs.
        self._dict_r = {}

        def F(tx):
            return tx.execute('''
                    SELECT id, name
                    FROM task_state
                    ORDER BY id
                    ''')

        for state_id, name in self.db.tx(F):
            self._dict_f[state_id] = name
            self._dict_r[name] = state_id

    def lookup(self, state_id, *, user=False):
        name = self._dict_f[state_id]

        if user:
            name = name[3:]

        return name

    def rlookup(self, name, *, user=False):
        if user:
            name = 'ts_' + name

        return self._dict_r[name]


class TaskReason(DatabaseView):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Mapping from names to IDs.
        self._dict_r = {}

        def F(tx):
            return tx.execute('''
                    SELECT id, name
                    FROM task_reason
                    ORDER BY id
                    ''')

        for reason_id, name in self.db.tx(F):
            self._dict_r[name] = reason_id

    def rlookup(self, name):
        return self._dict_r[name]


class TaskManager(Manager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._ts = TaskState(self.db)
        self._tr = TaskReason(self.db)

    def summary(self, args):
        def F(tx):
            return tx.execute('''
                    SELECT state_id, SUM(time_limit), COUNT(*)
                    FROM task
                    GROUP BY state_id
                    ''')

        tasks = self.db.tx(F)

        if not tasks:
            logger.info('No tasks found.')

            return

        task_data = []

        for state_id, time_limit, count in tasks:
            state_user = self._ts.lookup(state_id, user=True)

            task_data.append([state_user, count, time_limit])

        print_table(['State', 'Count', 'Total time'], task_data,
                    total=('Total', (1, 2)))

    def add(self, args):
        time_limit = timedelta(hours=args.time)
        mem_limit_mb = args.mem * 1024
        priority = args.priority
        name = args.name

        if ' ' in name:
            logger.error('Task name cannot contain spaces.')

            return

        def F(tx):
            task = tx.execute('''
                    INSERT INTO task (name, state_id, priority, time_limit,
                                      mem_limit_mb)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (name) DO NOTHING
                    RETURNING id
                    ''', (name, self._ts.rlookup('ts_new'), priority,
                          time_limit, mem_limit_mb))

            if len(task) == 0:
                logger.error(f'Task "{name}" already exists.')
                tx.rollback()

                return

            task_id, = task[0]
            tx.execute('''
                    INSERT INTO task_history (task_id, state_id, reason_id)
                    VALUES (%s, %s, %s)
                    ''', (task_id, self._ts.rlookup('ts_new'),
                          self._tr.rlookup('tr_task_add_new')))

        self.db.tx(F)

    def list(self, args):
        state_name = args.state

        if state_name is not None:
            try:
                state_id = self._ts.rlookup(state_name, user=True)
            except KeyError:
                logger.error(f'Invalid state "{state_name}".')

                return
        else:
            state_id = None

        def F(tx):
            query = '''
                    SELECT name, state_id, priority, time_limit, mem_limit_mb
                    FROM task
                    '''
            query_args = ()

            if state_id is not None:
                query += ' WHERE state_id = %s'
                query_args += (state_id,)

            query += ' ORDER BY id'

            return tx.execute(query, query_args)

        tasks = self.db.tx(F)

        task_data = []

        for name, state_id, priority, time_limit, mem_limit in tasks:
            state_user = self._ts.lookup(state_id, user=True)

            task_data.append([name, state_user, priority, time_limit,
                              mem_limit])

        print_table(['Name', 'State', 'P', 'Time', 'Mem (MB)'], task_data)
