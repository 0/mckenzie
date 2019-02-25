from contextlib import ExitStack
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

        @self.db.tx
        def states(tx):
            return tx.execute('''
                    SELECT id, name
                    FROM task_state
                    ORDER BY id
                    ''')

        for state_id, name in states:
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

        @self.db.tx
        def reasons(tx):
            return tx.execute('''
                    SELECT id, name
                    FROM task_reason
                    ORDER BY id
                    ''')

        for reason_id, name in reasons:
            self._dict_r[name] = reason_id

    def rlookup(self, name):
        return self._dict_r[name]


class TaskClaimError(Exception):
    pass


class ClaimStack(ExitStack):
    def __init__(self, mgr, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.tx_f = mgr.db.tx
        self.claimed_by = mgr.ident

        self.claimed_ids = set()

    def add(self, task_id):
        if task_id in self.claimed_ids:
            return

        self.claimed_ids.add(task_id)

        def F(tx):
            TaskManager._unclaim(tx, task_id, self.claimed_by)

        self.callback(self.tx_f, F)


class TaskManager(Manager):
    @staticmethod
    def _claim(tx, task_id, claimed_by):
        success = tx.callproc('task_claim', (task_id, claimed_by))[0][0]

        if not success:
            raise TaskClaimError()

    @staticmethod
    def _unclaim(tx, task_id, claimed_by):
        success = tx.callproc('task_unclaim', (task_id, claimed_by))[0][0]

        if not success:
            raise TaskClaimError()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._ts = TaskState(self.db)
        self._tr = TaskReason(self.db)

    def summary(self, args):
        @self.db.tx
        def tasks(tx):
            return tx.execute('''
                    SELECT state_id, SUM(time_limit), COUNT(*)
                    FROM task
                    GROUP BY state_id
                    ''')

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

        @self.db.tx
        def F(tx):
            task = tx.execute('''
                    INSERT INTO task (name, state_id, priority, time_limit,
                                      mem_limit_mb)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (name) DO NOTHING
                    RETURNING id, task_claim(id, %s)
                    ''', (name, self._ts.rlookup('ts_new'), priority,
                          time_limit, mem_limit_mb, self.ident))

            if len(task) == 0:
                logger.error(f'Task "{name}" already exists.')
                tx.rollback()

                return

            task_id, _ = task[0]

            tx.execute('''
                    INSERT INTO task_history (task_id, state_id, reason_id)
                    VALUES (%s, %s, %s)
                    ''', (task_id, self._ts.rlookup('ts_new'),
                          self._tr.rlookup('tr_task_add_new')))

            self._unclaim(tx, task_id, self.ident)

    def list(self, args):
        state_name = args.state
        name_pattern = args.name_pattern

        if state_name is not None:
            try:
                state_id = self._ts.rlookup(state_name, user=True)
            except KeyError:
                logger.error(f'Invalid state "{state_name}".')

                return
        else:
            state_id = None

        @self.db.tx
        def tasks(tx):
            query = '''
                    SELECT name, state_id, priority, time_limit, mem_limit_mb
                    FROM task
                    '''
            query_args = ()

            where_added = False

            if state_id is not None:
                query += f' {"AND" if where_added else "WHERE"} state_id = %s'
                query_args += (state_id,)
                where_added = True

            if name_pattern is not None:
                query += f' {"AND" if where_added else "WHERE"} name LIKE %s'
                query_args += (name_pattern,)
                where_added = True

            query += ' ORDER BY id'

            return tx.execute(query, query_args)

        task_data = []

        for name, state_id, priority, time_limit, mem_limit in tasks:
            state_user = self._ts.lookup(state_id, user=True)

            task_data.append([name, state_user, priority, time_limit,
                              mem_limit])

        print_table(['Name', 'State', 'P', 'Time', 'Mem (MB)'], task_data)
