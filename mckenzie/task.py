from contextlib import ExitStack
from datetime import timedelta
import logging

from .base import DatabaseReasonView, DatabaseStateView, Manager
from .database import CheckViolation
from .util import format_datetime, format_timedelta, print_table


logger = logging.getLogger(__name__)


# Task state flow:
#
#       held <----+<--------+
#       ^  |      ^         ^
#       |  v      |         |
# new ->+->+-> waiting -> ready -> running -> done
#                                     |
#                                     v
#                                  failed


class TaskState(DatabaseStateView):
    def __init__(self, *args, **kwargs):
        super().__init__('task_state', 'ts_', *args, **kwargs)


class TaskReason(DatabaseReasonView):
    def __init__(self, *args, **kwargs):
        super().__init__('task_reason', *args, **kwargs)


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

    @staticmethod
    def _parse_claim(ident):
        agent_type_id = (ident & (0xff << 24)) >> 24

        if agent_type_id == 0x01:
            agent_type = 'manager'
            agent_id = ident & 0xffff
        elif agent_type_id == 0x02:
            agent_type = 'worker'
            agent_id = ident & 0xffffff
        else:
            agent_type = '???'
            agent_id = ident

        if agent_id is not None:
            return f'{agent_type} {agent_id}'
        else:
            return agent_type

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
        held = args.held
        time_limit = timedelta(hours=args.time)
        mem_limit_mb = args.mem * 1024
        priority = args.priority
        depends_on = args.depends_on
        name = args.name

        if held:
            state_id = self._ts.rlookup('ts_held')
            reason_id = self._tr.rlookup('tr_task_add_held')
        else:
            state_id = self._ts.rlookup('ts_waiting')
            reason_id = self._tr.rlookup('tr_task_add_waiting')

        if depends_on is not None:
            dependency_names = set(depends_on)
        else:
            dependency_names = set()

        @self.db.tx
        def F(tx):
            try:
                task = tx.execute('''
                        INSERT INTO task (name, state_id, priority, time_limit,
                                          mem_limit_mb)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (name) DO NOTHING
                        RETURNING id, task_claim(id, %s)
                        ''', (name, self._ts.rlookup('ts_new'), priority,
                              time_limit, mem_limit_mb, self.ident))
            except CheckViolation as e:
                if e.constraint_name == 'name_spaces':
                    logger.error('Task name cannot contain spaces.')
                    tx.rollback()

                    return
                else:
                    raise

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

            for dependency_name in dependency_names:
                dependency = tx.execute('''
                        SELECT id
                        FROM task
                        WHERE name = %s
                        ''', (dependency_name,))

                if len(dependency) == 0:
                    logger.error(f'No such task "{dependency_name}".')
                    tx.rollback()

                    return

                dependency_id, = dependency[0]

                try:
                    tx.execute('''
                            INSERT INTO task_dependency (task_id,
                                                         dependency_id)
                            VALUES (%s, %s)
                            ''', (task_id, dependency_id))
                except CheckViolation as e:
                    if e.constraint_name == 'self_dependency':
                        logger.error('Task cannot depend on itself.')
                        tx.rollback()

                        return
                    else:
                        raise

            tx.execute('''
                    INSERT INTO task_history (task_id, state_id, reason_id)
                    VALUES (%s, %s, %s)
                    ''', (task_id, state_id, reason_id))

            self._unclaim(tx, task_id, self.ident)

    def hold(self, args):
        all_tasks = args.all
        names = args.name

        task_names = {name: True for name in names}

        with ClaimStack(self) as cs:
            if all_tasks:
                @self.db.tx
                def tasks(tx):
                    # All holdable, claimable tasks.
                    return tx.execute('''
                            WITH holdable_tasks AS (
                                SELECT t.id, t.name
                                FROM task t
                                JOIN task_state ts ON ts.id = t.state_id
                                WHERE ts.holdable
                                FOR UPDATE OF t
                            )
                            SELECT id, name
                            FROM holdable_tasks
                            WHERE task_claim(id, %s)
                            ''', (self.ident,))

                for task_id, task_name in tasks:
                    cs.add(task_id)

                    if task_name not in task_names:
                        task_names[task_name] = False

            for task_name, requested in task_names.items():
                @self.db.tx
                def task(tx):
                    return tx.execute('''
                            SELECT t.id, t.state_id, ts.holdable,
                                   task_claim(t.id, %s)
                            FROM task t
                            JOIN task_state ts ON ts.id = t.state_id
                            WHERE t.name = %s
                            ''', (self.ident, task_name))

                if len(task) == 0:
                    logger.warning(f'Task "{task_name}" does not exist.')

                    continue

                task_id, state_id, holdable, claim_success = task[0]
                state = self._ts.lookup(state_id)
                state_user = self._ts.lookup(state_id, user=True)

                if claim_success:
                    cs.add(task_id)
                else:
                    if requested:
                        logger.warning(f'Task "{task_name}" could not be '
                                        'claimed.')

                    continue

                if not holdable:
                    if requested:
                        if state == 'ts_held':
                            logger.warning(f'Task "{task_name}" is already '
                                           'held.')
                        else:
                            logger.warning(f'Task "{task_name}" is in state '
                                           f'"{state_user}".')

                    continue

                @self.db.tx
                def F(tx):
                    tx.execute('''
                            INSERT INTO task_history (task_id, state_id,
                                                      reason_id)
                            VALUES (%s, %s, %s)
                            ''', (task_id, self._ts.rlookup('ts_held'),
                                  self._tr.rlookup('tr_task_hold')))

                logger.info(task_name)

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
                    SELECT name, state_id, priority, time_limit, mem_limit_mb,
                           num_dependencies, num_dependencies_pending
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

        for (name, state_id, priority, time_limit, mem_limit, num_dep,
                num_dep_pend) in tasks:
            state_user = self._ts.lookup(state_id, user=True)

            dep = f'{num_dep-num_dep_pend}/{num_dep}'

            task_data.append([name, state_user, dep, priority, time_limit,
                              mem_limit])

        print_table(['Name', 'State', 'Dep', 'P', 'Time', 'Mem (MB)'],
                    task_data)

    def list_claimed(self, args):
        @self.db.tx
        def tasks(tx):
            return tx.execute('''
                    SELECT name, state_id, claimed_by, claimed_since,
                           NOW() - claimed_since
                    FROM task
                    WHERE claimed_by IS NOT NULL
                    ORDER BY NOW() - claimed_since
                    ''')

        if not tasks:
            logger.info('No claimed tasks found.')

            return

        task_data = []

        for name, state_id, claimed_by, claimed_since, claimed_for in tasks:
            state_user = self._ts.lookup(state_id, user=True)
            agent = self._parse_claim(claimed_by)

            task_data.append([name, state_user, agent, claimed_since,
                              claimed_for])

        print_table(['Name', 'State', 'Claimed by', 'Since', 'For'],
                    task_data)

    def release(self, args):
        all_tasks = args.all
        names = args.name

        task_names = {name: True for name in names}

        with ClaimStack(self) as cs:
            if all_tasks:
                @self.db.tx
                def tasks(tx):
                    # All held, claimable tasks.
                    return tx.execute('''
                            WITH held_tasks AS (
                                SELECT id, name
                                FROM task
                                WHERE state_id = %s
                                FOR UPDATE
                            )
                            SELECT id, name
                            FROM held_tasks
                            WHERE task_claim(id, %s)
                            ''', (self._ts.rlookup('ts_held'), self.ident))

                for task_id, task_name in tasks:
                    cs.add(task_id)

                    if task_name not in task_names:
                        task_names[task_name] = False

            for task_name, requested in task_names.items():
                @self.db.tx
                def task(tx):
                    return tx.execute('''
                            SELECT id, state_id, task_claim(id, %s)
                            FROM task
                            WHERE name = %s
                            ''', (self.ident, task_name))

                if len(task) == 0:
                    logger.warning(f'Task "{task_name}" does not exist.')

                    continue

                task_id, state_id, claim_success = task[0]
                state = self._ts.lookup(state_id)
                state_user = self._ts.lookup(state_id, user=True)

                if claim_success:
                    cs.add(task_id)
                else:
                    if requested:
                        logger.warning(f'Task "{task_name}" could not be '
                                       'claimed.')

                    continue

                if state != 'ts_held':
                    if requested:
                        logger.warning(f'Task "{task_name}" is in state '
                                       f'"{state_user}".')

                    continue

                @self.db.tx
                def F(tx):
                    tx.execute('''
                            INSERT INTO task_history (task_id, state_id,
                                                      reason_id)
                            VALUES (%s, %s, %s)
                            ''', (task_id, self._ts.rlookup('ts_waiting'),
                                  self._tr.rlookup('tr_task_release')))

                logger.info(task_name)

    def show(self, args):
        name = args.name

        @self.db.tx
        def task(tx):
            return tx.execute('''
                    SELECT id, claimed_by, claimed_since, NOW() - claimed_since
                    FROM task
                    WHERE name = %s
                    ''', (name,))

        if len(task) == 0:
            logger.error(f'Task "{name}" does not exist.')

            return

        task_id, claimed_by, claimed_since, claimed_for = task[0]

        @self.db.tx
        def task_history(tx):
            return tx.execute('''
                    SELECT state_id, time,
                           LEAD(time, 1, NOW()) OVER (ORDER BY time, id),
                           reason_id, worker_id
                    FROM task_history
                    WHERE task_id = %s
                    ORDER BY id
                    ''', (task_id,))

        task_data = []

        for state_id, time, time_next, reason_id, worker_id in task_history:
            state_user = self._ts.lookup(state_id, user=True)
            reason_desc = self._tr.dlookup(reason_id)

            duration = time_next - time

            if worker_id is not None:
                worker_id = str(worker_id)
            else:
                worker_id = ''

            task_data.append([time, duration, state_user, reason_desc,
                              worker_id])

        if task_data:
            print_table(['Time', 'Duration', 'State', 'Reason', 'Worker'],
                        task_data)
        else:
            print('No state history.')

        print()

        @self.db.tx
        def worker_task(tx):
            return tx.execute('''
                    SELECT worker_id, time_active, time_inactive,
                           NOW() - time_active
                    FROM worker_task
                    WHERE task_id = %s
                    ORDER BY id
                    ''', (task_id,))

        task_data = []

        for worker_id, time_active, time_inactive, time_since in worker_task:
            if time_inactive is not None:
                duration = time_inactive - time_active
            else:
                duration = time_since

            task_data.append([str(worker_id), time_active, time_inactive,
                              duration])

        if task_data:
            print_table(['Worker', 'Active at', 'Inactive at', 'Duration'],
                        task_data)
        else:
            print('No worker activity.')

        print()

        @self.db.tx
        def task_dependency(tx):
            return tx.execute('''
                    SELECT t.name, t.state_id
                    FROM task_dependency td
                    JOIN task t ON t.id = td.dependency_id
                    WHERE td.task_id = %s
                    ORDER BY t.id
                    ''', (task_id,))

        task_data = []

        for dependency_name, state_id in task_dependency:
            state_user = self._ts.lookup(state_id, user=True)

            task_data.append([dependency_name, state_user])

        if task_data:
            print_table(['Dependency', 'State'], task_data)
        else:
            print('No dependencies.')

        print()

        if claimed_by is not None:
            print(f'Claimed by "{self._parse_claim(claimed_by)}" since '
                  f'{format_datetime(claimed_since)} '
                  f'(for {format_timedelta(claimed_for)}).')
        else:
            print('Not claimed.')
