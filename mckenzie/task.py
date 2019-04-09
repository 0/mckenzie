from contextlib import ExitStack
from datetime import timedelta
import logging
import os
import signal
import subprocess
from threading import Event

from .base import (DatabaseNoteView, DatabaseReasonView, DatabaseStateView,
                   Manager)
from .database import AdvisoryKey, CheckViolation
from .util import DirectedAcyclicGraphNode as DAG
from .util import check_proc, format_datetime, format_timedelta


logger = logging.getLogger(__name__)


# Task state flow:
#
#       held <----+<--------+
#       ^  |      ^         ^
#       |  v      |         |
# new ->+->+-> waiting -> ready -> running -> done -> cleaned
#                 ^                   |        |         |
#                 |                   v        |         |
#                 +<-------------- failed      |         |
#                 ^                            |         |
#                 |                            |         |
#                 +<---------------------------+---------+


class TaskState(DatabaseStateView):
    def __init__(self, *args, **kwargs):
        super().__init__('task_state', 'ts_', *args, **kwargs)


class TaskReason(DatabaseReasonView):
    def __init__(self, *args, **kwargs):
        super().__init__('task_reason', *args, **kwargs)


class TaskNote(DatabaseNoteView):
    def __init__(self, *args, **kwargs):
        super().__init__('task_note', 'task_note_history', *args, **kwargs)


class TaskClaimError(Exception):
    def __init__(self, task_id, claimed_by):
        super().__init__()

        self.task_id = task_id
        self.claimed_by = claimed_by


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
    # 1 minute
    SYNTHESIZE_WAIT_SECONDS = 60

    STATE_ORDER = ['new', 'held', 'waiting', 'ready', 'running', 'failed (!)',
                   'done (!)', 'done', 'cleaned (!)', 'cleaned']

    @staticmethod
    def _claim(tx, task_id, claimed_by):
        success = tx.callproc('task_claim', (task_id, claimed_by))[0][0]

        if not success:
            raise TaskClaimError(task_id, claimed_by)

    @staticmethod
    def _unclaim(tx, task_id, claimed_by, *, force=False):
        success = tx.callproc('task_unclaim',
                              (task_id, claimed_by, force))[0][0]

        if not success:
            raise TaskClaimError(task_id, claimed_by)

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
        self._tn = TaskNote(self.db)

    def _format_state(self, state_id, synthesized):
        state = self._ts.lookup(state_id)
        state_user = self._ts.lookup(state_id, user=True)
        color = None

        if state == 'ts_failed':
            state_user += ' (!)'
            color = self.c('error')
        elif state == 'ts_done' and not synthesized:
            state_user += ' (!)'
            color = self.c('warning')
        elif state == 'ts_cleaned' and not synthesized:
            state_user += ' (!)'
            color = self.c('error')

        return state, state_user, color

    def _run_cmd(self, cmd, *args):
        if cmd is None:
            return True

        proc = subprocess.run((cmd,) + args, cwd=self.conf.general_chdir,
                              capture_output=True, text=True)

        return check_proc(proc, log=logger.error)

    def _clean(self, task_name, *, partial=False):
        args = []

        if partial:
            args.append('--partial')

        args.append(task_name)

        return self._run_cmd(self.conf.task_cleanup_cmd, *args)

    def _unsynthesize(self, task_name):
        return self._run_cmd(self.conf.task_unsynthesize_cmd, task_name)

    def summary(self, args):
        @self.db.tx
        def tasks(tx):
            return tx.execute('''
                    SELECT state_id, synthesized, SUM(time_limit),
                           SUM(elapsed_time), COUNT(*)
                    FROM task
                    GROUP BY state_id, synthesized
                    ''')

        if not tasks:
            logger.info('No tasks found.')

            return

        task_data = []

        for state_id, synthesized, time_limit, elapsed_time, count in tasks:
            state, state_user, state_color \
                    = self._format_state(state_id, synthesized)

            if state in ['ts_done', 'ts_cleaned']:
                time = elapsed_time
            else:
                time = time_limit

            task_data.append([(state_user, state_color), count, time])

        sorted_data = sorted(task_data,
                             key=lambda row: self.STATE_ORDER.index(row[0][0]))
        self.print_table(['State', 'Count', 'Total time'], sorted_data,
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

            with tx.advisory(AdvisoryKey.TASK_DEPENDENCY_ACCESS):
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

    def clean(self, args):
        allow_unsynthesized = args.allow_unsynthesized
        ignore_pending_dependents = args.ignore_pending_dependents
        partial = args.partial
        names = args.name

        if self.conf.task_cleanup_cmd is None:
            logger.warning('No cleanup command defined.')

            return

        for task_name in names:
            @self.db.tx
            def task(tx):
                return tx.execute('''
                        SELECT id, state_id, synthesized, task_claim(id, %s)
                        FROM task
                        WHERE name = %s
                        ''', (self.ident, task_name))

            if len(task) == 0:
                logger.warning(f'Task "{task_name}" does not exist.')

                continue

            task_id, state_id, synthesized, claim_success = task[0]
            state = self._ts.lookup(state_id)
            state_user = self._ts.lookup(state_id, user=True)

            if not claim_success:
                logger.warning(f'Task "{task_name}" could not be claimed.')

                continue

            with ClaimStack(self) as cs:
                cs.add(task_id)

                if state == 'ts_running':
                    logger.warning(f'Task "{task_name}" is in state '
                                   f'"{state_user}".')

                    continue

                if state == 'ts_done':
                    if not allow_unsynthesized and not synthesized:
                        logger.warning(f'Task "{task_name}" is not '
                                       'synthesized.')

                        continue

                with self.db.advisory(AdvisoryKey.TASK_DEPENDENCY_ACCESS,
                                      shared=True):
                    if not partial and state == 'ts_done':
                        @self.db.tx
                        def task_direct_dependents(tx):
                            return tx.execute('''
                                    SELECT td.task_id,
                                           task_claim(td.task_id, %s),
                                           ts.pending
                                    FROM task_dependency td
                                    JOIN task t ON t.id = td.task_id
                                    JOIN task_state ts ON ts.id = t.state_id
                                    WHERE td.dependency_id = %s
                                    ''', (self.ident, task_id))

                        dependent_any_claim_failed = False
                        dependent_any_pending = False

                        for (dependent_id, dependent_claim_success,
                                dependent_pending) in task_direct_dependents:
                            if dependent_claim_success:
                                cs.add(dependent_id)
                            else:
                                dependent_any_claim_failed = True

                            if dependent_pending:
                                dependent_any_pending = True

                        if dependent_any_claim_failed:
                            logger.warning('Failed to claim dependent of task '
                                           f'"{task_name}".')

                            continue

                        if (not ignore_pending_dependents
                                and dependent_any_pending):
                            logger.warning(f'Task "{task_name}" has pending '
                                           'dependents.')

                            continue

                    logger.info(task_name)

                    if state != 'ts_cleaned':
                        if not self._clean(task_name, partial=partial):
                            return

                    if state == 'ts_done':
                        if partial:
                            @self.db.tx
                            def F(tx):
                                tx.execute('''
                                        UPDATE task
                                        SET partial_cleaned = TRUE
                                        WHERE id = %s
                                        ''', (task_id,))
                        else:
                            @self.db.tx
                            def F(tx):
                                tx.execute('''
                                        INSERT INTO task_history (task_id,
                                                                  state_id,
                                                                  reason_id)
                                        VALUES (%s, %s, %s)
                                        ''', (task_id,
                                              self._ts.rlookup('ts_cleaned'),
                                              self._tr.rlookup('tr_task_clean')))

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
                logger.debug(f'Holding task "{task_name}".')

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
                           num_dependencies, num_dependencies_pending,
                           num_dependencies_cleaned, synthesized
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
                num_dep_pend, num_dep_clean, synthesized) in tasks:
            state, state_user, state_color \
                    = self._format_state(state_id, synthesized)

            if num_dep > 0:
                dep = f'{num_dep-num_dep_pend-num_dep_clean}/{num_dep}'

                if num_dep_pend + num_dep_clean > 0:
                    dep = (dep, self.c('notice'))
            else:
                dep = ''

            task_data.append([name, (state_user, state_color), dep, priority,
                              time_limit, mem_limit])

        self.print_table(['Name', 'State', 'Dep', 'P', 'Time', 'Mem (MB)'],
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

        self.print_table(['Name', 'State', 'Claimed by', 'Since', 'For'],
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
                logger.debug(f'Releasing task "{task_name}".')

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

    def rerun(self, args):
        allow_no_cleanup = args.allow_no_cleanup
        allow_no_unsynthesize = args.allow_no_unsynthesize
        task_name = args.name

        if self.conf.task_cleanup_cmd is None:
            if not allow_no_cleanup:
                logger.error('No cleanup command defined.')

                return

        if self.conf.task_unsynthesize_cmd is None:
            if not allow_no_unsynthesize:
                logger.error('No unsynthesis command defined.')

                return

        @self.db.tx
        def task(tx):
            return tx.execute('''
                    SELECT id, state_id, synthesized
                    FROM task
                    WHERE name = %s
                    ''', (task_name,))

        if len(task) == 0:
            logger.error(f'Task "{task_name}" does not exist.')

            return

        task_id, state_id, synthesized = task[0]
        state = self._ts.lookup(state_id)
        state_user = self._ts.lookup(state_id, user=True)

        if state not in ['ts_done', 'ts_cleaned']:
            logger.error(f'Task "{task_name}" is in state "{state_user}".')

            return

        target_data = task_id, task_name, state_id, synthesized

        task_names = {task_id: task_name}

        with self.db.advisory(AdvisoryKey.TASK_DEPENDENCY_ACCESS, shared=True):
            # Recursively collect and claim all tasks which have the named task
            # as a dependency.
            try:
                @self.db.tx
                def task_graph(tx):
                    result = DAG(target_data)
                    task_nodes = {target_data[0]: result}
                    task_queue = [result]
                    idx = 0

                    while idx < len(task_queue):
                        cur_node = task_queue[idx]
                        cur_id, *_ = cur_node.data

                        new_tasks = tx.execute('''
                                SELECT t.id, t.name, t.state_id, t.synthesized
                                FROM task t
                                JOIN task_dependency td ON td.task_id = t.id
                                WHERE td.dependency_id = %s
                                ''', (cur_id,))

                        for (task_id, task_name, state_id,
                                synthesized) in new_tasks:
                            task_names[task_id] = task_name

                            self._claim(tx, task_id, self.ident)

                            try:
                                new_node = task_nodes[task_id]
                            except KeyError:
                                new_node = DAG((task_id, task_name, state_id,
                                                synthesized))
                                task_nodes[task_id] = new_node

                            cur_node.add(new_node)
                            task_queue.append(new_node)

                        idx += 1

                    return result
            except TaskClaimError as e:
                task_name = task_names[e.task_id]
                logger.error(f'Task "{task_name}" could not be claimed.')

                return

            task_list = list(task_graph)

            with ClaimStack(self) as cs:
                for task_id, *_ in task_list:
                    cs.add(task_id)

                if len(task_list) == 1:
                    logger.debug('Rerunning 1 task.')
                else:
                    logger.debug(f'Rerunning {len(task_list)} tasks.')

                for task_id, task_name, state_id, synthesized in task_list:
                    state = self._ts.lookup(state_id)
                    state_user = self._ts.lookup(state_id, user=True)

                    if state == 'ts_running':
                        logger.error(f'Task "{task_name}" is in state '
                                     f'"{state_user}".')

                        return

                    if state not in ['ts_failed', 'ts_done', 'ts_cleaned']:
                        continue

                    logger.info(task_name)

                    if synthesized:
                        logger.debug(f'Unsynthesizing task "{task_name}".')

                        if not self._unsynthesize(task_name):
                            return

                        @self.db.tx
                        def F(tx):
                            tx.execute('''
                                    UPDATE task
                                    SET synthesized = FALSE
                                    WHERE id = %s
                                    ''', (task_id,))

                    logger.debug(f'Cleaning task "{task_name}".')

                    if not self._clean(task_name):
                        return

                    logger.debug(f'Changing state of task "{task_name}".')

                    @self.db.tx
                    def F(tx):
                        tx.execute('''
                                INSERT INTO task_history (task_id, state_id,
                                                          reason_id)
                                VALUES (%s, %s, %s)
                                ''', (task_id,
                                      self._ts.rlookup('ts_waiting'),
                                      self._tr.rlookup('tr_task_rerun')))

    def reset_claimed(self, args):
        names = args.name

        for task_name in names:
            logger.debug(f'Resetting claim on "{task_name}".')

            @self.db.tx
            def task(tx):
                return tx.execute('''
                        SELECT id
                        FROM task
                        WHERE name = %s
                        ''', (task_name,))

            if len(task) == 0:
                logger.warning(f'Task "{task_name}" does not exist.')

                continue

            task_id, = task[0]

            @self.db.tx
            def F(tx):
                self._unclaim(tx, task_id, None, force=True)

            logger.info(task_name)

    def reset_failed(self, args):
        skip_clean = args.skip_clean

        with ClaimStack(self) as cs:
            @self.db.tx
            def tasks(tx):
                return tx.execute('''
                        WITH failed_tasks AS (
                            SELECT id, name
                            FROM task
                            WHERE state_id = %s
                            FOR UPDATE
                        )
                        SELECT id, name
                        FROM failed_tasks
                        WHERE task_claim(id, %s)
                        ''', (self._ts.rlookup('ts_failed'), self.ident))

            for task_id, task_name in tasks:
                cs.add(task_id)

            for task_id, task_name in tasks:
                logger.info(task_name)

                if not skip_clean and not self._clean(task_name):
                    return

                @self.db.tx
                def F(tx):
                    tx.execute('''
                            INSERT INTO task_history (task_id, state_id,
                                                      reason_id)
                            VALUES (%s, %s, %s)
                            ''', (task_id, self._ts.rlookup('ts_waiting'),
                                  self._tr.rlookup('tr_task_reset_failed')))

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
            self.print_table(['Time', 'Duration', 'State', 'Reason', 'Worker'],
                             task_data)
        else:
            print('No state history.')

        print()

        @self.db.tx
        def task_note_history(tx):
            return tx.execute('''
                    SELECT id, note_id, time
                    FROM task_note_history
                    WHERE task_id = %s
                    ORDER BY id
                    ''', (task_id,))

        task_data = []

        for history_id, note_id, time in task_note_history:
            note_desc = self._tn.format(history_id, note_id)

            task_data.append([time, note_desc])

        if task_data:
            self.print_table(['Time', 'Note'], task_data)
        else:
            print('No notes.')

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
            self.print_table(['Worker', 'Active at', 'Inactive at',
                              'Duration'],
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
            self.print_table(['Dependency', 'State'], task_data)
        else:
            print('No dependencies.')

        print()

        @self.db.tx
        def task_direct_dependents(tx):
            return tx.execute('''
                    SELECT COUNT(*)
                    FROM task_dependency td
                    WHERE td.dependency_id = %s
                    ''', (task_id,))

        num_direct_dependents, = task_direct_dependents[0]

        @self.db.tx
        def task_recursive_dependents(tx):
            return tx.execute('''
                    WITH RECURSIVE deps(id) AS (
                        SELECT task_id
                        FROM task_dependency
                        WHERE dependency_id = %s
                    UNION
                        SELECT td.task_id
                        FROM deps
                        JOIN task_dependency td ON td.dependency_id = deps.id
                    )
                    SELECT COUNT(*) FROM deps
                    ''', (task_id,))

        num_recursive_dependents, = task_recursive_dependents[0]

        if num_recursive_dependents == 1:
            print('1 dependent.')
        elif num_recursive_dependents > 1:
            print(f'{num_recursive_dependents} dependents ({num_direct_dependents} direct).')
        else:
            print('No dependents.')

        print()

        if claimed_by is not None:
            print(f'Claimed by "{self._parse_claim(claimed_by)}" since '
                  f'{format_datetime(claimed_since)} '
                  f'(for {format_timedelta(claimed_for)}).')
        else:
            print('Not claimed.')

    def synthesize(self, args):
        forever = args.forever

        if self.conf.task_synthesize_cmd is None:
            logger.warning('No synthesis command defined.')

            return

        done = Event()

        def quit(signum=None, frame=None):
            logger.debug('Quitting.')
            done.set()

        signal.signal(signal.SIGINT, quit)

        while not done.is_set():
            logger.debug('Selecting next task.')

            @self.db.tx
            def task(tx):
                return tx.execute('''
                        WITH unsynthesized_task AS (
                            SELECT id, name, elapsed_time, max_mem_mb
                            FROM task
                            WHERE state_id = %s
                            AND NOT synthesized
                            AND NOT partial_cleaned
                            AND claimed_by IS NULL
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        SELECT id, name, elapsed_time, max_mem_mb,
                               task_claim(id, %s)
                        FROM unsynthesized_task
                        ''', (self._ts.rlookup('ts_done'), self.ident))

            if len(task) == 0:
                logger.debug('No tasks found.')

                if forever:
                    logger.debug('Taking a break.')
                    done.wait(self.SYNTHESIZE_WAIT_SECONDS)
                else:
                    quit()

                continue

            (task_id, task_name, elapsed_time, max_mem_mb,
                    claim_success) = task[0]

            if not claim_success:
                logger.debug(f'Failed to claim task "{task_name}".')

                continue

            with ClaimStack(self) as cs:
                cs.add(task_id)

                logger.info(task_name)

                elapsed_time_hours = elapsed_time.total_seconds() / 3600
                max_mem_gb = max_mem_mb / 1024

                logger.debug('Running synthesis command.')

                proc = subprocess.run([self.mck.conf.task_synthesize_cmd,
                                       task_name, str(elapsed_time_hours),
                                       str(max_mem_gb)],
                                      cwd=self.conf.general_chdir,
                                      capture_output=True, text=True,
                                      # Prevent the child process from
                                      # receiving any signals sent to this
                                      # process.
                                      preexec_fn=os.setpgrp)

                if not check_proc(proc, log=logger.error):
                    return

                @self.db.tx
                def F(tx):
                    tx.execute('''
                            UPDATE task
                            SET synthesized = TRUE
                            WHERE id = %s
                            ''', (task_id,))

        logger.debug('Exited cleanly.')

    def unsynthesize(self, args):
        names = args.name

        if self.conf.task_unsynthesize_cmd is None:
            logger.warning('No unsynthesis command defined.')

            return

        for task_name in names:
            @self.db.tx
            def task(tx):
                return tx.execute('''
                        SELECT id, name, synthesized, task_claim(id, %s)
                        FROM task
                        WHERE name = %s
                        ''', (self.ident, task_name))

            if len(task) == 0:
                logger.warning(f'Task "{task_name}" does not exist.')

                continue

            task_id, task_name, synthesized, claim_success = task[0]

            if not claim_success:
                logger.warning(f'Task "{task_name}" could not be claimed.')

                continue

            with ClaimStack(self) as cs:
                cs.add(task_id)

                if not synthesized:
                    logger.warning(f'Task "{task_name}" is not synthesized.')

                    continue

                logger.info(task_name)

                if not self._unsynthesize(task_name):
                    return

                @self.db.tx
                def F(tx):
                    tx.execute('''
                            UPDATE task
                            SET synthesized = FALSE
                            WHERE id = %s
                            ''', (task_id,))
