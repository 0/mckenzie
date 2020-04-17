from contextlib import ExitStack
from datetime import timedelta
from heapq import heappop, heappush
import logging
import os
import subprocess

from .base import (DatabaseNoteView, DatabaseReasonView, DatabaseStateView,
                   Manager)
from .database import AdvisoryKey, CheckViolation, RaisedException
from .util import DirectedAcyclicGraphNode as DAG
from .util import (HandledException, check_proc, format_datetime,
                   format_timedelta)


logger = logging.getLogger(__name__)


# Task state flow:
#
#    cancelled   cleaned <-------- cleaning
#       ^  :        |                  ^
#       :  v        |                  |
#       held        |              cleanable
#       ^  :        |                 ^ :
#       :  v        v                 : v
# --> waiting <-----+<-- failed   synthesized
#       ^  |               ^           ^
#       :  v               |           |
#      ready ---------> running ---> done
#
# waiting: The task is waiting for its dependencies to finish before it can
#          run.
# held: The task has been held and will not be selected to run even if its
#       dependencies have completed. The task will likely eventually be
#       released.
# cancelled: Same as held, only more so. The task will likely never be
#            uncancelled.
# ready: The task may be selected to run by a worker.
# running: The task is currently being run by a worker. It's possible that the
#          worker has exited abnormally without changing the task's state.
# failed: The task was being run, but failed to complete.
# done: The task was run successfully.
# synthesized: Same as done, but the task's data have been synthesized.
# cleanable: Same as synthesized, but the task is eligible for cleaning.
# cleaning: The task is currently in the process of being cleaned.
# cleaned: The task has been cleaned.


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
    def __init__(self, mgr, *args, init_task_ids=None, **kwargs):
        super().__init__(*args, **kwargs)

        self.tx_f = mgr.db.tx
        self.claimed_by = mgr.ident

        self.claimed_ids = set()

        if init_task_ids is not None:
            for task_id in init_task_ids:
                self.add(task_id)

    def add(self, task_id):
        if task_id in self.claimed_ids:
            return

        self.claimed_ids.add(task_id)

        def F(tx):
            TaskManager._unclaim(tx, task_id, self.claimed_by)

        self.callback(self.tx_f, F)


class TaskManager(Manager):
    STATE_ORDER = ['cancelled', 'held', 'waiting', 'ready', 'running',
                   'failed', 'done', 'synthesized', 'cleanable', 'cleaning',
                   'cleaned']

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

    @staticmethod
    def _run_cmd(chdir, cmd, *args):
        kwargs = {
            'cwd': chdir,
            'capture_output': True,
            'text': True,
            # Prevent the child process from receiving any signals sent to this
            # process.
            'preexec_fn': os.setpgrp,
        }

        proc = subprocess.run((cmd,) + args, **kwargs)

        return check_proc(proc, log=logger.error)

    @classmethod
    def _clean(cls, conf, task_name):
        return cls._run_cmd(conf.general_chdir, conf.task_clean_cmd, task_name)

    @classmethod
    def _scrub(cls, conf, task_name):
        return cls._run_cmd(conf.general_chdir, conf.task_scrub_cmd, task_name)

    @classmethod
    def _synthesize(cls, conf, task_name, elapsed_time_hours, max_mem_gb):
        return cls._run_cmd(conf.general_chdir, conf.task_synthesize_cmd,
                            task_name, elapsed_time_hours, max_mem_gb)

    @classmethod
    def _unsynthesize(cls, conf, task_name):
        return cls._run_cmd(conf.general_chdir, conf.task_unsynthesize_cmd,
                            task_name)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._ts = TaskState(self.db)
        self._tr = TaskReason(self.db)
        self._tn = TaskNote(self.db)

    def _format_state(self, state_id):
        state = self._ts.lookup(state_id)
        state_user = self._ts.lookup(state_id, user=True)
        color = None

        if state == 'ts_failed':
            color = self.c('error')
        elif state in ['ts_waiting', 'ts_ready']:
            color = self.c('notice')
        elif state == 'ts_running':
            color = self.c('good')
        elif state in ['ts_held', 'ts_done', 'ts_cleanable', 'ts_cleaning']:
            color = self.c('warning')

        return state, state_user, color

    def _parse_state(self, state_name):
        if state_name is None:
            return None

        try:
            return self._ts.rlookup(state_name, user=True)
        except KeyError:
            logger.error(f'Invalid state "{state_name}".')

            raise HandledException()

    def _simple_state_change(self, from_state_id, to_state_id, reason_id,
                             name_pattern, names):
        task_names = {name: True for name in names}

        with ClaimStack(self) as cs:
            if name_pattern is not None:
                @self.db.tx
                def tasks(tx):
                    query = '''
                            WITH all_tasks AS (
                                SELECT t.id, t.name
                                FROM task t
                                JOIN task_state ts ON ts.id = t.state_id
                                JOIN task_state_transition tst ON tst.from_state_id = t.state_id
                                WHERE t.name LIKE %s
                            '''
                    query_args = (name_pattern,)

                    if from_state_id is not None:
                        query += 'AND t.state_id = %s'
                        query_args += (from_state_id,)
                    else:
                        query += 'AND NOT ts.exceptional'

                    query += '''
                                AND tst.to_state_id = %s
                                AND tst.free_transition
                                FOR UPDATE OF t SKIP LOCKED
                            )
                            SELECT id, name
                            FROM all_tasks
                            WHERE task_claim(id, %s)
                            '''
                    query_args += (to_state_id, self.ident)

                    return tx.execute(query, query_args)

                for task_id, task_name in tasks:
                    cs.add(task_id)

                    if task_name not in task_names:
                        task_names[task_name] = False

            for task_name, requested in task_names.items():
                if self.mck.interrupted.is_set():
                    break

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
                state_user = self._ts.lookup(state_id, user=True)

                if not claim_success:
                    if requested:
                        logger.warning(f'Task "{task_name}" could not be '
                                       'claimed.')

                    continue

                cs.add(task_id)

                @self.db.tx
                def success(tx):
                    try:
                        tx.execute('''
                                INSERT INTO task_history (task_id, state_id,
                                                          reason_id)
                                VALUES (%s, %s, %s)
                                ''', (task_id, to_state_id, reason_id))
                    except RaisedException as e:
                        if e.message == 'Invalid state transition.':
                            if requested:
                                logger.warning(f'Task "{task_name}" is in '
                                               f'state "{state_user}".')
                            tx.rollback()

                            return False
                        else:
                            raise

                    return True

                if not success:
                    continue

                logger.info(task_name)

    def _build_rerun_task_list(self, cs, locked_dependency_keys, target_data):
        """
        Recursively collect and claim all tasks which have the target task as a
        dependency, stopping at incomplete tasks. An incomplete task doesn't
        need to be rerun (nor do its dependents), but it still needs to be
        claimed so that it doesn't try to run while we're messing around with
        its dependencies.
        """

        task_id, *_ = target_data

        task_graph = DAG(target_data)
        task_nodes = {task_id: task_graph}

        # We would like a max-heap, but heapq provides a min-heap. Due to the
        # modulo, we know the maximum priority we could have, so we flip the
        # values. Also due to the modulo, we're not guaranteed that we will
        # obtain all the locks in the desired order.
        task_queue = [((1<<31) - task_id % (1<<31), task_graph)]

        while task_queue:
            _, cur_node = heappop(task_queue)
            cur_id, cur_name, *_ = cur_node.data

            dependency_key = cur_id % (1<<31)
            locked_dependency_keys.append(dependency_key)

            @self.db.tx
            def new_tasks(tx):
                tx.advisory_lock(AdvisoryKey.TASK_DEPENDENCY_ACCESS,
                                 dependency_key, shared=True)

                return tx.execute('''
                        SELECT t.id, t.name, t.state_id, ts.incomplete,
                               t.elapsed_time, t.max_mem_mb,
                               task_claim(t.id, %s)
                        FROM task t
                        JOIN task_state ts ON ts.id = t.state_id
                        JOIN task_dependency td ON td.task_id = t.id
                        WHERE td.dependency_id = %s
                        ''', (self.ident, cur_id))

            any_claim_failed = False

            for task_id, task_name, *_, claim_success in new_tasks:
                if claim_success:
                    cs.add(task_id)
                else:
                    any_claim_failed = True

            if any_claim_failed:
                logger.error(f'Dependent of task "{cur_name}" could not be '
                             'claimed.')

                raise HandledException()

            for task_id, task_name, *rest, claim_success in new_tasks:
                state_id, incomplete, *_ = rest

                if incomplete:
                    # Tasks that haven't been completed yet don't need to be
                    # included in the graph, since there's no need to rerun
                    # them or inspect their dependents.
                    continue

                try:
                    new_node = task_nodes[task_id]
                except KeyError:
                    new_node = DAG((task_id, task_name, *rest))
                    task_nodes[task_id] = new_node

                cur_node.add(new_node)
                new_id = (1<<31) - task_id % (1<<31)
                heappush(task_queue, (new_id, new_node))

        return list(task_graph)

    def summary(self, args):
        @self.db.tx
        def tasks(tx):
            return tx.execute('''
                    SELECT t.state_id, SUM(t.time_limit), SUM(t.elapsed_time),
                           ts.incomplete, COUNT(*)
                    FROM task t
                    JOIN task_state ts ON ts.id = t.state_id
                    GROUP BY t.state_id, ts.incomplete
                    ''')

        if not tasks:
            logger.info('No tasks found.')

            return

        task_data = []

        for state_id, time_limit, elapsed_time, incomplete, count in tasks:
            state, state_user, state_color = self._format_state(state_id)

            if incomplete:
                time = time_limit
            else:
                time = elapsed_time

            task_data.append([(state_user, state_color), count, time])

        sorted_data = sorted(task_data,
                             key=lambda row: self.STATE_ORDER.index(row[0][0]))
        self.print_table(['State', 'Count', 'Total time'], sorted_data,
                         total=('Total', (1, 2)))

    def add(self, args):
        time_limit = timedelta(hours=args.time_hr)
        mem_limit_mb = args.mem_gb * 1024
        priority = args.priority
        depends_on = args.depends_on
        soft_depends_on = args.soft_depends_on
        name = args.name

        if depends_on is not None:
            dependency_names = set(depends_on)
        else:
            dependency_names = set()

        if soft_depends_on is not None:
            soft_dependency_names = set(soft_depends_on)
        else:
            soft_dependency_names = set()

        @self.db.tx
        def F(tx):
            try:
                task = tx.execute('''
                        INSERT INTO task (name, state_id, priority, time_limit,
                                          mem_limit_mb)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (name) DO NOTHING
                        RETURNING id, task_claim(id, %s)
                        ''', (name, self._ts.rlookup('ts_waiting'), priority,
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
                    ''', (task_id, self._ts.rlookup('ts_waiting'),
                          self._tr.rlookup('tr_task_add')))

            dependency_ids = []
            dependency_ids_mod = []

            for soft, names in [(False, dependency_names),
                                   (True, soft_dependency_names)]:
                for dependency_name in names:
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
                    dependency_ids.append((soft, dependency_id))
                    dependency_ids_mod.append(dependency_id % (1<<31))

            tx.advisory_lock(AdvisoryKey.TASK_DEPENDENCY_ACCESS,
                             sorted(dependency_ids_mod, reverse=True),
                             xact=True)

            for soft, dependency_id in dependency_ids:
                try:
                    tx.execute('''
                            INSERT INTO task_dependency (task_id,
                                                         dependency_id, soft)
                            VALUES (%s, %s, %s)
                            ''', (task_id, dependency_id, soft))
                except CheckViolation as e:
                    if e.constraint_name == 'self_dependency':
                        logger.error('Task cannot depend on itself.')
                        tx.rollback()

                        return
                    else:
                        raise

            self._unclaim(tx, task_id, self.ident)

    def cancel(self, args):
        name_pattern = args.name_pattern
        names = args.name

        self._simple_state_change(None,
                                  self._ts.rlookup('ts_cancelled'),
                                  self._tr.rlookup('tr_task_cancel'),
                                  name_pattern, names)

    def clean(self, args):
        ignore_pending_dependents = args.ignore_pending_dependents

        task_names = []

        while not self.mck.interrupted.is_set():
            logger.debug('Selecting next task.')

            if not task_names:
                logger.debug('Fetching new batch.')

                @self.db.tx
                def tasks(tx):
                    return tx.execute('''
                            SELECT name
                            FROM task
                            WHERE state_id IN (%s, %s)
                            AND claimed_by IS NULL
                            LIMIT 1000
                            ''', (self._ts.rlookup('ts_cleanable'),
                                  self._ts.rlookup('ts_cleaning')))

                for task_name, in tasks:
                    task_names.append(task_name)

            if not task_names:
                logger.debug('No tasks found.')

                break

            task_name = task_names.pop()

            @self.db.tx
            def task(tx):
                return tx.execute('''
                        SELECT id, state_id, task_claim(id, %s)
                        FROM task
                        WHERE name = %s
                        ''', (self.ident, task_name))

            task_id, state_id, claim_success = task[0]

            if not claim_success:
                logger.debug(f'Failed to claim task "{task_name}".')

                continue

            with ClaimStack(self, init_task_ids=[task_id]) as cs:
                state = self._ts.lookup(state_id)
                state_user = self._ts.lookup(state_id, user=True)

                if state == 'ts_cleanable':
                    # The task is up for cleaning, but it currently satisfies
                    # hard dependencies. After we start, it will only satisfy
                    # soft dependencies, so we need to make sure that this
                    # doesn't ruin anyone's day.
                    with self.db.advisory(AdvisoryKey.TASK_DEPENDENCY_ACCESS,
                                          task_id % (1<<31),
                                          shared=True):
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
                                    AND NOT td.soft
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
                            # If there were any dependents that we couldn't
                            # claim, there's no safe way to proceed.
                            logger.warning('Failed to claim dependent of task '
                                           f'"{task_name}".')

                            continue

                        if (not ignore_pending_dependents
                                and dependent_any_pending):
                            logger.warning(f'Task "{task_name}" has pending '
                                           'dependents.')

                            continue

                        @self.db.tx
                        def F(tx):
                            tx.execute('''
                                    INSERT INTO task_history (task_id, state_id,
                                                              reason_id)
                                    VALUES (%s, %s, %s)
                                    ''', (task_id,
                                          self._ts.rlookup('ts_cleaning'),
                                          self._tr.rlookup('tr_task_clean_cleaning')))
                elif state != 'ts_cleaning':
                    logger.debug(f'Task "{task_name}" is in state '
                                 f'"{state_user}" and cannot be cleaned.')

                    continue

                logger.info(task_name)

                if not self._clean(self.conf, task_name):
                    return

                @self.db.tx
                def F(tx):
                    tx.execute('''
                            INSERT INTO task_history (task_id, state_id,
                                                      reason_id)
                            VALUES (%s, %s, %s)
                            ''', (task_id, self._ts.rlookup('ts_cleaned'),
                                  self._tr.rlookup('tr_task_clean_cleaned')))

        logger.debug('Exited cleanly.')

    def cleanablize(self, args):
        name_pattern = args.name_pattern
        names = args.name

        self._simple_state_change(None,
                                  self._ts.rlookup('ts_cleanable'),
                                  self._tr.rlookup('tr_task_cleanablize'),
                                  name_pattern, names)

    def hold(self, args):
        name_pattern = args.name_pattern
        names = args.name

        self._simple_state_change(None,
                                  self._ts.rlookup('ts_held'),
                                  self._tr.rlookup('tr_task_hold'),
                                  name_pattern, names)

    def list(self, args):
        state_name = args.state
        name_pattern = args.name_pattern

        state_id = self._parse_state(state_name)

        @self.db.tx
        def tasks(tx):
            query = '''
                    SELECT name, state_id, priority, time_limit, mem_limit_mb,
                           num_dependencies, num_dependencies_incomplete
                    FROM task
                    WHERE TRUE
                    '''
            query_args = ()

            if state_id is not None:
                query += ' AND state_id = %s'
                query_args += (state_id,)

            if name_pattern is not None:
                query += ' AND name LIKE %s'
                query_args += (name_pattern,)

            query += ' ORDER BY id'

            return tx.execute(query, query_args)

        task_data = []

        for (name, state_id, priority, time_limit, mem_limit_mb, num_dep,
                num_dep_inc) in tasks:
            state, state_user, state_color = self._format_state(state_id)

            if num_dep > 0:
                dep = f'{num_dep-num_dep_inc}/{num_dep}'

                if num_dep_inc > 0:
                    dep = (dep, self.c('notice'))
            else:
                dep = ''

            task_data.append([name, (state_user, state_color), dep, priority,
                              time_limit, mem_limit_mb])

        self.print_table(['Name', 'State', 'Dep', 'Priority', 'Time',
                          'Mem (MB)'],
                         task_data)

    def list_claimed(self, args):
        state_name = args.state
        name_pattern = args.name_pattern
        longer_than_hr = args.longer_than_hr

        state_id = self._parse_state(state_name)

        @self.db.tx
        def tasks(tx):
            query = '''
                    SELECT name, state_id, claimed_by, claimed_since,
                           NOW() - claimed_since AS claimed_for
                    FROM task
                    WHERE claimed_by IS NOT NULL
                    '''
            query_args = ()

            if state_id is not None:
                query += ' AND state_id = %s'
                query_args += (state_id,)

            if name_pattern is not None:
                query += ' AND name LIKE %s'
                query_args += (name_pattern,)

            if longer_than_hr is not None:
                # claimed_for
                query += ' AND NOW() - claimed_since > %s'
                query_args += (timedelta(hours=longer_than_hr),)

            query += ' ORDER BY claimed_for'

            return tx.execute(query, query_args)

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
        name_pattern = args.name_pattern
        names = args.name

        self._simple_state_change(self._ts.rlookup('ts_held'),
                                  self._ts.rlookup('ts_waiting'),
                                  self._tr.rlookup('tr_task_release'),
                                  name_pattern, names)

    def rerun(self, args):
        task_name = args.name

        @self.db.tx
        def task(tx):
            return tx.execute('''
                    SELECT t.id, t.state_id, ts.incomplete, t.elapsed_time,
                           t.max_mem_mb, task_claim(t.id, %s)
                    FROM task t
                    JOIN task_state ts ON ts.id = t.state_id
                    WHERE t.name = %s
                    ''', (self.ident, task_name))

        if len(task) == 0:
            logger.error(f'Task "{task_name}" does not exist.')

            return

        (task_id, state_id, incomplete, elapsed_time, max_mem_mb,
                claim_success) = task[0]

        if not claim_success:
            logger.error(f'Failed to claim task "{task_name}".')

            return

        with ClaimStack(self, init_task_ids=[task_id]) as cs:
            state_user = self._ts.lookup(state_id, user=True)

            if incomplete:
                logger.warning(f'Task "{task_name}" is in state {state_user}.')

                return

            target_data = (task_id, task_name, state_id, incomplete,
                           elapsed_time, max_mem_mb)

            locked_dependency_keys = []

            try:
                task_list \
                        = self._build_rerun_task_list(cs,
                                                      locked_dependency_keys,
                                                      target_data)

                if len(task_list) == 1:
                    logger.debug('Rerunning 1 task.')
                else:
                    logger.debug(f'Rerunning {len(task_list)} tasks.')

                # Rerun all the tasks that were deemed necessary to rerun. We
                # need to push each task through all the states that it has to
                # visit on its way back to waiting.
                for (task_id, task_name, state_id, incomplete, elapsed_time,
                        max_mem_mb) in task_list:
                    logger.info(task_name)

                    state = self._ts.lookup(state_id)
                    state_user = self._ts.lookup(state_id, user=True)

                    if state not in ['ts_done', 'ts_synthesized',
                                     'ts_cleanable', 'ts_cleaning',
                                     'ts_cleaned']:
                        logger.error(f'Task "{task_name}" is in unrecognized '
                                     f'state "{state_user}".')

                        raise HandledException()

                    if state == 'ts_done':
                        elapsed_time_hours = elapsed_time.total_seconds() / 3600
                        max_mem_gb = max_mem_mb / 1024

                        logger.debug('Synthesizing task.')

                        if not self._synthesize(self.conf, task_name,
                                                str(elapsed_time_hours),
                                                str(max_mem_gb)):
                            return

                        @self.db.tx
                        def F(tx):
                            tx.execute('''
                                    INSERT INTO task_history (task_id,
                                                              state_id,
                                                              reason_id)
                                    VALUES (%s, %s, %s)
                                    ''', (task_id,
                                          self._ts.rlookup('ts_synthesized'),
                                          self._tr.rlookup('tr_task_rerun_synthesize')))

                        state = 'ts_synthesized'

                    if state == 'ts_synthesized':
                        logger.debug('Marking task cleanable.')

                        @self.db.tx
                        def F(tx):
                            tx.execute('''
                                    INSERT INTO task_history (task_id,
                                                              state_id,
                                                              reason_id)
                                    VALUES (%s, %s, %s)
                                    ''', (task_id,
                                          self._ts.rlookup('ts_cleanable'),
                                          self._tr.rlookup('tr_task_rerun_cleanablize')))

                        state = 'ts_cleanable'

                    if state == 'ts_cleanable':
                        logger.debug('Preparing task for cleaning.')

                        @self.db.tx
                        def F(tx):
                            tx.execute('''
                                    INSERT INTO task_history (task_id,
                                                              state_id,
                                                              reason_id)
                                    VALUES (%s, %s, %s)
                                    ''', (task_id,
                                          self._ts.rlookup('ts_cleaning'),
                                          self._tr.rlookup('tr_task_rerun_cleaning')))

                        state = 'ts_cleaning'

                    if state == 'ts_cleaning':
                        logger.debug('Cleaning task.')

                        if not self._clean(self.conf, task_name):
                            return

                        @self.db.tx
                        def F(tx):
                            tx.execute('''
                                    INSERT INTO task_history (task_id,
                                                              state_id,
                                                              reason_id)
                                    VALUES (%s, %s, %s)
                                    ''', (task_id,
                                          self._ts.rlookup('ts_cleaned'),
                                          self._tr.rlookup('tr_task_rerun_cleaned')))

                        state = 'ts_cleaned'

                    if state == 'ts_cleaned':
                        logger.debug('Unsynthesizing task.')

                        if not self._unsynthesize(self.conf, task_name):
                            return

                        @self.db.tx
                        def F(tx):
                            tx.execute('''
                                    INSERT INTO task_history (task_id,
                                                              state_id,
                                                              reason_id)
                                    VALUES (%s, %s, %s)
                                    ''', (task_id,
                                          self._ts.rlookup('ts_waiting'),
                                          self._tr.rlookup('tr_task_rerun_reset')))

                        if not self._scrub(self.conf, task_name):
                            return

                        state = 'ts_waiting'
            finally:
                @self.db.tx
                def F(tx):
                    tx.advisory_unlock(AdvisoryKey.TASK_DEPENDENCY_ACCESS,
                                       locked_dependency_keys, shared=True)

    def reset_claimed(self, args):
        names = args.name

        for task_name in names:
            if self.mck.interrupted.is_set():
                break

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

        with ClaimStack(self, init_task_ids=[x[0] for x in tasks]):
            for task_id, task_name in tasks:
                if self.mck.interrupted.is_set():
                    break

                logger.info(task_name)

                if not self._clean(self.conf, task_name):
                    return

                @self.db.tx
                def F(tx):
                    tx.execute('''
                            INSERT INTO task_history (task_id, state_id,
                                                      reason_id)
                            VALUES (%s, %s, %s)
                            ''', (task_id, self._ts.rlookup('ts_waiting'),
                                  self._tr.rlookup('tr_task_reset_failed')))

                if not self._scrub(self.conf, task_name):
                    return

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
        while not self.mck.interrupted.is_set():
            logger.debug('Selecting next task.')

            @self.db.tx
            def task(tx):
                return tx.execute('''
                        WITH unsynthesized_task AS (
                            SELECT id, name, elapsed_time, max_mem_mb
                            FROM task
                            WHERE state_id = %s
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

                break

            (task_id, task_name, elapsed_time, max_mem_mb,
                    claim_success) = task[0]

            if not claim_success:
                logger.debug(f'Failed to claim task "{task_name}".')

                continue

            with ClaimStack(self, init_task_ids=[task_id]):
                logger.info(task_name)

                elapsed_time_hours = elapsed_time.total_seconds() / 3600
                max_mem_gb = max_mem_mb / 1024

                logger.debug('Running synthesis command.')

                if not self._synthesize(self.conf, task_name,
                                        str(elapsed_time_hours),
                                        str(max_mem_gb)):
                    return

                @self.db.tx
                def F(tx):
                    tx.execute('''
                            INSERT INTO task_history (task_id, state_id,
                                                      reason_id)
                            VALUES (%s, %s, %s)
                            ''', (task_id, self._ts.rlookup('ts_synthesized'),
                                  self._tr.rlookup('tr_task_synthesize')))

        logger.debug('Exited cleanly.')

    def uncancel(self, args):
        name_pattern = args.name_pattern
        names = args.name

        self._simple_state_change(self._ts.rlookup('ts_cancelled'),
                                  self._ts.rlookup('ts_waiting'),
                                  self._tr.rlookup('tr_task_uncancel'),
                                  name_pattern, names)

    def uncleanablize(self, args):
        name_pattern = args.name_pattern
        names = args.name

        self._simple_state_change(self._ts.rlookup('ts_cleanable'),
                                  self._ts.rlookup('ts_synthesized'),
                                  self._tr.rlookup('tr_task_uncleanablize'),
                                  name_pattern, names)
