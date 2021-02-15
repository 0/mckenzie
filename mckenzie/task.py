from collections import defaultdict
from contextlib import ExitStack
from datetime import datetime, timedelta
from enum import unique
from heapq import heappop, heappush
import logging
import os
import subprocess

from .arguments import argparsable, argument, description
from .base import DatabaseEnum, Manager
from .database import (AdvisoryKey, CheckViolation, IsolationLevel,
                       RaisedException)
from .util import DirectedAcyclicGraphNode as DAG
from .util import (HandledException, check_proc, format_datetime,
                   format_timedelta, humanize_datetime, print_histograms)


logger = logging.getLogger(__name__)


# Task state flow:
#
#     cancelled   cleaned <-------- cleaning
#        ^  :        |                  ^
#        :  v        |                  |
#        held        |              cleanable
#        ^  :        |                 ^ :
#        :  v        v                 : v
# -+-> waiting <-----+<-- failed   synthesized
#  ^     ^  |               ^           ^
#  |     :  v               |           |
#  |    ready ---------> running ---> done
#  |                                    |
#  |                                    v
#  +<-----------------------------------+
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


@unique
class TaskState(DatabaseEnum):
    ts_waiting = 1
    ts_held = 2
    ts_cancelled = 3
    ts_ready = 4
    ts_running = 5
    ts_failed = 6
    ts_done = 7
    ts_synthesized = 8
    ts_cleanable = 9
    ts_cleaning = 10
    ts_cleaned = 11

    def user(s):
        # Drop "ts_" prefix.
        return s[3:]

    def unuser(s):
        return 'ts_' + s


@unique
class TaskReason(DatabaseEnum):
    tr_task_add = 1
    tr_task_cancel = 2
    tr_task_uncancel = 3
    tr_task_hold = 4
    tr_task_release = 5
    tr_ready = 6
    tr_waiting_dep = 7
    tr_running = 8
    tr_failure_exit_code = 9
    tr_failure_string = 10
    tr_failure_abort = 11
    tr_failure_worker_clean = 12
    tr_failure_run = 13
    tr_failure_memory = 14
    tr_failure_scrapped = 15
    tr_limit_retry = 16
    tr_scrapped_reset = 17
    tr_scrapped_moved = 18
    tr_task_reset_failed = 19
    tr_success = 20
    tr_task_synthesize = 21
    tr_task_cleanablize = 22
    tr_task_uncleanablize = 23
    tr_task_clean_cleaning = 24
    tr_task_clean_cleaned = 25
    tr_task_rerun_synthesize = 26
    tr_task_rerun_cleanablize = 27
    tr_task_rerun_cleaning = 28
    tr_task_rerun_cleaned = 29
    tr_task_rerun_reset = 30


class TaskClaimError(Exception):
    def __init__(self, task_id):
        super().__init__()

        self.task_id = task_id


class ClaimStack(ExitStack):
    def __init__(self, mgr, *args, init_task_ids=None, **kwargs):
        super().__init__(*args, **kwargs)

        if init_task_ids is not None:
            self.claimed_ids = set(init_task_ids)
        else:
            self.claimed_ids = set()

        def unclaim_all():
            @mgr.db.tx
            def F(tx):
                for task_id in self.claimed_ids:
                    TaskManager._unclaim(tx, task_id)

        self.callback(unclaim_all)

    def add(self, task_id):
        self.claimed_ids.add(task_id)


class LimitChecker:
    def __init__(self, *, count_limit=None, time_limit_hr=None):
        self.count_remaining = count_limit

        if time_limit_hr is not None:
            self.time_end = datetime.now() + timedelta(hours=time_limit_hr)
        else:
            self.time_end = None

    def count(self):
        if self.count_remaining is not None:
            self.count_remaining -= 1

    @property
    def reached(self):
        if self.count_remaining is not None and self.count_remaining <= 0:
            logger.debug('Count limit reached.')

            return True

        if self.time_end is not None and datetime.now() >= self.time_end:
            logger.debug('Time limit reached.')

            return True

        return False


@argparsable('task management')
class TaskManager(Manager, name='task'):
    STATE_ORDER = ['cancelled', 'held', 'waiting', 'ready', 'running',
                   'failed', 'done', 'synthesized', 'cleanable', 'cleaning',
                   'cleaned']

    @staticmethod
    def _claim(tx, task_id):
        success = tx.callproc('task_claim', (task_id,))[0][0]

        if not success:
            raise TaskClaimError(task_id)

    @staticmethod
    def _unclaim(tx, task_id, *, force=False):
        success = tx.callproc('task_unclaim', (task_id, force))[0][0]

        if not success:
            raise TaskClaimError(task_id)

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

    @staticmethod
    def _parse_state(state_name):
        if state_name is None:
            return None

        try:
            return TaskState[TaskState.unuser(state_name)]
        except KeyError:
            logger.error(f'Invalid state "{state_name}".')

            raise HandledException()

    @classmethod
    def _clean(cls, conf, task_name):
        return cls._run_cmd(conf.general_work_path, conf.task_clean_cmd,
                            task_name)

    @classmethod
    def _scrub(cls, conf, task_name):
        return cls._run_cmd(conf.general_work_path, conf.task_scrub_cmd,
                            task_name)

    @classmethod
    def _synthesize(cls, conf, task_name, elapsed_time_hours, max_mem_gb):
        return cls._run_cmd(conf.general_work_path, conf.task_synthesize_cmd,
                            task_name, elapsed_time_hours, max_mem_gb)

    @classmethod
    def _unsynthesize(cls, conf, task_name):
        return cls._run_cmd(conf.general_work_path, conf.task_unsynthesize_cmd,
                            task_name)

    def _format_state(self, state_id):
        state = TaskState(state_id).name
        state_user = TaskState.user(state)
        color = None

        if state == 'ts_failed':
            color = self.c('error')
        elif state in ['ts_waiting', 'ts_ready']:
            color = self.c('notice')
        elif state == 'ts_running':
            color = self.c('good')
        elif state not in ['ts_cancelled', 'ts_synthesized', 'ts_cleaned']:
            # NOT task_state.terminal
            color = self.c('warning')

        return state, state_user, color

    def _simple_state_change(self, from_state_id, to_state_id, reason_id,
                             name_pattern, names, *, count_limit=None,
                             time_limit_hr=None):
        task_names = names.copy()
        lc = LimitChecker(count_limit=count_limit, time_limit_hr=time_limit_hr)

        while not self.mck.interrupted and not lc.reached:
            logger.debug('Selecting next task.')

            with ClaimStack(self) as cs:
                # These values must be set by the time we update the task.
                task_id = None
                task_name = None
                state_user = None

                if task_names:
                    logger.debug('Using the next explicitly-given task.')
                    task_name = task_names.pop(0)

                    @self.db.tx
                    def task(tx):
                        return tx.execute('''
                                SELECT t.id, t.state_id, tst.id,
                                       task_claim(t.id)
                                FROM task t
                                LEFT JOIN task_state_transition tst
                                    ON (tst.from_state_id = t.state_id
                                        AND tst.to_state_id = %s)
                                WHERE t.name = %s
                                ''', (to_state_id, task_name))

                    if len(task) == 0:
                        logger.warning(f'Task "{task_name}" does not exist.')

                        continue

                    task_id, state_id, transition, claim_success = task[0]
                    state_user = TaskState.user(TaskState(state_id).name)

                    if not claim_success:
                        logger.warning(f'Task "{task_name}" could not be '
                                       'claimed.')

                        continue

                    cs.add(task_id)

                    if transition is None:
                        logger.warning(f'Task "{task_name}" is in state '
                                       f'"{state_user}".')

                        continue
                elif name_pattern is not None:
                    logger.debug('Finding an eligible task.')

                    @self.db.tx
                    def task(tx):
                        query = '''
                                WITH next_task AS (
                                    SELECT t.id, t.name, t.state_id
                                    FROM task t
                                    JOIN task_state ts ON ts.id = t.state_id
                                    JOIN task_state_transition tst
                                        ON tst.from_state_id = t.state_id
                                    WHERE t.claimed_by IS NULL
                                    AND t.name LIKE %s
                                '''
                        query_args = (name_pattern,)

                        if from_state_id is not None:
                            query += ' AND t.state_id = %s'
                            query_args += (from_state_id,)
                        else:
                            query += ' AND NOT ts.terminal'
                            query += ' AND NOT ts.exceptional'

                        query += '''
                                    AND tst.to_state_id = %s
                                    AND tst.free_transition
                                    LIMIT 1
                                    FOR UPDATE OF t SKIP LOCKED
                                )
                                SELECT id, name, state_id, task_claim(id)
                                FROM next_task
                                '''
                        query_args += (to_state_id,)

                        return tx.execute(query, query_args)

                    if len(task) == 0:
                        logger.debug('No tasks found.')

                        break

                    task_id, task_name, state_id, claim_success = task[0]
                    state_user = TaskState.user(TaskState(state_id).name)

                    if not claim_success:
                        logger.debug(f'Failed to claim task "{task_name}".')

                        continue

                    cs.add(task_id)
                else:
                    logger.debug('Out of tasks.')

                    break

                logger.debug('Updating task.')

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
                            logger.warning(f'Task "{task_name}" is in state '
                                           f'"{state_user}".')
                            tx.rollback()

                            return False
                        else:
                            raise

                    return True

                if not success:
                    continue

                logger.info(task_name)
                lc.count()

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
                               task_claim(t.id)
                        FROM task t
                        JOIN task_state ts ON ts.id = t.state_id
                        JOIN task_dependency td ON td.task_id = t.id
                        WHERE td.dependency_id = %s
                        ''', (cur_id,))

            any_claim_failed = False

            for task_id, *_, claim_success in new_tasks:
                if claim_success:
                    cs.add(task_id)
                else:
                    any_claim_failed = True

            if any_claim_failed:
                logger.error(f'Dependent of task "{cur_name}" could not be '
                             'claimed.')

                raise HandledException()

            for *data, incomplete, claim_success in new_tasks:
                task_id, *_ = data

                if incomplete:
                    # Tasks that haven't been completed yet don't need to be
                    # included in the graph, since there's no need to rerun
                    # them or inspect their dependents.
                    continue

                try:
                    new_node = task_nodes[task_id]
                except KeyError:
                    new_node = DAG(data)
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
                         total=('Total', (1, 2), (0, timedelta())))

    @description('create a new task')
    @argument('--time-hr', metavar='T', type=float, required=True, help='time limit in hours')
    @argument('--mem-gb', metavar='M', type=float, required=True, help='memory limit in GB')
    @argument('--priority', metavar='P', type=int, default=0, help='priority (default: 0)')
    @argument('--depends-on', metavar='DEP', action='append', help='task DEP is a dependency')
    @argument('--soft-depends-on', metavar='DEP', action='append', help='task DEP is a soft dependency')
    @argument('name', help='task name')
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
                        RETURNING id, task_claim(id)
                        ''', (name, TaskState.ts_waiting, priority, time_limit,
                              mem_limit_mb))
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
                    ''', (task_id, TaskState.ts_waiting,
                          TaskReason.tr_task_add))

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

            self._unclaim(tx, task_id)

    @description('change task state to "cancelled"')
    @argument('--count-limit', metavar='N', type=int, help='stop after N tasks')
    @argument('--time-limit-hr', metavar='T', type=float, help='stop after T hours')
    @argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
    @argument('name', nargs='*', help='task name')
    def cancel(self, args):
        count_limit = args.count_limit
        time_limit_hr = args.time_limit_hr
        name_pattern = args.name_pattern
        names = args.name

        self._simple_state_change(None,
                                  TaskState.ts_cancelled,
                                  TaskReason.tr_task_cancel,
                                  name_pattern, names, count_limit=count_limit,
                                  time_limit_hr=time_limit_hr)

    @description('visualize task state changes')
    @argument('--span-hr', metavar='T', type=float, required=True, help='show bars spanning T hours')
    def churn(self, args):
        span = timedelta(hours=args.span_hr)

        B = 80

        @self.db.tx
        def task_states(tx):
            return tx.execute('''
                    WITH edge (x) AS (
                        SELECT y * %s
                        FROM GENERATE_SERIES(1, %s) AS y
                    ),
                    states (bucket, state_id, count) AS (
                        SELECT WIDTH_BUCKET(NOW() - th.time,
                                            ARRAY(SELECT x FROM edge)) AS bucket,
                               th.state_id, COUNT(*)
                        FROM task_history th
                        WHERE th.time > NOW() - (SELECT MAX(x) FROM edge)
                        GROUP BY bucket, th.state_id
                        ORDER BY bucket, th.state_id
                    )
                    SELECT bucket, ARRAY_AGG(ARRAY[state_id, count])
                    FROM states
                    GROUP BY bucket
                    ORDER BY bucket
                    ''', (span, B))

        data = []
        idx = 0

        for bucket, states in task_states:
            while idx + 1 < bucket:
                data.append([])
                idx += 1

            bucket_data = defaultdict(int)

            for state_id, count in states:
                _, _, color = self._format_state(state_id)

                if color is None:
                    color = ''

                bucket_data[color] += count

            data.append(list(sorted(bucket_data.items())))

            idx += 1

        while len(data) < B:
            data.append([])

        td_str = format_timedelta(span)
        now = datetime.now()
        data_start = now - B * span
        data_start_regular = format_datetime(data_start)
        data_start_human = humanize_datetime(data_start, now, force=True)

        print(f'{td_str} (from {data_start_regular}, or {data_start_human})')
        self.print_time_series(list(reversed(data)))

    @description('run clean command for "cleanable" tasks')
    @argument('--count-limit', metavar='N', type=int, help='stop after N tasks')
    @argument('--time-limit-hr', metavar='T', type=float, help='stop after T hours')
    def clean(self, args):
        count_limit = args.count_limit
        time_limit_hr = args.time_limit_hr

        lc = LimitChecker(count_limit=count_limit, time_limit_hr=time_limit_hr)

        # We start by finishing the cleaning process for tasks that were left
        # in "cleaning". Once there are no more, we move on to "cleanable"
        # tasks, which are more difficult to handle.
        searching_for_cleaning = True

        while not self.mck.interrupted and not lc.reached:
            logger.debug('Selecting next task.')

            with ClaimStack(self) as cs:
                # These values must be set by the time we update the task.
                task_id = None
                task_name = None

                if searching_for_cleaning:
                    logger.debug('Searching for task in "cleaning".')

                    @self.db.tx
                    def task(tx):
                        return tx.execute('''
                                WITH next_task AS (
                                    SELECT id, name
                                    FROM task
                                    WHERE state_id = %s
                                    AND claimed_by IS NULL
                                    LIMIT 1
                                    FOR UPDATE SKIP LOCKED
                                )
                                SELECT id, name, task_claim(id)
                                FROM next_task
                                ''', (TaskState.ts_cleaning,))

                    if len(task) == 0:
                        logger.debug('No tasks found.')
                        searching_for_cleaning = False

                        continue

                    task_id, task_name, claim_success = task[0]

                    if not claim_success:
                        logger.debug(f'Failed to claim task "{task_name}".')

                        continue

                    cs.add(task_id)
                else:
                    logger.debug('Searching for task in "cleanable".')

                    # Tasks in "cleanable" are up for cleaning, but they
                    # satisfy hard dependencies. After we start cleaning them,
                    # they will only satisfy soft dependencies, so we need to
                    # make sure that this transition doesn't ruin anyone's day.
                    # We do this by claiming all the hard dependents and making
                    # sure that none of them are in a pending state.
                    #
                    # First, we select and claim a task that at some recent
                    # point in time satisfied the criteria.
                    @self.db.tx
                    def task(tx):
                        # If there are no dependent tasks, the LEFT JOINs in
                        # the inner-most subquery will populate columns of td,
                        # t2, and ts2 with NULL, so we must be ready to accept
                        # such rows.
                        return tx.execute('''
                                WITH next_task AS (
                                    WITH cleanable_tasks AS (
                                        SELECT t1.id
                                        FROM task t1
                                        LEFT JOIN task_dependency td
                                            ON (td.dependency_id = t1.id
                                                AND NOT td.soft)
                                        LEFT JOIN task t2 ON t2.id = td.task_id
                                        LEFT JOIN task_state ts2
                                            ON ts2.id = t2.state_id
                                        WHERE t1.state_id = %s
                                        AND t1.claimed_by IS NULL
                                        GROUP BY t1.id
                                        HAVING COUNT(t2.claimed_by) = 0
                                        AND COALESCE(NOT BOOL_OR(ts2.pending),
                                                     TRUE)
                                    )
                                    SELECT t.id, t.name
                                    FROM task t
                                    JOIN cleanable_tasks ct ON ct.id = t.id
                                    WHERE t.state_id = %s
                                    AND t.claimed_by IS NULL
                                    LIMIT 1
                                    FOR UPDATE OF t SKIP LOCKED
                                )
                                SELECT id, name, task_claim(id)
                                FROM next_task
                                ''', (TaskState.ts_cleanable,
                                      TaskState.ts_cleanable))

                    if len(task) == 0:
                        logger.debug('No tasks found.')

                        break

                    task_id, task_name, claim_success = task[0]

                    if not claim_success:
                        logger.debug(f'Failed to claim task "{task_name}".')

                        continue

                    cs.add(task_id)

                    # We have successfully claimed the task, but it's possible
                    # that it either has new dependents since we first checked,
                    # or some of them have been claimed or changed state. We
                    # prevent the addition of any new dependents with the
                    # advisory lock, and then we'll try to claim all existing
                    # dependents and check their state.
                    with self.db.advisory(AdvisoryKey.TASK_DEPENDENCY_ACCESS,
                                          task_id % (1<<31),
                                          shared=True):
                        with ClaimStack(self) as cs_dep:
                            @self.db.tx
                            def direct_dependents(tx):
                                return tx.execute('''
                                        SELECT td.task_id,
                                               task_claim(td.task_id),
                                               ts.pending
                                        FROM task_dependency td
                                        JOIN task t ON t.id = td.task_id
                                        JOIN task_state ts ON ts.id = t.state_id
                                        WHERE td.dependency_id = %s
                                        AND NOT td.soft
                                        ''', (task_id,))

                            any_dependent_claim_failed = False
                            any_dependent_pending = False

                            for (dependent_id, dependent_claim_success,
                                    dependent_pending) in direct_dependents:
                                if dependent_claim_success:
                                    cs_dep.add(dependent_id)
                                else:
                                    any_dependent_claim_failed = True

                                if dependent_pending:
                                    any_dependent_pending = True

                            if any_dependent_claim_failed:
                                # If there were any dependents that we couldn't
                                # claim, there's no safe way to proceed.
                                logger.debug('Failed to claim dependent of task '
                                             f'"{task_name}".')

                                continue

                            if any_dependent_pending:
                                logger.debug(f'Task "{task_name}" has pending '
                                             'dependents.')

                                continue

                            # Everything looks good, so it's safe to change the
                            # task's state from "cleanable" to "cleaning", at
                            # which point it will no longer satisfy any hard
                            # dependencies and may be safely cleaned. Once the
                            # state has been changed, we will no longer need to
                            # hold any claims on the dependent tasks or prevent
                            # the addition of new ones.
                            @self.db.tx
                            def F(tx):
                                tx.execute('''
                                        INSERT INTO task_history (task_id,
                                                                  state_id,
                                                                  reason_id)
                                        VALUES (%s, %s, %s)
                                        ''', (task_id, TaskState.ts_cleaning,
                                              TaskReason.tr_task_clean_cleaning))

                logger.info(task_name)
                logger.debug('Updating task.')

                if not self._clean(self.conf, task_name):
                    return

                @self.db.tx
                def F(tx):
                    tx.execute('''
                            INSERT INTO task_history (task_id, state_id,
                                                      reason_id)
                            VALUES (%s, %s, %s)
                            ''', (task_id, TaskState.ts_cleaned,
                                  TaskReason.tr_task_clean_cleaned))

            lc.count()

        logger.debug('Exited cleanly.')

    @description('change task state from "synthesized" to "cleanable"')
    @argument('--count-limit', metavar='N', type=int, help='stop after N tasks')
    @argument('--time-limit-hr', metavar='T', type=float, help='stop after T hours')
    @argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
    @argument('name', nargs='*', help='task name')
    def cleanablize(self, args):
        count_limit = args.count_limit
        time_limit_hr = args.time_limit_hr
        name_pattern = args.name_pattern
        names = args.name

        self._simple_state_change(TaskState.ts_synthesized,
                                  TaskState.ts_cleanable,
                                  TaskReason.tr_task_cleanablize,
                                  name_pattern, names, count_limit=count_limit,
                                  time_limit_hr=time_limit_hr)

    @description('show outstanding tasks')
    @argument('--prefix-separator', metavar='S', help='task name prefix separator string')
    @argument('--blocking', action='store_true', help='include blocking dependencies')
    def health(self, args):
        prefix_separator = args.prefix_separator
        show_blocking = args.blocking

        @self.db.tx
        def ready_tasks(tx):
            tx.isolate(IsolationLevel.REPEATABLE_READ)

            if prefix_separator is not None:
                prefix_counts = tx.execute('''
                        SELECT SPLIT_PART(name, %s, 1) AS prefix,
                               COUNT(*) AS count
                        FROM task
                        WHERE state_id = %s
                        GROUP BY prefix
                        ORDER BY prefix
                        ''', (prefix_separator, TaskState.ts_ready))
            else:
                prefix_counts = None

            by_time = tx.execute('''
                    (
                        SELECT name, state_id, priority, time_limit,
                               mem_limit_mb
                        FROM task
                        WHERE state_id = %s
                        ORDER BY time_limit ASC, id ASC
                        LIMIT 4
                    )
                    UNION
                    (
                        SELECT name, state_id, priority, time_limit,
                               mem_limit_mb
                        FROM task
                        WHERE state_id = %s
                        ORDER BY time_limit DESC, id DESC
                        LIMIT 3
                    )
                    ORDER BY time_limit
                    ''', (TaskState.ts_ready, TaskState.ts_ready))

            by_mem = tx.execute('''
                    (
                        SELECT name, state_id, priority, time_limit,
                               mem_limit_mb
                        FROM task
                        WHERE state_id = %s
                        ORDER BY mem_limit_mb ASC, id ASC
                        LIMIT 4
                    )
                    UNION
                    (
                        SELECT name, state_id, priority, time_limit,
                               mem_limit_mb
                        FROM task
                        WHERE state_id = %s
                        ORDER BY mem_limit_mb DESC, id DESC
                        LIMIT 3
                    )
                    ORDER BY mem_limit_mb
                    ''', (TaskState.ts_ready, TaskState.ts_ready))

            times = tx.execute('''
                    WITH time (seconds) AS (
                        SELECT EXTRACT(EPOCH FROM time_limit)
                        FROM task
                        WHERE state_id = %s
                    ),
                    bound (min, max) AS (
                        SELECT MIN(seconds), MAX(seconds)
                        FROM time
                    )
                    SELECT WIDTH_BUCKET(LOG(time.seconds), LOG(bound.min),
                                        LOG(bound.max+1), 10) AS bucket,
                           MAKE_INTERVAL(SECS => MIN(time.seconds)),
                           MAKE_INTERVAL(SECS => MAX(time.seconds)), COUNT(*)
                    FROM time, bound
                    GROUP BY bucket
                    ORDER BY bucket
                    ''', (TaskState.ts_ready,))

            mems = tx.execute('''
                    WITH mem (mbs) AS (
                        SELECT mem_limit_mb
                        FROM task
                        WHERE state_id = %s
                    ),
                    bound (min, max) AS (
                        SELECT MIN(mbs), MAX(mbs)
                        FROM mem
                    )
                    SELECT WIDTH_BUCKET(LOG(mem.mbs), LOG(bound.min),
                                        LOG(bound.max+1), 10) AS bucket,
                           MIN(mem.mbs), MAX(mem.mbs), COUNT(*)
                    FROM mem, bound
                    GROUP BY bucket
                    ORDER BY bucket
                    ''', (TaskState.ts_ready,))

            return prefix_counts, by_time, by_mem, times, mems

        (prefix_counts, ready_tasks_by_time, ready_tasks_by_mem,
                ready_task_times, ready_task_mems) = ready_tasks

        if ready_tasks_by_time:
            if prefix_counts is not None:
                self.print_table(['Prefix', 'Count'], prefix_counts)

                print()

            # If there are at least 7 tasks to choose from, we can remove the
            # middle one from those we've obtained and know that at least one
            # is missing. Otherwise, we show all ready tasks.
            if len(ready_tasks_by_time) == 7:
                ready_tasks_by_time[3] = [None] * 5
                ready_tasks_by_mem[3] = [None] * 5

            task_data = []

            for (name, state_id, priority, time_limit,
                    mem_limit_mb) in ready_tasks_by_time:
                if name is None:
                    task_data.append(Ellipsis)

                    continue

                state, state_user, state_color = self._format_state(state_id)

                task_data.append([name, (state_user, state_color), priority,
                                  (time_limit, self.c('bold')), mem_limit_mb])

            self.print_table(['Name', 'State', 'Priority', 'Time', 'Mem (MB)'],
                             task_data)

            print()

            task_data = []

            for (name, state_id, priority, time_limit,
                    mem_limit_mb) in ready_tasks_by_mem:
                if name is None:
                    task_data.append(Ellipsis)

                    continue

                state, state_user, state_color = self._format_state(state_id)

                task_data.append([name, (state_user, state_color), priority,
                                  time_limit, (mem_limit_mb, self.c('bold'))])

            self.print_table(['Name', 'State', 'Priority', 'Time', 'Mem (MB)'],
                             task_data)

            print()

            print_histograms(['Time', 'Mem (MB)'],
                             [ready_task_times, ready_task_mems])
        else:
            print('No ready tasks.')

        print()

        @self.db.tx
        def claimed_nonworker_tasks(tx):
            return tx.execute('''
                    SELECT name, state_id, parse_claim(claimed_by),
                           claimed_since, NOW() - claimed_since AS claimed_for
                    FROM task t
                    WHERE claimed_by IS NOT NULL
                    AND parse_claim_type(claimed_by) != 'worker'
                    ORDER BY claimed_for DESC, id
                    LIMIT 5
                    ''')

        task_data = []

        for (name, state_id, claimed_by, claimed_since,
                claimed_for) in claimed_nonworker_tasks:
            state, state_user, state_color = self._format_state(state_id)

            task_data.append([name, (state_user, state_color), claimed_by,
                              claimed_since, claimed_for])

        if task_data:
            self.print_table(['Name', 'State', 'Claimed by', 'Since', 'For'],
                             reversed(task_data))
        else:
            print('No tasks claimed by non-workers.')

        print()

        @self.db.tx
        def claimed_nonrunning_tasks(tx):
            return tx.execute('''
                    SELECT name, state_id, parse_claim(claimed_by),
                           claimed_since, NOW() - claimed_since AS claimed_for
                    FROM task t
                    WHERE claimed_by IS NOT NULL
                    AND parse_claim_type(claimed_by) = 'worker'
                    AND state_id != %s
                    ORDER BY claimed_for DESC, id
                    LIMIT 5
                    ''', (TaskState.ts_running,))

        task_data = []

        for (name, state_id, claimed_by, claimed_since,
                claimed_for) in claimed_nonrunning_tasks:
            state, state_user, state_color = self._format_state(state_id)

            task_data.append([name, (state_user, state_color), claimed_by,
                              claimed_since, claimed_for])

        if task_data:
            self.print_table(['Name', 'State', 'Claimed by', 'Since', 'For'],
                             reversed(task_data))
        else:
            print('No non-running tasks claimed by workers.')

        if show_blocking:
            print()

            @self.db.tx
            def blocking_dependencies(tx):
                return tx.execute('''
                        SELECT t2.name, COUNT(*) AS count
                        FROM task t1
                        JOIN task_state ts1 ON ts1.id = t1.state_id
                        JOIN task_dependency td ON td.task_id = t1.id
                        JOIN task t2 ON t2.id = td.dependency_id
                        JOIN task_state ts2 ON ts2.id = t2.state_id
                        WHERE ts1.pending
                        AND NOT (ts2.satisfies_dependency
                                 OR (td.soft AND ts2.satisfies_soft_dependency)
                                 OR ts2.pending)
                        GROUP BY t2.name
                        ORDER BY count DESC
                        LIMIT 5
                        ''')

            task_data = []

            for task_name, count in blocking_dependencies:
                task_data.append([task_name, count])

            if task_data:
                self.print_table(['Name', 'Blocked dependents'],
                                 reversed(task_data))
            else:
                print('No blocking dependencies.')

    @description('change task state to "held"')
    @argument('--count-limit', metavar='N', type=int, help='stop after N tasks')
    @argument('--time-limit-hr', metavar='T', type=float, help='stop after T hours')
    @argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
    @argument('name', nargs='*', help='task name')
    def hold(self, args):
        count_limit = args.count_limit
        time_limit_hr = args.time_limit_hr
        name_pattern = args.name_pattern
        names = args.name

        self._simple_state_change(None,
                                  TaskState.ts_held,
                                  TaskReason.tr_task_hold,
                                  name_pattern, names, count_limit=count_limit,
                                  time_limit_hr=time_limit_hr)

    @description('list tasks')
    @argument('--state', metavar='S', help='only tasks in state S')
    @argument('--name-pattern', metavar='P', help='only tasks with names matching the SQL LIKE pattern P')
    @argument('--allow-all', action='store_true', help='allow all tasks to be listed')
    def list(self, args):
        state_name = args.state
        name_pattern = args.name_pattern
        allow_all = args.allow_all

        state_id = self._parse_state(state_name)

        if not allow_all and state_id is None and name_pattern is None:
            logger.error('Refusing to list all tasks. Use --allow-all.')

            return

        @self.db.tx
        def tasks(tx):
            query = '''
                    SELECT name, state_id, priority, time_limit, mem_limit_mb,
                           num_dependencies, num_dependencies_satisfied
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

            query += ' ORDER BY priority ASC, id'

            return tx.execute(query, query_args)

        task_data = []

        for (name, state_id, priority, time_limit, mem_limit_mb, num_dep,
                num_dep_sat) in tasks:
            state, state_user, state_color = self._format_state(state_id)

            if num_dep > 0:
                dep = f'{num_dep_sat}/{num_dep}'

                if num_dep_sat != num_dep:
                    dep = (dep, self.c('notice'))
            else:
                dep = ''

            task_data.append([name, (state_user, state_color), dep, priority,
                              time_limit, mem_limit_mb])

        self.print_table(['Name', 'State', 'Dep', 'Priority', 'Time',
                          'Mem (MB)'],
                         task_data)

    @description('list claimed tasks')
    @argument('--state', metavar='S', help='only tasks in state S')
    @argument('--name-pattern', metavar='P', help='only tasks with names matching the SQL LIKE pattern P')
    @argument('--longer-than-hr', metavar='T', type=float, help='only tasks claimed for longer than T hours')
    def list_claimed(self, args):
        state_name = args.state
        name_pattern = args.name_pattern
        longer_than_hr = args.longer_than_hr

        state_id = self._parse_state(state_name)

        @self.db.tx
        def tasks(tx):
            query = '''
                    SELECT name, state_id, parse_claim(claimed_by),
                           claimed_since, NOW() - claimed_since AS claimed_for
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

        task_data = []

        for name, state_id, claimed_by, claimed_since, claimed_for in tasks:
            state_user = TaskState.user(TaskState(state_id).name)

            task_data.append([name, state_user, claimed_by, claimed_since,
                              claimed_for])

        self.print_table(['Name', 'State', 'Claimed by', 'Since', 'For'],
                         task_data)

    @description('change "held" task state to "waiting"')
    @argument('--count-limit', metavar='N', type=int, help='stop after N tasks')
    @argument('--time-limit-hr', metavar='T', type=float, help='stop after T hours')
    @argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
    @argument('name', nargs='*', help='task name')
    def release(self, args):
        count_limit = args.count_limit
        time_limit_hr = args.time_limit_hr
        name_pattern = args.name_pattern
        names = args.name

        self._simple_state_change(TaskState.ts_held,
                                  TaskState.ts_waiting,
                                  TaskReason.tr_task_release,
                                  name_pattern, names, count_limit=count_limit,
                                  time_limit_hr=time_limit_hr)

    @description('rerun a task and all its dependents')
    @argument('name', help='task name')
    def rerun(self, args):
        task_name = args.name

        @self.db.tx
        def task(tx):
            return tx.execute('''
                    SELECT t.id, t.state_id, ts.incomplete, task_claim(t.id)
                    FROM task t
                    JOIN task_state ts ON ts.id = t.state_id
                    WHERE t.name = %s
                    ''', (task_name,))

        if len(task) == 0:
            logger.error(f'Task "{task_name}" does not exist.')

            return

        task_id, state_id, incomplete, claim_success = task[0]

        if not claim_success:
            logger.error(f'Failed to claim task "{task_name}".')

            return

        with ClaimStack(self, init_task_ids=[task_id]) as cs:
            state_user = TaskState.user(TaskState(state_id).name)

            if incomplete:
                logger.warning(f'Task "{task_name}" is in state {state_user}.')

                return

            target_data = task_id, task_name, state_id

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
                for task_id, task_name, state_id in task_list:
                    logger.info(task_name)

                    state = TaskState(state_id).name
                    state_user = TaskState.user(state)

                    if state not in ['ts_done', 'ts_synthesized',
                                     'ts_cleanable', 'ts_cleaning',
                                     'ts_cleaned']:
                        logger.error(f'Task "{task_name}" is in unrecognized '
                                     f'state "{state_user}".')

                        raise HandledException()

                    if state == 'ts_done':
                        logger.debug('Resetting task.')

                        @self.db.tx
                        def F(tx):
                            tx.execute('''
                                    INSERT INTO task_history (task_id,
                                                              state_id,
                                                              reason_id)
                                    VALUES (%s, %s, %s)
                                    ''', (task_id, TaskState.ts_waiting,
                                          TaskReason.tr_task_rerun_reset))

                        logger.debug('Cleaning task.')

                        if not self._clean(self.conf, task_name):
                            return

                        if not self._scrub(self.conf, task_name):
                            return

                        state = 'ts_waiting'

                    if state == 'ts_synthesized':
                        logger.debug('Marking task cleanable.')

                        @self.db.tx
                        def F(tx):
                            tx.execute('''
                                    INSERT INTO task_history (task_id,
                                                              state_id,
                                                              reason_id)
                                    VALUES (%s, %s, %s)
                                    ''', (task_id, TaskState.ts_cleanable,
                                          TaskReason.tr_task_rerun_cleanablize))

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
                                    ''', (task_id, TaskState.ts_cleaning,
                                          TaskReason.tr_task_rerun_cleaning))

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
                                    ''', (task_id, TaskState.ts_cleaned,
                                          TaskReason.tr_task_rerun_cleaned))

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
                                    ''', (task_id, TaskState.ts_waiting,
                                          TaskReason.tr_task_rerun_reset))

                        if not self._scrub(self.conf, task_name):
                            return

                        state = 'ts_waiting'
            finally:
                @self.db.tx
                def F(tx):
                    tx.advisory_unlock(AdvisoryKey.TASK_DEPENDENCY_ACCESS,
                                       locked_dependency_keys, shared=True)

    @description('unclaim abandoned tasks')
    @argument('name', nargs='*', help='task name')
    def reset_claimed(self, args):
        names = args.name

        for task_name in names:
            if self.mck.interrupted:
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
                self._unclaim(tx, task_id, force=True)

            logger.info(task_name)

    @description('reset all "failed" tasks to "waiting"')
    def reset_failed(self, args):
        while not self.mck.interrupted:
            logger.debug('Selecting next task.')

            @self.db.tx
            def task(tx):
                return tx.execute('''
                        WITH failed_task AS (
                            SELECT id, name
                            FROM task
                            WHERE state_id = %s
                            AND claimed_by IS NULL
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        SELECT id, name, task_claim(id)
                        FROM failed_task
                        ''', (TaskState.ts_failed,))

            if len(task) == 0:
                logger.debug('No tasks found.')

                break

            task_id, task_name, claim_success = task[0]

            if not claim_success:
                logger.debug(f'Failed to claim task "{task_name}".')

                continue

            with ClaimStack(self, init_task_ids=[task_id]):
                logger.info(task_name)

                logger.debug('Running clean command.')

                if not self._clean(self.conf, task_name):
                    return

                logger.debug('Updating task.')

                @self.db.tx
                def F(tx):
                    tx.execute('''
                            INSERT INTO task_history (task_id, state_id,
                                                      reason_id)
                            VALUES (%s, %s, %s)
                            ''', (task_id, TaskState.ts_waiting,
                                  TaskReason.tr_task_reset_failed))

                logger.debug('Running scrub command.')

                if not self._scrub(self.conf, task_name):
                    return

        logger.debug('Exited cleanly.')

    @description('show task details')
    @argument('name', help='task name')
    def show(self, args):
        name = args.name

        @self.db.tx
        def task(tx):
            return tx.execute('''
                    SELECT id, priority, time_limit, mem_limit_mb,
                           elapsed_time, max_mem_mb, parse_claim(claimed_by),
                           claimed_since, NOW() - claimed_since
                    FROM task
                    WHERE name = %s
                    ''', (name,))

        if len(task) == 0:
            logger.error(f'Task "{name}" does not exist.')

            return

        (task_id, priority, time_limit, mem_limit_mb, elapsed_time, max_mem_mb,
                claimed_by, claimed_since, claimed_for) = task[0]

        elapsed_time = elapsed_time if elapsed_time is not None else '-'
        max_mem_mb = max_mem_mb if max_mem_mb is not None else '-'

        self.print_table(['Name', 'Priority', ('Time (A/E)', 2),
                          ('Mem (MB;A/E)', 2)],
                         [[name, priority, elapsed_time, time_limit,
                           max_mem_mb, mem_limit_mb]])

        print()

        @self.db.tx
        def task_history(tx):
            return tx.execute('''
                    SELECT th.state_id, th.time,
                           LEAD(th.time, 1, NOW())
                               OVER (ORDER BY th.time, th.id),
                           th.worker_id, tr.description
                    FROM task_history th
                    JOIN task_reason tr ON tr.id = th.reason_id
                    WHERE th.task_id = %s
                    ORDER BY th.id
                    ''', (task_id,))

        task_data = []

        for state_id, time, time_next, worker_id, reason in task_history:
            state, state_user, state_color = self._format_state(state_id)
            duration = time_next - time

            if worker_id is not None:
                worker_id = str(worker_id)
            else:
                worker_id = ''

            task_data.append([time, duration, (state_user, state_color),
                              reason, worker_id])

        if task_data:
            self.print_table(['Time', 'Duration', 'State', 'Reason', 'Worker'],
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
            self.print_table(['Worker', 'Active at', 'Inactive at',
                              'Duration'],
                             task_data)
        else:
            print('No worker activity.')

        print()

        @self.db.tx
        def task_dependency(tx):
            return tx.execute('''
                    SELECT t.name, td.soft, t.state_id
                    FROM task_dependency td
                    JOIN task t ON t.id = td.dependency_id
                    WHERE td.task_id = %s
                    ORDER BY t.id
                    ''', (task_id,))

        task_data = []

        for dependency_name, soft, state_id in task_dependency:
            state, state_user, state_color = self._format_state(state_id)

            task_data.append([dependency_name, 'soft' if soft else 'hard',
                              (state_user, state_color)])

        if task_data:
            self.print_table(['Dependency', 'Type', 'State'], task_data)
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
            print(f'{num_recursive_dependents} dependents '
                  f'({num_direct_dependents} direct).')
        else:
            print('No dependents.')

        print()

        if claimed_by is not None:
            print(f'Claimed by "{claimed_by}" since '
                  f'{format_datetime(claimed_since)} '
                  f'(for {format_timedelta(claimed_for)}).')
        else:
            print('Not claimed.')

    @description('synthesize completed tasks')
    @argument('--count-limit', metavar='N', type=int, help='stop after N tasks')
    @argument('--time-limit-hr', metavar='T', type=float, help='stop after T hours')
    def synthesize(self, args):
        count_limit = args.count_limit
        time_limit_hr = args.time_limit_hr

        lc = LimitChecker(count_limit=count_limit, time_limit_hr=time_limit_hr)

        while not self.mck.interrupted and not lc.reached:
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
                               task_claim(id)
                        FROM unsynthesized_task
                        ''', (TaskState.ts_done,))

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
                            ''', (task_id, TaskState.ts_synthesized,
                                  TaskReason.tr_task_synthesize))

            lc.count()

        logger.debug('Exited cleanly.')

    @description('change "cancelled" task state to "waiting"')
    @argument('--count-limit', metavar='N', type=int, help='stop after N tasks')
    @argument('--time-limit-hr', metavar='T', type=float, help='stop after T hours')
    @argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
    @argument('name', nargs='*', help='task name')
    def uncancel(self, args):
        count_limit = args.count_limit
        time_limit_hr = args.time_limit_hr
        name_pattern = args.name_pattern
        names = args.name

        self._simple_state_change(TaskState.ts_cancelled,
                                  TaskState.ts_waiting,
                                  TaskReason.tr_task_uncancel,
                                  name_pattern, names, count_limit=count_limit,
                                  time_limit_hr=time_limit_hr)

    @description('change task state from "cleanable" to "synthesized"')
    @argument('--count-limit', metavar='N', type=int, help='stop after N tasks')
    @argument('--time-limit-hr', metavar='T', type=float, help='stop after T hours')
    @argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
    @argument('name', nargs='*', help='task name')
    def uncleanablize(self, args):
        count_limit = args.count_limit
        time_limit_hr = args.time_limit_hr
        name_pattern = args.name_pattern
        names = args.name

        self._simple_state_change(TaskState.ts_cleanable,
                                  TaskState.ts_synthesized,
                                  TaskReason.tr_task_uncleanablize,
                                  name_pattern, names, count_limit=count_limit,
                                  time_limit_hr=time_limit_hr)
