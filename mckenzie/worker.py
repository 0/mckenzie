from concurrent import futures
from datetime import datetime, timedelta
from enum import IntEnum, unique
import logging
from math import ceil, floor
import os
from pathlib import Path
import shlex
import signal
import subprocess
import threading
from time import sleep

from . import slurm
from .arguments import argparsable, argument, description
from .base import DatabaseEnum, Instance, Manager, preflight
from .database import ForeignKeyViolation
from .task import TaskManager, TaskReason, TaskState
from .util import HandledException, humanize_datetime, mem_rss_mb


logger = logging.getLogger(__name__)


# Worker state flow:
#
#   cancelled      +<---------+
#       ^          |          ^
#       |          v          |
# --> queued -> running -> quitting -> done
#       |          |          |         ^
#       v          v          v         |
#       +----------+------> failed ---->+
#
# queued: The worker should have a pending Slurm job (PD). It's possible that
#         the job has disappeared without us noticing (cancelled manually
#         before running or crashed very early on).
# running: The worker should have a running Slurm job (R). It's possible that
#          the job has disappeared without us noticing (cancelled manually or
#          crashed).
# quitting: Same as running, but with the intention of ending soon.
# cancelled: The worker's Slurm job was cancelled before it could run.
# failed: The worker's Slurm job encountered a problem.
# done: The worker's Slurm job either finished normally or its failure was
#       acknowledged.


@unique
class WorkerState(DatabaseEnum):
    ws_queued = 1
    ws_running = 2
    ws_quitting = 3
    ws_cancelled = 4
    ws_failed = 5
    ws_done = 6

    def user(s):
        # Drop "ws_" prefix.
        return s[3:]

    def unuser(s):
        return 'ws_' + s


@unique
class WorkerReason(DatabaseEnum):
    wr_worker_spawn = 1
    wr_worker_quit_cancelled = 2
    wr_start = 3
    wr_quit = 4
    wr_unquit = 5
    wr_success = 6
    wr_success_idle = 7
    wr_success_abort = 8
    wr_failure = 9
    wr_worker_ack_failed = 10
    wr_worker_clean_queued = 11
    wr_worker_clean_running = 12


@unique
class WorkerMessage(IntEnum):
    wm_quit = 1
    wm_abort = 2
    wm_unquit = 3
    wm_scrap_task = 4

    def user(s):
        # Drop "wm_" prefix.
        return s[3:]

    def unuser(s):
        return 'wm_' + s


class Worker(Instance):
    NUM_EXECUTE_RETRIES = 4

    # 0.2 minutes
    EXECUTE_RETRY_SECONDS = 12
    # 0.1 minutes
    TASK_WAIT_SECONDS = 6
    # 0.05 minutes
    TASK_CLEANUP_WAIT_SECONDS = 3
    # 5 minutes
    GIVE_UP_SECONDS = 300
    # 5 minutes
    IDLE_MAX_SECONDS = 300

    @staticmethod
    def impersonate(slurm_job_id):
        # 00000010 SSSSSSSS SSSSSSSS SSSSSSSS
        return (0x02 << 24) | (slurm_job_id & 0xffffff)

    def __init__(self, slurm_job_id, worker_cpus, worker_mem_mb,
                 time_end_projected, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # These handlers are called inside a transaction, and therefore must be
        # idempotent with respect to nontrivial side-effects.
        self.MESSAGE_HANDLERS = {
            WorkerMessage.wm_quit: self.hdl_quit,
            WorkerMessage.wm_abort: self.hdl_abort,
            WorkerMessage.wm_unquit: self.hdl_unquit,
            WorkerMessage.wm_scrap_task: self.hdl_scrap_task,
        }

        self.slurm_job_id = slurm_job_id
        self.worker_cpus = worker_cpus
        self.worker_mem_mb = worker_mem_mb
        self.time_end_projected = time_end_projected

        ident = self.impersonate(self.slurm_job_id)
        self.db.set_session_parameter('mck.ident', ident)

        self.lock = threading.Lock()

        self._pool = None

        # All the tasks have finished (or will be killed) and worker can exit.
        self.done = False
        # Worker will be done when all the running tasks have finished.
        self.quitting = False

        # Worker exited normally, not through an exception.
        self.clean_exit = False
        # Worker is exiting because it was asked to abort.
        self.done_due_to_abort = False
        # Worker is exiting because it was idle for too long.
        self.done_due_to_idle = False

        # Futures currently executing.
        self.fut_executing = set()
        # PIDs currently executing (mapping from task IDs). This is modified
        # from inside the ThreadPoolExecutor, so self.lock must be acquired for
        # every access.
        self.running_pids = {}
        # Memory usage for tasks that are over their limit.
        self.overmemory_mb = {}
        # Target states for tasks that have been scrapped.
        self.scrapped_tasks = {}

        # Mapping of futures to task names.
        self.fut_task = {}

        # Task names.
        self.task_names = {}
        # Task IDs.
        self.task_ids = {}
        # Times at which tasks start running.
        self.task_starts = {}
        # Expected maximum memory usage of running tasks.
        self.task_mems_mb = {}

        # If there's a lack of tasks, when it began.
        self.idle_start = None
        # Only output the warning once.
        self.rss_fail_warn = False

    @property
    def remaining_time(self):
        return self.time_end_projected - datetime.now()

    @property
    def remaining_mem_mb(self):
        return self.worker_mem_mb - sum(self.task_mems_mb.values())

    def hdl_quit(self, *, tx):
        if self.quitting:
            return

        logger.info('Quitting.')

        self.quitting = True

        tx.execute('''
                INSERT INTO worker_history (worker_id, state_id, reason_id)
                VALUES (%s, %s, %s)
                ''', (self.slurm_job_id, WorkerState.ws_quitting,
                      WorkerReason.wr_quit))

    def quit(self):
        @self.db.tx
        def F(tx):
            self.hdl_quit(tx=tx)

    def hdl_abort(self, *, tx):
        logger.info('Aborting.')

        self.hdl_quit(tx=tx)

        self.done = True
        self.done_due_to_abort = True

    def abort(self, signum=None, frame=None):
        @self.db.tx
        def F(tx):
            self.hdl_abort(tx=tx)

    def hdl_unquit(self, *, tx):
        if not self.quitting:
            return

        if self.done_due_to_abort:
            # Can't unquit if aborting.
            return

        logger.info('Unquitting.')

        self.quitting = False
        self.done_due_to_idle = False

        tx.execute('''
                INSERT INTO worker_history (worker_id, state_id, reason_id)
                VALUES (%s, %s, %s)
                ''', (self.slurm_job_id, WorkerState.ws_running,
                      WorkerReason.wr_unquit))

    def hdl_scrap_task(self, *, tx, task_id, state_id):
        with self.lock:
            try:
                pid = self.running_pids[task_id]
            except KeyError:
                return

        task_name = self.task_names[task_id]
        logger.info(f'Scrapping task "{task_name}" ({pid}).')

        self.scrapped_tasks[task_name] = state_id

        try:
            os.killpg(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def _choose_task(self, task_name_pattern, *, tx):
        query_args = (TaskState.ts_ready, self.remaining_time,
                      self.remaining_mem_mb)

        if task_name_pattern is not None:
            name_clause = ' AND name LIKE %s'
            query_args += (task_name_pattern,)
        else:
            name_clause = ''

        task = tx.execute(f'''
                WITH chosen_task AS (
                    SELECT id, name, mem_limit_mb
                    FROM task
                    WHERE state_id = %s
                    AND claimed_by IS NULL
                    AND time_limit < %s
                    AND mem_limit_mb < %s
                    {name_clause}
                    ORDER BY priority DESC, mem_limit_mb DESC, time_limit DESC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                SELECT id, name, mem_limit_mb
                FROM chosen_task
                WHERE task_claim(id)
                ''', query_args)

        if len(task) == 0:
            return None

        return task[0]

    def _start_task(self, task_id, task_name, task_mem_mb, *, tx):
        tx.execute('''
                INSERT INTO task_history (task_id, state_id, reason_id,
                                          worker_id)
                VALUES (%s, %s, %s, %s)
                ''', (task_id, TaskState.ts_running, TaskReason.tr_running,
                      self.slurm_job_id))

        # Reset the idle timer.
        self.idle_start = None

        self.task_names[task_id] = task_name
        self.task_ids[task_name] = task_id
        self.task_starts[task_name] = datetime.now()
        self.task_mems_mb[task_name] = task_mem_mb

        logger.debug(f'Submitting task "{task_name}" to the pool.')
        fut = self._pool.submit(self._execute_task, task_name)

        self.fut_executing.add(fut)
        self.fut_task[fut] = task_name

    def _execute_task(self, task_name):
        task_id = self.task_ids[task_name]
        args = [self.conf.worker_execute_cmd, task_name]

        for _ in range(self.NUM_EXECUTE_RETRIES):
            try:
                p = subprocess.Popen(args, stdout=subprocess.PIPE, text=True,
                                     # If the child process spawns its own
                                     # processes, we can reliably kill the
                                     # entire process group when needed.
                                     start_new_session=True)
            except OSError:
                sleep(self.EXECUTE_RETRY_SECONDS)
            else:
                break
        else:
            return False, False, '', 0

        with self.lock:
            self.running_pids[task_id] = p.pid

        # Block until the process is done.
        wait_pid, wait_return, wait_usage = os.wait4(p.pid, 0)

        with self.lock:
            self.running_pids.pop(task_id)

        success = wait_pid == p.pid and wait_return == 0

        # The units of ru_maxrss should be KB.
        return True, success, p.stdout.read(), int(wait_usage.ru_maxrss)

    def _handle_completed_task(self, task_name, task_ran, success, output,
                               max_mem_kb):
        task_id = self.task_ids[task_name]
        max_mem_mb = max_mem_kb / 1024

        if success:
            output = output.split('\n')

            # Ignore any empty lines that may have been included after the
            # success string.
            while output and output[-1] == '':
                output.pop()

            ss = self.conf.worker_success_string

            if output and output[-1] == ss:
                reason_id = TaskReason.tr_success
            else:
                success = False
                reason_id = TaskReason.tr_failure_string
        elif not task_ran:
            reason_id = TaskReason.tr_failure_run
        elif task_name in self.overmemory_mb:
            reason_id = TaskReason.tr_failure_memory
        elif task_name in self.scrapped_tasks:
            reason_id = TaskReason.tr_failure_scrapped
        else:
            reason_id = TaskReason.tr_failure_exit_code

        if success:
            state_id = TaskState.ts_done
        else:
            state_id = TaskState.ts_failed

        logger.debug(f'"{task_name}": {success}')

        # Update the task as necessary before giving it up.
        duration = datetime.now() - self.task_starts[task_name]

        @self.db.tx
        def F(tx):
            limit_retry = False
            scrapped_reset = False
            scrapped_state = None

            # Extend the task memory limit if it was underestimated, even if
            # the task finished successfully, because it might get rerun later.
            if task_name in self.overmemory_mb:
                rss_mb = self.overmemory_mb.pop(task_name)
                new_limit_mb = 1.2 * rss_mb

                tx.execute('''
                        UPDATE task
                        SET mem_limit_mb = %s
                        WHERE id = %s
                        AND mem_limit_mb < %s
                        ''', (new_limit_mb, task_id, rss_mb))

                if not success:
                    limit_retry = True

            if task_name in self.scrapped_tasks:
                target_state = self.scrapped_tasks.pop(task_name)

                if not success and target_state != TaskState.ts_failed:
                    scrapped_reset = True

                    if target_state != TaskState.ts_waiting:
                        scrapped_state = target_state

            tx.execute('''
                    UPDATE task
                    SET elapsed_time = %s,
                        max_mem_mb = %s
                    WHERE id = %s
                    ''', (duration, max_mem_mb, task_id))

            tx.execute('''
                    INSERT INTO task_history (task_id, state_id, reason_id,
                                              worker_id)
                    VALUES (%s, %s, %s, %s)
                    ''', (task_id, state_id, reason_id, self.slurm_job_id))

            if scrapped_reset and TaskManager._clean(self.conf, task_name):
                # The task is being scrapped, and the user has requested that
                # we reset it.
                tx.execute('''
                        INSERT INTO task_history (task_id, state_id, reason_id,
                                                  worker_id)
                        VALUES (%s, %s, %s, %s)
                        ''', (task_id, TaskState.ts_waiting,
                              TaskReason.tr_scrapped_reset, self.slurm_job_id))

                if scrapped_state is not None:
                    # Additionally, the user wants the task moved to a certain
                    # state.
                    tx.execute('''
                            INSERT INTO task_history (task_id, state_id,
                                                      reason_id, worker_id)
                            VALUES (%s, %s, %s, %s)
                            ''', (task_id, scrapped_state,
                                  TaskReason.tr_scrapped_moved,
                                  self.slurm_job_id))
            elif limit_retry and TaskManager._clean(self.conf, task_name):
                # The task was unsuccessful and its memory limit was
                # underestimated, but we've increased the limit, and will retry
                # the task automatically.
                tx.execute('''
                        INSERT INTO task_history (task_id, state_id, reason_id,
                                                  worker_id)
                        VALUES (%s, %s, %s, %s)
                        ''', (task_id, TaskState.ts_waiting,
                              TaskReason.tr_limit_retry, self.slurm_job_id))

            # We're completely done with the task, so let it go.
            TaskManager._unclaim(tx, task_id)

        self.task_names.pop(task_id)
        self.task_ids.pop(task_name)
        self.task_starts.pop(task_name)
        self.task_mems_mb.pop(task_name)

    def _run(self):
        # Check messages.
        while True:
            @self.db.tx
            def message_found(tx):
                new_message = tx.execute('''
                        WITH next_message AS (
                            SELECT id
                            FROM worker_message
                            WHERE worker_id = %s
                            AND read_at IS NULL
                            LIMIT 1
                            FOR UPDATE
                        )
                        UPDATE worker_message wm
                        SET read_at = NOW()
                        FROM next_message
                        WHERE wm.id = next_message.id
                        RETURNING type_id, args
                        ''', (self.slurm_job_id,))

                if len(new_message) == 0:
                    return False

                message_type_id, args = new_message[0]
                self.MESSAGE_HANDLERS[message_type_id](tx=tx, **args)

                return True

            if not message_found:
                break

        if self.remaining_time.total_seconds() < self.GIVE_UP_SECONDS:
            # Too little time remaining in job, so we will not take on any
            # more tasks.
            self.quit()

        # Check memory usage.
        with self.lock:
            pids = self.running_pids.copy()

        for task_id, pid in pids.items():
            task_name = self.task_names[task_id]
            rss_mb = mem_rss_mb(pid, log=logger.warning)
            limit_mb = self.task_mems_mb[task_name]

            if rss_mb is None:
                if not self.rss_fail_warn:
                    self.rss_fail_warn = True
                    logger.warning('Failed to get memory usage for task '
                                   f'"{task_name}" ({pid}).')
            elif rss_mb > limit_mb:
                if self.remaining_mem_mb <= rss_mb - limit_mb:
                    logger.debug(f'Killing task "{task_name}" ({pid}) for '
                                 'using too much memory '
                                 f'({rss_mb} MB > {limit_mb} MB).')

                    self.overmemory_mb[task_name] = rss_mb

                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                else:
                    logger.debug('Extending memory limit for task '
                                 f'"{task_name}" from {limit_mb} MB to '
                                 f'{rss_mb} MB.')

                    self.task_mems_mb[task_name] = rss_mb

                    @self.db.tx
                    def F(tx):
                        tx.execute('''
                                UPDATE task
                                SET mem_limit_mb = %s
                                WHERE name = %s
                                AND mem_limit_mb < %s
                                ''', (rss_mb, task_name, rss_mb))

        # Get current pattern.
        @self.db.tx
        def task_name_pattern(tx):
            return tx.execute('''
                    SELECT task_name_pattern
                    FROM worker
                    WHERE id = %s
                    ''', (self.slurm_job_id,))[0][0]

        # Fill up with tasks.
        while (not self.quitting
                and len(self.fut_executing) < self.worker_cpus):
            @self.db.tx
            def task_chosen(tx):
                task = self._choose_task(task_name_pattern, tx=tx)

                if task is None:
                    return False

                self._start_task(*task, tx=tx)

                return True

            if not task_chosen:
                break

        # Update the status.
        @self.db.tx
        def F(tx):
            tx.execute('''
                    UPDATE worker
                    SET heartbeat = NOW(),
                        cur_mem_usage_mb = %s
                    WHERE id = %s
                    ''', (sum(self.task_mems_mb.values()),
                          self.slurm_job_id))

        if self.fut_executing:
            # Wait for running tasks.
            r = futures.wait(self.fut_executing,
                             timeout=self.TASK_WAIT_SECONDS,
                             return_when=futures.FIRST_COMPLETED)
            fut_done, self.fut_executing = r

            for fut in fut_done:
                task_name = self.fut_task.pop(fut)
                self._handle_completed_task(task_name, *fut.result())
        elif self.quitting:
            self.done = True
        else:
            if self.idle_start is None:
                # Start the idle timer.
                self.idle_start = datetime.now()

            idle_sec = (datetime.now() - self.idle_start).total_seconds()

            if idle_sec > self.IDLE_MAX_SECONDS:
                self.quit()
                self.done = True
                self.done_due_to_idle = True
            else:
                # Take a break.
                sleep(self.TASK_WAIT_SECONDS)

    def run(self):
        with futures.ThreadPoolExecutor(max_workers=self.worker_cpus) as pool:
            logger.debug('Diving into pool.')
            self._pool = pool

            try:
                while not self.done:
                    self._run()
            except:
                logger.error('Aborting due to unhandled exception.')

                raise
            finally:
                logger.debug('Leaving pool.')
                self._pool = None

                # Kill any remaining processes.
                with self.lock:
                    to_kill = self.running_pids.copy()

                for task_id, pid in to_kill.items():
                    task_name = self.task_names[task_id]
                    logger.info(f'Killing task "{task_name}" ({pid}).')

                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

                # Collect remaining futures.
                r = futures.wait(self.fut_executing,
                                 timeout=self.TASK_CLEANUP_WAIT_SECONDS,
                                 return_when=futures.ALL_COMPLETED)
                fut_done, self.fut_executing = r

                # If we're here and fut_done isn't empty, something went wrong.
                # There's a chance that we're being terminated by Slurm and are
                # currently in the KillWait window, so we'll receive a SIGKILL
                # very soon and need to run this loop as quickly as possible.
                for fut in fut_done:
                    task_name = self.fut_task.pop(fut)
                    task_id = self.task_ids[task_name]

                    logger.debug(f'Task "{task_name}" was aborted.')

                    # Update the task as necessary before giving it up.
                    duration = datetime.now() - self.task_starts[task_name]

                    @self.db.tx
                    def F(tx):
                        # Extend task time limit if it was underestimated.
                        new_limit = 1.5 * duration

                        tx.execute('''
                                UPDATE task
                                SET time_limit = %s
                                WHERE id = %s
                                AND time_limit < %s
                                ''', (new_limit, task_id, duration))

                        tx.execute('''
                                INSERT INTO task_history (task_id, state_id,
                                                          reason_id, worker_id)
                                VALUES (%s, %s, %s, %s)
                                ''', (task_id, TaskState.ts_failed,
                                      TaskReason.tr_failure_abort,
                                      self.slurm_job_id))

                        if TaskManager._clean(self.conf, task_name):
                            # We will retry the task automatically as long as
                            # we can clean it.
                            tx.execute('''
                                    INSERT INTO task_history (task_id,
                                                              state_id,
                                                              reason_id,
                                                              worker_id)
                                    VALUES (%s, %s, %s, %s)
                                    ''', (task_id, TaskState.ts_waiting,
                                          TaskReason.tr_limit_retry,
                                          self.slurm_job_id))

                        # We're completely done with the task, so let it go.
                        TaskManager._unclaim(tx, task_id)

        self.clean_exit = True
        logger.debug('Left pool normally.')


@argparsable('worker management')
class WorkerManager(Manager, name='worker'):
    # Path to worker output files, relative to work path.
    WORKER_OUTPUT_DIR = Path('worker_output')
    # Worker output file name template.
    WORKER_OUTPUT_FILE_TEMPLATE = 'worker-{}.out'

    # 3 minutes
    EXIT_BUFFER_SECONDS = 180
    # 2 minutes
    END_SIGNAL_SECONDS = 120
    # 2 minutes
    HEARTBEAT_TIMEOUT_SECONDS = 120

    STATE_ORDER = ['cancelled', 'queued', 'running', 'running (?)', 'quitting',
                   'quitting (?)', 'failed', 'done']

    @staticmethod
    def _parse_state(state_name):
        if state_name is None:
            return None

        try:
            return WorkerState[WorkerState.unuser(state_name)]
        except KeyError:
            logger.error(f'Invalid state "{state_name}".')

            raise HandledException()

    def _format_state(self, state_id, job_running, timeout):
        state = WorkerState(state_id).name
        state_user = WorkerState.user(state)
        color = None

        if job_running and timeout:
            state_user += ' (?)'
            color = self.c('warning')
        elif state in 'ws_queued':
            color = self.c('notice')
        elif state == 'ws_running':
            color = self.c('good')
        elif state == 'ws_quitting':
            color = self.c('notice')
        elif state == 'ws_failed':
            color = self.c('error')

        return state, state_user, color

    def _worker_output_file(self, slurm_job_id=None, *, absolute=False):
        if slurm_job_id is None:
            # Replacement symbol for sbatch.
            slurm_job_id = '%j'
        else:
            slurm_job_id = str(slurm_job_id)

        path = (self.WORKER_OUTPUT_DIR
                    / self.WORKER_OUTPUT_FILE_TEMPLATE.format(slurm_job_id))

        if absolute:
            path = self.conf.general_work_path / path

        return path

    def summary(self, args):
        @self.db.tx
        def workers(tx):
            td = timedelta(seconds=self.HEARTBEAT_TIMEOUT_SECONDS)

            return tx.execute('''
                    SELECT w.state_id, ws.job_exists, ws.job_running,
                           worker_timeout(w, %s) AS timeout, SUM(w.num_cores),
                           SUM(w.num_cores * w.time_limit),
                           SUM(w.num_cores * (w.time_start + w.time_limit - NOW())),
                           COUNT(*)
                    FROM worker w
                    JOIN worker_state ws ON ws.id = w.state_id
                    GROUP BY w.state_id, ws.job_exists, ws.job_running, timeout
                    ''', (td,))

        worker_data = []

        for (state_id, job_exists, job_running, timeout, num_tasks, total_time,
                remaining_time, count) in workers:
            state, state_user, state_color \
                    = self._format_state(state_id, job_running, timeout)

            if job_running:
                time = remaining_time
            elif job_exists:
                time = total_time
            else:
                num_tasks = None
                time = None

            worker_data.append([(state_user, state_color), count, num_tasks,
                                time])

        sorted_data = sorted(worker_data,
                             key=lambda row: self.STATE_ORDER.index(row[0][0]))
        self.print_table(['State', 'Count', 'Tasks', 'Remaining time'],
                         sorted_data,
                         total=('Total', (1, 2, 3), (0, 0, timedelta())))

    @description('acknowledge all failed workers')
    def ack_failed(self, args):
        @self.db.tx
        def workers(tx):
            return tx.execute('''
                    SELECT id
                    FROM worker
                    WHERE state_id = %s
                    ''', (WorkerState.ws_failed,))

        for slurm_job_id, in workers:
            if self.mck.interrupted:
                break

            logger.info(slurm_job_id)

            @self.db.tx
            def F(tx):
                tx.execute('''
                        INSERT INTO worker_history (worker_id, state_id,
                                                    reason_id)
                        VALUES (%s, %s, %s)
                        ''', (slurm_job_id, WorkerState.ws_done,
                              WorkerReason.wr_worker_ack_failed))

    @description('list active tasks')
    @argument('--bar-segments', metavar='S', type=int, default=10, help='number of progress bar segments (default: 10)')
    @argument('slurm_job_id', nargs='*', type=int, help='Slurm job ID of worker')
    def active_tasks(self, args):
        S = args.bar_segments
        slurm_job_ids = args.slurm_job_id

        @self.db.tx
        def worker_task(tx):
            query = '''
                    SELECT t.name, t.time_limit, wt.worker_id, w.node,
                           NOW() - wt.time_active AS elapsed_time,
                           wt.time_active + t.time_limit - NOW() AS remaining_time
                    FROM worker_task wt
                    JOIN task t ON t.id = wt.task_id
                    JOIN worker w ON w.id = wt.worker_id
                    WHERE wt.active
                    '''
            query_args = ()

            if slurm_job_ids:
                query += ' AND wt.worker_id = ANY (%s)'
                query_args += (slurm_job_ids,)

            query += '''
                    ORDER BY remaining_time DESC, t.id
                    '''

            return tx.execute(query, query_args)

        task_data = []

        for (task_name, time_limit, slurm_job_id, node, elapsed_time,
                remaining_time) in worker_task:
            if elapsed_time <= time_limit:
                progress = floor(elapsed_time / time_limit * S)
                progress_bar = ('=' * progress) + '>' + ('-' * (S - progress))
                progress_bar = progress_bar[:-1]
                progress_color = None
                percent = ceil(elapsed_time / time_limit * 100)
                percent_str = f'{percent}%'
                percent_color = None
            else:
                progress_bar = '?' * S
                progress_color = self.c('warning')
                percent_str = f'???'
                percent_color = self.c('warning')

            task_data.append([str(slurm_job_id), node, task_name, elapsed_time,
                              remaining_time, time_limit,
                              (progress_bar, progress_color),
                              (percent_str, percent_color)])

        self.print_table(['Job ID', 'Node', 'Task', ('Time (E/R/T)', 3),
                          ('Progress', 2)],
                         task_data)

    @description('clean up dead workers')
    @argument('--state', metavar='S', help='only workers in state S')
    def clean(self, args):
        state_name = args.state

        state_id = self._parse_state(state_name)

        ok_worker_ids = []

        while True:
            if self.mck.interrupted:
                break

            @self.db.tx
            def result(tx):
                query = '''
                        SELECT w.id, w.state_id, ws.job_running
                        FROM worker w
                        JOIN worker_state ws ON ws.id = w.state_id
                        WHERE ws.job_exists
                        AND COALESCE(worker_timeout(w, %s), TRUE)
                        AND w.id != ALL (%s)
                        '''
                td = timedelta(seconds=self.HEARTBEAT_TIMEOUT_SECONDS)
                query_args = (td, ok_worker_ids)

                if state_id is not None:
                    query += ' AND w.state_id = %s'
                    query_args += (state_id,)

                query += '''
                        ORDER BY w.id
                        LIMIT 1
                        FOR UPDATE OF w SKIP LOCKED
                        '''

                worker = tx.execute(query, query_args)

                if len(worker) == 0:
                    return None

                slurm_job_id, worker_state_id, job_running = worker[0]
                ident = Worker.impersonate(slurm_job_id)
                state = WorkerState(worker_state_id).name
                state_user = WorkerState.user(state)

                logger.debug(f'Cleaning worker {slurm_job_id}.')

                if job_running:
                    reason_id = WorkerReason.wr_worker_clean_running
                elif state == 'ws_queued':
                    reason_id = WorkerReason.wr_worker_clean_queued
                else:
                    logger.error(f'Worker {slurm_job_id} is in state '
                                 f'"{state_user}".')

                    return None

                if slurm.does_job_exist(slurm_job_id, log=logger.error):
                    ok_worker_ids.append(slurm_job_id)

                    return False

                worker_tasks = tx.execute('''
                        SELECT t.id, t.name, t.claimed_by
                        FROM worker_task wt
                        JOIN task t ON t.id = wt.task_id
                        WHERE wt.worker_id = %s
                        AND wt.active
                        ORDER BY t.id
                        FOR UPDATE OF t
                        ''', (slurm_job_id,))

                task_names = []

                for task_id, task_name, claimed_by in worker_tasks:
                    logger.debug(f'Updating task "{task_name}".')

                    if claimed_by != ident:
                        logger.error(f'Task "{task_name}" is not claimed by '
                                     f'worker {slurm_job_id}.')

                        raise HandledException()

                    tx.execute('''
                            INSERT INTO task_history (task_id, state_id,
                                                      reason_id)
                            VALUES (%s, %s, %s)
                            ''', (task_id, TaskState.ts_failed,
                                  TaskReason.tr_failure_worker_clean))

                    TaskManager._unclaim(tx, task_id, force=True)

                    task_names.append(task_name)

                tx.execute('''
                        UPDATE worker
                        SET time_end = heartbeat
                        WHERE id = %s
                        AND time_start IS NOT NULL
                        ''', (slurm_job_id,))

                tx.execute('''
                        INSERT INTO worker_history (worker_id, state_id,
                                                    reason_id)
                        VALUES (%s, %s, %s)
                        ''', (slurm_job_id, WorkerState.ws_failed, reason_id))

                return slurm_job_id, task_names

            if result is None:
                break
            elif not result:
                continue

            slurm_job_id, task_names = result

            logger.info(slurm_job_id)

            for task_name in task_names:
                logger.info(f' {task_name}')

    @description('list workers')
    @argument('--state', metavar='S', help='only workers in state S')
    def list(self, args):
        state_name = args.state

        state_id = self._parse_state(state_name)

        @self.db.tx
        def workers(tx):
            query = '''
                    SELECT w.id, w.state_id, ws.job_exists, ws.job_running,
                           w.num_cores, w.time_limit, w.mem_limit_mb,
                           w.task_name_pattern, w.node, w.time_start,
                           w.time_end, w.num_tasks, w.num_tasks_active,
                           w.cur_mem_usage_mb, worker_timeout(w, %s),
                           NOW() - w.time_start
                    FROM worker w
                    JOIN worker_state ws ON ws.id = w.state_id
                    '''
            td = timedelta(seconds=self.HEARTBEAT_TIMEOUT_SECONDS)
            query_args = (td,)

            if state_id is not None:
                query += ' WHERE w.state_id = %s'
                query_args += (state_id,)
            else:
                query += ' WHERE (ws.job_running OR w.state_id = %s)'
                query_args += (WorkerState.ws_failed,)

            # Order by remaining time, putting failed workers at the bottom.
            query += '''
                    ORDER BY w.state_id = %s,
                             w.time_start + w.time_limit - NOW() DESC
                    '''
            query_args += (WorkerState.ws_failed,)

            return tx.execute(query, query_args)

        worker_data = []

        for (slurm_job_id, state_id, job_exists, job_running, num_cores,
                time_limit, mem_limit_mb, task_name_pattern, node, time_start,
                time_end, num_tasks, num_tasks_active, cur_mem_usage_mb,
                timeout, elapsed_time) in workers:
            state, state_user, state_color \
                    = self._format_state(state_id, job_running, timeout)

            remaining_time = '-'

            if job_exists:
                if time_start is None:
                    remaining_time = time_limit
                elif time_end is None:
                    remaining_time = time_limit - elapsed_time

            if job_running:
                tasks_running_show = num_tasks_active
                tasks_frac = num_tasks_active / num_cores
                tasks_percent = ceil(tasks_frac * 100)
                tasks_percent_str = f'{tasks_percent}%'
            else:
                tasks_running_show = '-'
                tasks_percent = None
                tasks_percent_str = '-'

            mem_limit_gb = ceil(mem_limit_mb / 1024)

            if job_running and cur_mem_usage_mb is not None:
                cur_mem_usage_gb = ceil(cur_mem_usage_mb / 1024)
                cur_mem_usage_frac = cur_mem_usage_mb / mem_limit_mb
                cur_mem_usage_percent = ceil(cur_mem_usage_frac * 100)
                cur_mem_usage_percent_str = f'{cur_mem_usage_percent}%'
            else:
                cur_mem_usage_gb = '-'
                cur_mem_usage_percent = None
                cur_mem_usage_percent_str = '-'

            # Color based on resource usage.
            if state == 'ws_quitting':
                pass
            elif (tasks_percent is not None and tasks_percent < 95
                    and cur_mem_usage_percent is not None
                    and cur_mem_usage_percent < 80):
                tasks_percent_str = (tasks_percent_str, self.c('error'))
                cur_mem_usage_percent_str = (cur_mem_usage_percent_str,
                                             self.c('error'))
            elif tasks_percent is not None and tasks_percent < 95:
                tasks_percent_str = (tasks_percent_str, self.c('warning'))
            elif (cur_mem_usage_percent is not None
                    and cur_mem_usage_percent < 80):
                cur_mem_usage_percent_str = (cur_mem_usage_percent_str,
                                             self.c('warning'))

            worker_data.append([str(slurm_job_id), (state_user, state_color),
                                node, remaining_time, time_limit,
                                tasks_running_show, num_cores,
                                tasks_percent_str, num_tasks, cur_mem_usage_gb,
                                mem_limit_gb, cur_mem_usage_percent_str,
                                task_name_pattern])

        self.print_table(['Job ID', 'State', 'Node', ('Time (R/T)', 2),
                          ('Tasks (R/C/%/T)', 4), ('Mem (GB;U/T/%)', 3),
                          'Pattern'],
                         worker_data)

    @description('list completed worker jobs')
    @argument('--last-hr', metavar='T', type=float, required=True, help='jobs completed in the last T hours')
    @preflight(database_init=False, database_current=False)
    def list_completed(self, args):
        last = timedelta(hours=args.last_hr)

        columns = ['JobID', 'AllocCPUS', 'TotalCPU', 'CPUTime', 'MaxRSS',
                   'ReqMem', 'MaxDiskRead', 'MaxDiskWrite', 'End']
        jobs = slurm.list_completed_jobs(self.conf.worker_job_name, columns,
                                         last, log=logger.error)

        worker_data = []

        for (jobid, cpus, cpu_used, cpu_total, mem_used, mem_total, disk_read,
                disk_write, time_end) in jobs:
            now = datetime.now()

            if not jobid.endswith('.batch'):
                continue

            jobid = jobid[:-6]

            cpus = int(cpus)
            cpu_used = slurm.parse_timedelta(cpu_used)
            cpu_total = slurm.parse_timedelta(cpu_total)
            cpu_percent_str = f'{ceil(cpu_used / cpu_total * 100)}%'
            mem_used_mb = ceil(slurm.parse_units_mb(mem_used))
            mem_total_mb = ceil(slurm.parse_units_mb(mem_total[:-1]))

            if mem_total[-1] == 'n':
                # By node.
                pass
            elif mem_total[-1] == 'c':
                # By CPU.
                mem_total_mb *= cpus
            else:
                logger.error('Invalid suffix.')

                raise HandledException()

            mem_percent_str = f'{ceil(mem_used_mb / mem_total_mb * 100)}%'
            disk_read_mb = ceil(slurm.parse_units_mb(disk_read))
            disk_write_mb = ceil(slurm.parse_units_mb(disk_write))

            try:
                dt = datetime.fromisoformat(time_end)
            except ValueError:
                pass
            else:
                time_end = humanize_datetime(dt, now)

            worker_data.append([jobid, cpus, cpu_used, cpu_total,
                                cpu_percent_str, mem_used_mb, mem_total_mb,
                                mem_percent_str, disk_read_mb, disk_write_mb,
                                time_end])

        self.print_table(['Job ID', 'Cores', ('CPU (U/T/%)', 3),
                          ('Mem (MB;U/T/%)', 3), ('Disk (MB;R/W)', 2), 'End'],
                         worker_data)

    @description('list queued workers')
    def list_queued(self, args):
        @self.db.tx
        def workers(tx):
            return tx.execute('''
                    SELECT num_cores, time_limit, mem_limit_mb,
                           MAX(time_start), NOW(),
                           ARRAY_AGG(task_name_pattern) FILTER
                               (WHERE state_id = %s),
                           COUNT(*) FILTER (WHERE state_id = %s)
                    FROM worker
                    GROUP BY num_cores, time_limit, mem_limit_mb
                    ORDER BY MAX(time_start) NULLS FIRST,
                             num_cores, time_limit, mem_limit_mb
                    ''', (WorkerState.ws_queued, WorkerState.ws_queued))

        worker_data = []

        for (num_cores, time_limit, mem_limit_mb, max_time_start, now,
                task_name_patterns, count) in workers:
            mem_limit_gb = ceil(mem_limit_mb / 1024)

            if max_time_start is not None:
                max_time_start = humanize_datetime(max_time_start, now)
            else:
                max_time_start = '-'

            if task_name_patterns is not None:
                patterns_raw = set(task_name_patterns)
                patterns = [x if x is not None else '%' for x in patterns_raw]
                patterns_str = ', '.join(sorted(patterns))
            else:
                patterns_str = ''

            if count == 0:
                count = '-'

            worker_data.append([num_cores, time_limit, mem_limit_gb, count,
                                max_time_start, patterns_str])

        self.print_table(['Cores', 'Time', 'Mem (GB)', 'Count',
                          'Most recent start', 'Patterns'],
                         worker_data)

    @description('request worker job to quit')
    @argument('--abort', action='store_true', help='quit immediately, killing running tasks')
    @argument('--all', action='store_true', help='request all worker jobs')
    @argument('--state', metavar='S', help='only workers in state S for "--all"')
    @argument('slurm_job_id', nargs='*', type=int, help='Slurm job ID of worker')
    def quit(self, args):
        abort = args.abort
        all_workers = args.all
        state_name = args.state
        slurm_job_ids = set(args.slurm_job_id)

        if abort:
            message_type_id = WorkerMessage.wm_abort
        else:
            message_type_id = WorkerMessage.wm_quit

        state_id = self._parse_state(state_name)

        if all_workers:
            @self.db.tx
            def workers(tx):
                # All workers with jobs.
                query = '''
                        SELECT w.id
                        FROM worker w
                        JOIN worker_state ws ON ws.id = w.state_id
                        WHERE ws.job_exists
                        '''
                query_args = ()

                if state_id is not None:
                    query += ' AND w.state_id = %s'
                    query_args += (state_id,)

                return tx.execute(query, query_args)

            for slurm_job_id, in workers:
                slurm_job_ids.add(slurm_job_id)

        for slurm_job_id in slurm_job_ids:
            if self.mck.interrupted:
                break

            # Only try to cancel pending jobs.
            logger.debug(f'Attempting to cancel worker {slurm_job_id}.')
            cancel_success, signalled_running \
                    = slurm.cancel_job(slurm_job_id,
                                       name=self.conf.worker_job_name,
                                       signal=signal, log=logger.error,
                                       running=False)

            logger.info(slurm_job_id)

            if cancel_success:
                @self.db.tx
                def F(tx):
                    tx.execute('''
                            INSERT INTO worker_history (worker_id, state_id,
                                                        reason_id)
                            VALUES (%s, %s, %s)
                            ''', (slurm_job_id, WorkerState.ws_cancelled,
                                  WorkerReason.wr_worker_quit_cancelled))
            else:
                # If we couldn't cancel it, we assume that it's running and
                # send it a message.
                @self.db.tx
                def F(tx):
                    tx.execute('''
                            INSERT INTO worker_message (worker_id, type_id,
                                                        args)
                            VALUES (%s, %s, %s)
                            ''', (slurm_job_id, message_type_id, {}))

    @description('run worker inside Slurm job')
    # Prefer to wait for the database to become available rather than quitting
    # immediately and losing the allocation.
    @preflight(database_init=False)
    def run(self, args):
        slurm_job_id = slurm.get_job_id(log=logger.error)
        worker_node, worker_cpus, worker_mem_mb = slurm.get_job_variables()

        @self.db.tx
        def time_limit(tx):
            worker = tx.execute('''
                    SELECT time_limit
                    FROM worker
                    WHERE id = %s
                    FOR UPDATE
                    ''', (slurm_job_id,))

            if len(worker) == 0:
                logger.error(f'Worker {slurm_job_id} not found.')

                return None

            time_limit, = worker[0]

            tx.execute('''
                    UPDATE worker
                    SET node = %s,
                        time_start = NOW(),
                        heartbeat = NOW(),
                        cur_mem_usage_mb = 0
                    WHERE id = %s
                    ''', (worker_node, slurm_job_id))

            tx.execute('''
                    INSERT INTO worker_history (worker_id, state_id, reason_id)
                    VALUES (%s, %s, %s)
                    ''', (slurm_job_id, WorkerState.ws_running,
                          WorkerReason.wr_start))

            return time_limit

        if time_limit is None:
            return

        time_end_projected = (datetime.now() + time_limit
                              - timedelta(seconds=self.EXIT_BUFFER_SECONDS)
                              - timedelta(seconds=self.END_SIGNAL_SECONDS))

        logger.info(f'Starting worker {slurm_job_id}.')
        worker = Worker(slurm_job_id, worker_cpus, worker_mem_mb,
                        time_end_projected, self.mck)

        signal.signal(signal.SIGTERM, worker.abort)

        try:
            worker.run()
            logger.debug('Success!')
        finally:
            logger.info('Worker done.')

            signal.signal(signal.SIGTERM, signal.SIG_DFL)

            if not worker.clean_exit:
                state_id = WorkerState.ws_failed
                reason_id = WorkerReason.wr_failure
            else:
                state_id = WorkerState.ws_done

                if worker.done_due_to_abort:
                    reason_id = WorkerReason.wr_success_abort
                elif worker.done_due_to_idle:
                    reason_id = WorkerReason.wr_success_idle
                else:
                    reason_id = WorkerReason.wr_success

            @self.db.tx
            def F(tx):
                tx.execute('''
                        UPDATE worker
                        SET time_end = NOW()
                        WHERE id = %s
                        ''', (slurm_job_id,))

                tx.execute('''
                        INSERT INTO worker_history (worker_id, state_id,
                                                    reason_id)
                        VALUES (%s, %s, %s)
                        ''', (slurm_job_id, state_id, reason_id))

    @description('scrap running tasks')
    @argument('--state', metavar='S', help='target task state (default: failed)')
    @argument('name', nargs='*', help='task name')
    def scrap_task(self, args):
        task_state_name = args.state
        task_names = args.name

        if task_state_name is None:
            task_state_id = TaskState.ts_failed
        else:
            task_state_id = TaskManager._parse_state(task_state_name)

            if task_state_id not in [TaskState.ts_failed,
                                     TaskState.ts_waiting]:
                @self.db.tx
                def allowed_states(tx):
                    return tx.execute('''
                            SELECT to_state_id
                            FROM task_state_transition
                            WHERE from_state_id = %s
                            AND free_transition
                            ''', (TaskState.ts_waiting,))

                for candidate_state_id, in allowed_states:
                    if task_state_id == candidate_state_id:
                        break
                else:
                    logger.error(f'Cannot set state to "{task_state_name}".')

                    raise HandledException()

        for task_name in task_names:
            if self.mck.interrupted:
                break

            @self.db.tx
            def info(tx):
                return tx.execute('''
                        SELECT t.id, wt.worker_id
                        FROM task t
                        LEFT JOIN worker_task wt ON (wt.task_id = t.id
                                                     AND wt.active)
                        WHERE t.name = %s
                        ''', (task_name,))

            if len(info) == 0:
                logger.error(f'Task "{task_name}" does not exist.')

                continue

            task_id, worker_id = info[0]

            if worker_id is None:
                logger.error(f'Task "{task_name}" is not running.')

                continue

            logger.info(task_name)

            @self.db.tx
            def F(tx):
                tx.execute('''
                        INSERT INTO worker_message (worker_id, type_id, args)
                        VALUES (%s, %s, %s)
                        ''', (worker_id, WorkerMessage.wm_scrap_task,
                              {'task_id': task_id, 'state_id': task_state_id}))

    @description('show worker details')
    @argument('--inactive-tasks', action='store_true', help='include inactive tasks')
    @argument('slurm_job_id', type=int, help='Slurm job ID of worker')
    def show(self, args):
        hide_inactive_tasks = not args.inactive_tasks
        slurm_job_id = args.slurm_job_id

        @self.db.tx
        def worker(tx):
            return tx.execute('''
                    SELECT num_cores, time_limit, mem_limit_mb, node,
                           heartbeat, NOW()
                    FROM worker
                    WHERE id = %s
                    ''', (slurm_job_id,))

        if len(worker) == 0:
            logger.error(f'Worker "{slurm_job_id}" does not exist.')

            return

        num_cores, time_limit, mem_limit_mb, node, heartbeat, now = worker[0]

        mem_limit_gb = ceil(mem_limit_mb / 1024)
        node = node if node is not None else '-'

        if heartbeat is not None:
            heartbeat = humanize_datetime(heartbeat, now)
        else:
            heartbeat = '-'

        self.print_table(['Job ID', 'Cores', 'Time', 'Mem (GB)', 'Node',
                          'Heartbeat'],
                         [[str(slurm_job_id), num_cores, time_limit,
                           mem_limit_gb, node, heartbeat]])

        print()

        @self.db.tx
        def worker_history(tx):
            return tx.execute('''
                    SELECT wh.state_id, wh.time,
                           LEAD(wh.time, 1, NOW())
                               OVER (ORDER BY wh.time, wh.id),
                           wr.description
                    FROM worker_history wh
                    JOIN worker_reason wr ON wr.id = wh.reason_id
                    WHERE wh.worker_id = %s
                    ORDER BY wh.id
                    ''', (slurm_job_id,))

        worker_data = []

        for state_id, time, time_next, reason in worker_history:
            state_user = WorkerState.user(WorkerState(state_id).name)
            duration = time_next - time

            worker_data.append([time, duration, state_user, reason])

        if worker_data:
            self.print_table(['Time', 'Duration', 'State', 'Reason'],
                             worker_data)
        else:
            print('No state history.')

        print()

        if hide_inactive_tasks:
            @self.db.tx
            def hidden_inactive_tasks(tx):
                return tx.execute('''
                        SELECT COUNT(*)
                        FROM worker_task
                        WHERE worker_id = %s
                        AND NOT active
                        ''', (slurm_job_id,))[0][0]
        else:
            hidden_inactive_tasks = 0

        @self.db.tx
        def worker_task(tx):
            query = '''
                    SELECT t.name, wt.time_active, wt.time_inactive,
                           NOW() - wt.time_active
                    FROM worker_task wt
                    JOIN task t ON t.id = wt.task_id
                    WHERE wt.worker_id = %s
                    '''
            query_args = (slurm_job_id,)

            if hide_inactive_tasks:
                query += ' AND wt.active'

            query += '''
                    ORDER BY wt.id
                    '''

            return tx.execute(query, query_args)

        task_data = []

        for task_name, time_active, time_inactive, time_since in worker_task:
            if time_inactive is not None:
                duration = time_inactive - time_active
            else:
                duration = time_since

            task_data.append([task_name, time_active, time_inactive, duration])

        if task_data:
            self.print_table(['Task', 'Active at', 'Inactive at', 'Duration'],
                             task_data)

            if hidden_inactive_tasks >= 1:
                print()
        else:
            if hidden_inactive_tasks >= 1:
                print('No active tasks. ', end='')
            else:
                print('No worker tasks.')

        if hidden_inactive_tasks == 1:
            print('Omitted 1 inactive task.')
        elif hidden_inactive_tasks >= 2:
            print(f'Omitted {hidden_inactive_tasks} inactive tasks.')

        print()

        worker_output_file = self._worker_output_file(slurm_job_id,
                                                      absolute=True)

        try:
            with open(worker_output_file) as f:
                lines = f.readlines()[-10:]
        except OSError as e:
            print(f'Could not open "{worker_output_file}".')
        else:
            print(f'==> {worker_output_file} <==')

            for line in lines:
                print(line.strip())

    @description('spawn Slurm worker job')
    @argument('--cpus', metavar='C', type=int, required=True, help='number of CPUs')
    @argument('--time-hr', metavar='T', type=float, required=True, help='time limit in hours')
    @argument('--mem-gb', metavar='M', type=float, required=True, help='amount of memory in GB')
    @argument('--sbatch-args', metavar='SA', help='additional arguments to pass to sbatch')
    @argument('--task-name-pattern', metavar='P', help='only run tasks with names matching the SQL LIKE pattern P')
    @argument('--num', type=int, default=1, help='number of workers to spawn (default: 1)')
    def spawn(self, args):
        worker_cpus = args.cpus
        worker_time_hours = args.time_hr
        worker_mem_gb = args.mem_gb
        sbatch_args = args.sbatch_args
        task_name_pattern = args.task_name_pattern
        num = args.num

        if num < 1:
            logger.error('Must spawn at least 1 worker.')

            return

        mck_cmd = shlex.quote(self.conf.worker_mck_cmd)

        if self.conf.worker_mck_args is not None:
            mck_args = self.conf.worker_mck_args
        else:
            mck_args = ''

        script = f'''
                #!/bin/bash

                export PYTHONUNBUFFERED=1
                exec {mck_cmd} {mck_args} worker run
                '''

        submitter = slurm.JobSubmitter(name=self.conf.worker_job_name,
                                       signal='TERM',
                                       signal_seconds=self.END_SIGNAL_SECONDS,
                                       chdir_path=self.conf.general_work_path,
                                       output_file_path=self._worker_output_file(),
                                       cpus=worker_cpus,
                                       time_hours=worker_time_hours,
                                       mem_gb=worker_mem_gb,
                                       sbatch_argss=[self.conf.general_sbatch_args,
                                                     self.conf.worker_sbatch_args,
                                                     sbatch_args],
                                       script=script,
                                       hold=True)

        for _ in range(num):
            if self.mck.interrupted:
                break

            logger.debug('Spawning worker job.')
            slurm_job_id = submitter.submit(log=logger.error)
            logger.info(slurm_job_id)

            worker_time = timedelta(hours=worker_time_hours)
            worker_mem_mb = ceil(worker_mem_gb * 1024)

            @self.db.tx
            def F(tx):
                tx.execute('''
                        INSERT INTO worker (id, state_id, num_cores,
                                            time_limit, mem_limit_mb,
                                            task_name_pattern)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ''', (slurm_job_id, WorkerState.ws_queued, worker_cpus,
                              worker_time, worker_mem_mb, task_name_pattern))

                tx.execute('''
                        INSERT INTO worker_history (worker_id, state_id,
                                                    reason_id)
                        VALUES (%s, %s, %s)
                        ''', (slurm_job_id, WorkerState.ws_queued,
                              WorkerReason.wr_worker_spawn))

            logger.debug(f'Releasing worker job {slurm_job_id}.')
            slurm.release_job(slurm_job_id, log=logger.error)

    @description('request worker job to resume running')
    @argument('--all', action='store_true', help='request all worker jobs')
    @argument('slurm_job_id', nargs='*', type=int, help='Slurm job ID of worker')
    def unquit(self, args):
        all_workers = args.all
        slurm_job_ids = set(args.slurm_job_id)

        if all_workers:
            @self.db.tx
            def workers(tx):
                # All quitting workers.
                return tx.execute('''
                        SELECT w.id
                        FROM worker w
                        WHERE state_id = %s
                        ''', (WorkerState.ws_quitting,))

            for slurm_job_id, in workers:
                slurm_job_ids.add(slurm_job_id)

        for slurm_job_id in slurm_job_ids:
            if self.mck.interrupted:
                break

            logger.info(slurm_job_id)

            @self.db.tx
            def F(tx):
                tx.execute('''
                        INSERT INTO worker_message (worker_id, type_id, args)
                        VALUES (%s, %s, %s)
                        ''', (slurm_job_id, WorkerMessage.wm_unquit, {}))


@argparsable('worker debugging')
class WorkerDebugManager(Manager, name='worker.debug'):
    @staticmethod
    def _parse_message_type(message_type_name):
        if message_type_name is None:
            return None

        try:
            return WorkerMessage[WorkerMessage.unuser(message_type_name)]
        except KeyError:
            logger.error(f'Invalid message type "{message_type_name}".')

            raise HandledException()

    def summary(self, args):
        logger.info('No action specified.')

    @description('list worker messages')
    @argument('--type', metavar='T', help='type of message')
    @argument('--read', action='store_true', help='only read messages')
    @argument('--unread', action='store_true', help='only unread messages')
    @argument('--done', action='store_true', help='include done workers')
    @argument('slurm_job_id', nargs='*', type=int, help='Slurm job ID of worker')
    def list_messages(self, args):
        message_type_id = self._parse_message_type(args.type)
        only_read = args.read
        only_unread = args.unread
        done = args.done
        slurm_job_ids = args.slurm_job_id

        @self.db.tx
        def messages(tx):
            query = '''
                    SELECT wm.worker_id, wm.time, wm.type_id, wm.args,
                           wm.read_at
                    FROM worker_message wm
                    JOIN worker w ON w.id = wm.worker_id
                    WHERE TRUE
                    '''
            query_args = ()

            if message_type_id is not None:
                query += ' AND wm.type_id = %s'
                query_args += (message_type_id,)

            if only_read:
                # Arbitrarily, --read takes precedence over --unread.
                query += ' AND wm.read_at IS NOT NULL'
            elif only_unread:
                query += ' AND wm.read_at IS NULL'

            if slurm_job_ids:
                query += ' AND wm.worker_id = ANY (%s)'
                query_args += (slurm_job_ids,)
            elif not done:
                query += ' AND w.state_id != %s'
                query_args += (WorkerState.ws_done,)

            query += ' ORDER BY wm.time, wm.id'

            return tx.execute(query, query_args)

        message_data = []

        for (slurm_job_id, time, message_type_id, message_args,
                read_at) in messages:
            message_type = WorkerMessage(message_type_id).name
            message_type_user = WorkerMessage.user(message_type)

            if read_at is not None:
                read_after = read_at - time
            else:
                read_after = '-'

            message_data.append([str(slurm_job_id), time, message_type_user,
                                 message_args, read_after])

        self.print_table(['Worker', 'Sent at', 'Type', 'Args', 'Read after'],
                         message_data)

    @description('send a message to workers')
    @argument('--type', metavar='T', required=True, help='type of message')
    @argument('--args', metavar='A', required=True, help='message args as a JSON string')
    @argument('slurm_job_id', nargs='*', type=int, help='Slurm job ID of worker')
    def send_message(self, args):
        message_type_id = self._parse_message_type(args.type)
        message_args = args.args
        slurm_job_ids = set(args.slurm_job_id)

        for slurm_job_id in slurm_job_ids:
            if self.mck.interrupted:
                break

            @self.db.tx
            def success(tx):
                try:
                    tx.execute('''
                            INSERT INTO worker_message (worker_id, type_id,
                                                        args)
                            VALUES (%s, %s, %s)
                            ''', (slurm_job_id, message_type_id, message_args))
                except ForeignKeyViolation as e:
                    if e.constraint_name == 'worker_message_worker_id_fkey':
                        logger.error(f'Worker "{slurm_job_id}" does not '
                                     'exist.')
                        tx.rollback()

                        return False
                    else:
                        raise

                return True

            if success:
                logger.info(slurm_job_id)
