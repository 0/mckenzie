from concurrent import futures
from datetime import datetime, timedelta
import logging
from math import ceil
import os
from pathlib import Path
import shlex
import signal
import subprocess
import threading
from time import sleep

from .base import DatabaseReasonView, DatabaseStateView, Instance, Manager
from .task import TaskManager, TaskReason, TaskState
from .util import (HandledException, check_proc, check_scancel,
                   humanize_datetime, mem_rss_mb)


logger = logging.getLogger(__name__)


# Worker state flow:
#
#   cancelled
#       ^
#       |
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


class WorkerState(DatabaseStateView):
    def __init__(self, *args, **kwargs):
        super().__init__('worker_state', 'ws_', *args, **kwargs)


class WorkerReason(DatabaseReasonView):
    def __init__(self, *args, **kwargs):
        super().__init__('worker_reason', *args, **kwargs)


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

        self.slurm_job_id = slurm_job_id
        self.worker_cpus = worker_cpus
        self.worker_mem_mb = worker_mem_mb
        self.time_end_projected = time_end_projected

        self._ident = self.impersonate(self.slurm_job_id)

        self.lock = threading.Lock()

        self._ws = WorkerState(self.db)
        self._wr = WorkerReason(self.db)

        self._ts = TaskState(self.db)
        self._tr = TaskReason(self.db)

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
        # PIDs currently executing (mapping to task names). This is modified
        # from inside the ThreadPoolExecutor, so self.lock must be acquired for
        # every access.
        self.running_pids = {}

        # Mapping of futures to task IDs and names.
        self.fut_task = {}

        # Times at which tasks start running.
        self.task_starts = {}
        # Expected maximum memory usage of running tasks.
        self.task_mems_mb = {}

        # If there's a lack of tasks, when it began.
        self.idle_start = None

    @property
    def remaining_time(self):
        return self.time_end_projected - datetime.now()

    def quit(self, signum=None, frame=None):
        if self.quitting:
            return

        logger.info('Quitting.')

        self.quitting = True

        @self.db.tx
        def F(tx):
            tx.execute('''
                    INSERT INTO worker_history (worker_id, state_id, reason_id)
                    VALUES (%s, %s, %s)
                    ''', (self.slurm_job_id, self._ws.rlookup('ws_quitting'),
                          self._wr.rlookup('wr_quit')))

    def abort(self, signum=None, frame=None):
        logger.info('Aborting.')

        self.quit()

        self.done = True
        self.done_due_to_abort = True

    def _choose_task(self):
        remaining_mem_mb = self.worker_mem_mb - sum(self.task_mems_mb.values())

        @self.db.tx
        def task(tx):
            return tx.execute('''
                    WITH chosen_task AS (
                        SELECT id, name, mem_limit_mb
                        FROM task
                        WHERE state_id = %s
                        AND claimed_by IS NULL
                        AND time_limit < %s
                        AND mem_limit_mb < %s
                        ORDER BY priority DESC, mem_limit_mb DESC,
                                 time_limit DESC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    SELECT id, name, mem_limit_mb
                    FROM chosen_task
                    WHERE task_claim(id, %s)
                    ''', (self._ts.rlookup('ts_ready'), self.remaining_time,
                          remaining_mem_mb, self.ident))

        if len(task) == 0:
            return None

        task_id, task_name, task_mem_mb = task[0]

        @self.db.tx
        def F(tx):
            tx.execute('''
                    INSERT INTO task_history (task_id, state_id, reason_id,
                                              worker_id)
                    VALUES (%s, %s, %s, %s)
                    ''', (task_id, self._ts.rlookup('ts_running'),
                          self._tr.rlookup('tr_running'), self.slurm_job_id))

        return task_id, task_name, task_mem_mb

    def _execute_task(self, task_name):
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
            # We're assuming PID reuse is not a problem.
            self.running_pids[p.pid] = task_name

        # Block until the process is done.
        wait_pid, wait_return, wait_usage = os.wait4(p.pid, 0)

        with self.lock:
            self.running_pids.pop(p.pid)

        success = wait_pid == p.pid and wait_return == 0

        # The units of ru_maxrss should be KB.
        return True, success, p.stdout.read(), int(wait_usage.ru_maxrss)

    def _run(self, pool):
        # Memory usage for tasks that are over their limit.
        overmemory_mb = {}
        # Only output the warning once.
        rss_fail_warn = False

        while not self.done:
            if self.remaining_time.total_seconds() < self.GIVE_UP_SECONDS:
                # Too little time remaining in job, so we will not take on any
                # more tasks.
                self.quit()

            # Check memory usage.
            with self.lock:
                pids = self.running_pids.copy()

            for pid, task_name in pids.items():
                rss_mb = mem_rss_mb(pid, log=logger.warning)
                limit_mb = self.task_mems_mb[task_name]

                if rss_mb is None:
                    if not rss_fail_warn:
                        rss_fail_warn = True
                        logger.warning('Failed to get memory usage for task '
                                       f'"{task_name}" ({pid}).')
                elif rss_mb > limit_mb:
                    logger.info(f'Killing task "{task_name}" ({pid}) for '
                                'using too much memory '
                                f'({rss_mb} MB > {limit_mb} MB).')

                    overmemory_mb[task_name] = rss_mb

                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

            # Fill up with tasks.
            while (not self.quitting
                    and len(self.fut_executing) < self.worker_cpus):
                task = self._choose_task()

                if task is None:
                    break

                # Reset the idle timer.
                self.idle_start = None

                task_id, task_name, task_mem_mb = task

                self.task_starts[task_name] = datetime.now()
                self.task_mems_mb[task_name] = task_mem_mb

                logger.debug(f'Submitting task "{task_name}" to the pool.')
                fut = pool.submit(self._execute_task, task_name)

                self.fut_executing.add(fut)
                self.fut_task[fut] = task_id, task_name

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
                    # Handle completed task.
                    task_id, task_name = self.fut_task.pop(fut)
                    task_ran, success, output, max_mem_kb = fut.result()
                    max_mem_mb = max_mem_kb / 1024

                    if success:
                        output = output.split('\n')

                        # Ignore any empty lines that may have been included
                        # after the success string.
                        while output and output[-1] == '':
                            output.pop()

                        ss = self.conf.worker_success_string

                        if output and output[-1] == ss:
                            reason_id = self._tr.rlookup('tr_success')
                        else:
                            success = False
                            reason_id = self._tr.rlookup('tr_failure_string')
                    elif not task_ran:
                        reason_id = self._tr.rlookup('tr_failure_run')
                    elif task_name in overmemory_mb:
                        reason_id = self._tr.rlookup('tr_failure_memory')
                    else:
                        reason_id = self._tr.rlookup('tr_failure_exit_code')

                    if success:
                        state_id = self._ts.rlookup('ts_done')
                    else:
                        state_id = self._ts.rlookup('ts_failed')

                    logger.debug(f'"{task_name}": {success}')

                    # Update the task as necessary before giving it up.
                    duration = datetime.now() - self.task_starts[task_name]

                    @self.db.tx
                    def F(tx):
                        limit_retry = False

                        # Extend the task memory limit if it was
                        # underestimated, even if the task finished
                        # successfully, because it might get rerun later.
                        if task_name in overmemory_mb:
                            rss_mb = overmemory_mb.pop(task_name)
                            new_limit_mb = 1.2 * rss_mb

                            tx.execute('''
                                    UPDATE task
                                    SET mem_limit_mb = %s
                                    WHERE id = %s
                                    AND mem_limit_mb < %s
                                    ''', (new_limit_mb, task_id, rss_mb))

                            if not success:
                                limit_retry = True

                        tx.execute('''
                                UPDATE task
                                SET elapsed_time = %s,
                                    max_mem_mb = %s
                                WHERE id = %s
                                ''', (duration, max_mem_mb, task_id))

                        tx.execute('''
                                INSERT INTO task_history (task_id, state_id,
                                                          reason_id, worker_id)
                                VALUES (%s, %s, %s, %s)
                                ''', (task_id, state_id, reason_id,
                                      self.slurm_job_id))

                        if (limit_retry
                                and TaskManager._clean(self.conf, task_name)):
                            # The task was unsuccessful and its memory limit
                            # was underestimated. We've increased the limit,
                            # and will retry the task automatically as long as
                            # we can clean it.
                            tx.execute('''
                                    INSERT INTO task_history (task_id,
                                                              state_id,
                                                              reason_id,
                                                              worker_id)
                                    VALUES (%s, %s, %s, %s)
                                    ''', (task_id,
                                          self._ts.rlookup('ts_waiting'),
                                          self._tr.rlookup('tr_limit_retry'),
                                          self.slurm_job_id))

                        # We're completely done with the task, so let it go.
                        TaskManager._unclaim(tx, task_id, self.ident)

                    self.task_starts.pop(task_name)
                    self.task_mems_mb.pop(task_name)
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

            try:
                self._run(pool)
            except:
                logger.error('Aborting due to unhandled exception.')

                raise
            finally:
                logger.debug('Leaving pool.')

                # Kill any remaining processes.
                with self.lock:
                    to_kill = self.running_pids.copy()

                for pid, task_name in to_kill.items():
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
                    task_id, task_name = self.fut_task.pop(fut)

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
                                ''', (task_id, self._ts.rlookup('ts_failed'),
                                      self._tr.rlookup('tr_failure_abort'),
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
                                    ''', (task_id,
                                          self._ts.rlookup('ts_waiting'),
                                          self._tr.rlookup('tr_limit_retry'),
                                          self.slurm_job_id))

                        # We're completely done with the task, so let it go.
                        TaskManager._unclaim(tx, task_id, self.ident)

        self.clean_exit = True
        logger.debug('Left pool normally.')


class WorkerManager(Manager):
    # Path to worker output files, relative to chdir.
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._ws = WorkerState(self.db)
        self._wr = WorkerReason(self.db)

        self._ts = TaskState(self.db)
        self._tr = TaskReason(self.db)

    def _format_state(self, state_id, job_running, timeout):
        state = self._ws.lookup(state_id)
        state_user = self._ws.lookup(state_id, user=True)
        color = None

        if job_running and timeout:
            state_user += ' (?)'
            color = self.c('warning')
        elif state == 'ws_quitting':
            color = self.c('notice')
        elif state == 'ws_failed':
            color = self.c('error')

        return state, state_user, color

    def _parse_state(self, state_name):
        if state_name is None:
            return None

        try:
            return self._ws.rlookup(state_name, user=True)
        except KeyError:
            logger.error(f'Invalid state "{state_name}".')

            raise HandledException()

    def _worker_output_file(self, slurm_job_id=None, *, absolute=False):
        if slurm_job_id is None:
            # Replacement symbol for sbatch.
            slurm_job_id = '%j'
        else:
            slurm_job_id = str(slurm_job_id)

        path = (self.WORKER_OUTPUT_DIR
                    / self.WORKER_OUTPUT_FILE_TEMPLATE.format(slurm_job_id))

        if absolute:
            path = self.conf.general_chdir / path

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

        if not workers:
            logger.info('No workers found.')

            return

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
                         sorted_data, total=('Total', (1, 2, 3)))

    def ack_failed(self, args):
        @self.db.tx
        def workers(tx):
            return tx.execute('''
                    SELECT id
                    FROM worker
                    WHERE state_id = %s
                    ''', (self._ws.rlookup('ws_failed'),))

        for slurm_job_id, in workers:
            if self.mck.interrupted.is_set():
                break

            logger.info(slurm_job_id)

            @self.db.tx
            def F(tx):
                tx.execute('''
                        INSERT INTO worker_history (worker_id, state_id,
                                                    reason_id)
                        VALUES (%s, %s, %s)
                        ''', (slurm_job_id, self._ws.rlookup('ws_done'),
                              self._wr.rlookup('wr_worker_ack_failed')))

    def clean(self, args):
        state_name = args.state

        state_id = self._parse_state(state_name)

        ok_worker_ids = []

        while True:
            if self.mck.interrupted.is_set():
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
                state = self._ws.lookup(worker_state_id)
                state_user = self._ws.lookup(worker_state_id, user=True)

                logger.debug(f'Cleaning worker {slurm_job_id}.')

                if job_running:
                    reason_id = self._wr.rlookup('wr_worker_clean_running')
                elif state == 'ws_queued':
                    reason_id = self._wr.rlookup('wr_worker_clean_queued')
                else:
                    logger.error(f'Worker {slurm_job_id} is in state '
                                 f'"{state_user}".')

                    return None

                proc = subprocess.run(['squeue', '--noheader',
                                       '--jobs='+str(slurm_job_id)],
                                      capture_output=True, text=True)

                if proc.returncode == 0 and proc.stdout:
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
                            ''', (task_id, self._ts.rlookup('ts_failed'),
                                  self._tr.rlookup('tr_failure_worker_clean')))

                    TaskManager._unclaim(tx, task_id, ident)

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
                        ''', (slurm_job_id, self._ws.rlookup('ws_failed'),
                              reason_id))

                return slurm_job_id, task_names

            if result is None:
                break
            elif not result:
                continue

            slurm_job_id, task_names = result

            logger.info(slurm_job_id)

            for task_name in task_names:
                logger.info(f' {task_name}')

    def list(self, args):
        state_name = args.state

        state_id = self._parse_state(state_name)

        @self.db.tx
        def workers(tx):
            query = '''
                    SELECT w.id, w.state_id, ws.job_exists, ws.job_running,
                           w.num_cores, w.time_limit, w.mem_limit_mb, w.node,
                           w.time_start, w.time_end, w.num_tasks,
                           w.num_tasks_active, w.cur_mem_usage_mb,
                           worker_timeout(w, %s), NOW() - w.time_start
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
                query_args += (self._ws.rlookup('ws_failed'),)

            # Order by remaining time, putting failed workers first.
            query += '''
                    ORDER BY w.state_id = %s DESC,
                             w.time_start + w.time_limit - NOW()
                    '''
            query_args += (self._ws.rlookup('ws_failed'),)

            return tx.execute(query, query_args)

        worker_data = []

        for (slurm_job_id, state_id, job_exists, job_running, num_cores,
                time_limit, mem_limit_mb, node, time_start, time_end,
                num_tasks, num_tasks_active, cur_mem_usage_mb, timeout,
                elapsed_time) in workers:
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
            elif (tasks_percent is not None and tasks_percent < 50
                    and cur_mem_usage_percent is not None
                    and cur_mem_usage_percent < 50):
                tasks_percent_str = (tasks_percent_str, self.c('error'))
                cur_mem_usage_percent_str = (cur_mem_usage_percent_str,
                                             self.c('error'))
            elif tasks_percent is not None and tasks_percent < 50:
                tasks_percent_str = (tasks_percent_str, self.c('warning'))
            elif (cur_mem_usage_percent is not None
                    and cur_mem_usage_percent < 50):
                cur_mem_usage_percent_str = (cur_mem_usage_percent_str,
                                             self.c('warning'))

            worker_data.append([str(slurm_job_id), (state_user, state_color),
                                remaining_time, time_limit, tasks_running_show,
                                num_cores, tasks_percent_str, num_tasks,
                                cur_mem_usage_gb, mem_limit_gb,
                                cur_mem_usage_percent_str])

        self.print_table(['Job ID', 'State', ('Time (R/T)', 2),
                          ('Tasks (R/C/%/T)', 4), ('Mem (GB;U/T/%)', 3)],
                         worker_data)

    def list_queued(self, args):
        @self.db.tx
        def workers(tx):
            return tx.execute('''
                    SELECT num_cores, time_limit, mem_limit_mb,
                           MAX(time_start), NOW(),
                           COUNT(CASE WHEN state_id = %s THEN 1 END)
                    FROM worker
                    GROUP BY num_cores, time_limit, mem_limit_mb
                    ORDER BY MAX(time_start) NULLS FIRST,
                             num_cores, time_limit, mem_limit_mb
                    ''', (self._ws.rlookup('ws_queued'),))

        worker_data = []

        for (num_cores, time_limit, mem_limit_mb, max_time_start, now,
                count) in workers:
            mem_limit_gb = ceil(mem_limit_mb / 1024)

            if max_time_start is not None:
                max_time_start = humanize_datetime(max_time_start, now)
            else:
                max_time_start = '-'

            if count == 0:
                count = '-'

            worker_data.append([num_cores, time_limit, mem_limit_gb, count,
                                max_time_start])

        self.print_table(['Cores', 'Time', 'Mem (GB)', 'Count',
                          'Most recent start'],
                         worker_data)

    def quit(self, args):
        abort = args.abort
        all_workers = args.all
        state_name = args.state
        slurm_job_ids = set(args.slurm_job_id)

        if abort:
            signal = 'TERM'
        else:
            signal = 'INT'

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
            if self.mck.interrupted.is_set():
                break

            # Try to cancel it before it gets a chance to run.
            logger.debug(f'Attempting to cancel worker {slurm_job_id}.')

            proc = subprocess.run(['scancel', '--verbose', '--state=PENDING',
                                   str(slurm_job_id)],
                                  capture_output=True, text=True)
            cancel_success = check_scancel(proc, log=logger.error)

            if cancel_success is None:
                # We encountered an error, so give up.
                return
            elif cancel_success:
                logger.info(slurm_job_id)

                @self.db.tx
                def F(tx):
                    tx.execute('''
                            INSERT INTO worker_history (worker_id, state_id,
                                                        reason_id)
                            VALUES (%s, %s, %s)
                            ''', (slurm_job_id,
                                  self._ws.rlookup('ws_cancelled'),
                                  self._wr.rlookup('wr_worker_quit_cancelled')))

                continue

            # It's already running (or finished), so try to send a signal.
            logger.debug(f'Attempting to signal worker {slurm_job_id}.')

            proc = subprocess.run(['scancel', '--verbose', '--state=RUNNING',
                                   '--batch', f'--signal={signal}',
                                   str(slurm_job_id)],
                                  capture_output=True, text=True)
            signal_success = check_scancel(proc, log=logger.error)

            if signal_success is None:
                # We encountered an error, so give up.
                return
            elif signal_success:
                logger.info(slurm_job_id)

                continue

    def run(self, args):
        try:
            slurm_job_id = int(os.getenv('SLURM_JOB_ID'))
        except TypeError:
            logger.error('Not running in a Slurm job.')

            return

        worker_node = os.getenv('SLURMD_NODENAME')
        worker_cpus = int(os.getenv('SLURM_JOB_CPUS_PER_NODE'))
        worker_mem_mb = int(os.getenv('SLURM_MEM_PER_NODE'))

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
                    ''', (slurm_job_id, self._ws.rlookup('ws_running'),
                          self._wr.rlookup('wr_start')))

            return time_limit

        if time_limit is None:
            return

        time_end_projected = (datetime.now() + time_limit
                              - timedelta(seconds=self.EXIT_BUFFER_SECONDS)
                              - timedelta(seconds=self.END_SIGNAL_SECONDS))

        logger.info(f'Starting worker {slurm_job_id}.')
        worker = Worker(slurm_job_id, worker_cpus, worker_mem_mb,
                        time_end_projected, self)

        signal.signal(signal.SIGINT, worker.quit)
        signal.signal(signal.SIGTERM, worker.abort)

        try:
            worker.run()
            logger.debug('Success!')
        finally:
            logger.info('Worker done.')

            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)

            if not worker.clean_exit:
                state_id = self._ws.rlookup('ws_failed')
                reason_id = self._wr.rlookup('wr_failure')
            else:
                state_id = self._ws.rlookup('ws_done')

                if worker.done_due_to_abort:
                    reason_id = self._wr.rlookup('wr_success_abort')
                elif worker.done_due_to_idle:
                    reason_id = self._wr.rlookup('wr_success_idle')
                else:
                    reason_id = self._wr.rlookup('wr_success')

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

    def show(self, args):
        slurm_job_id = args.slurm_job_id

        @self.db.tx
        def worker_history(tx):
            return tx.execute('''
                    SELECT state_id, time,
                           LEAD(time, 1, NOW()) OVER (ORDER BY time, id),
                           reason_id
                    FROM worker_history
                    WHERE worker_id = %s
                    ORDER BY id
                    ''', (slurm_job_id,))

        worker_data = []

        for state_id, time, time_next, reason_id in worker_history:
            state_user = self._ws.lookup(state_id, user=True)
            reason_desc = self._wr.dlookup(reason_id)

            duration = time_next - time

            worker_data.append([time, duration, state_user, reason_desc])

        if worker_data:
            self.print_table(['Time', 'Duration', 'State', 'Reason'],
                             worker_data)
        else:
            print('No state history.')

        print()

        @self.db.tx
        def worker_task(tx):
            return tx.execute('''
                    SELECT t.name, wt.time_active, wt.time_inactive,
                           NOW() - wt.time_active
                    FROM worker_task wt
                    JOIN task t ON t.id = wt.task_id
                    WHERE wt.worker_id = %s
                    ORDER BY wt.id
                    ''', (slurm_job_id,))

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
        else:
            print('No worker tasks.')

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

    def spawn(self, args):
        worker_cpus = args.cpus
        worker_time_hours = args.time_hr
        worker_mem_gb = args.mem_gb
        sbatch_args = args.sbatch_args
        num = args.num

        if num < 1:
            logger.error('Must spawn at least 1 worker.')

            return

        worker_time_minutes = ceil(worker_time_hours * 60)
        worker_mem_mb = ceil(worker_mem_gb * 1024)

        proc_args = ['sbatch']
        proc_args.append('--hold')
        proc_args.append('--parsable')
        proc_args.append('--job-name=' + self.conf.worker_name)
        proc_args.append('--signal=B:TERM@' + str(self.END_SIGNAL_SECONDS))
        proc_args.append('--chdir=' + str(self.conf.general_chdir))
        proc_args.append('--output=' + str(self._worker_output_file()))
        proc_args.append('--cpus-per-task=' + str(worker_cpus))
        proc_args.append('--time=' + str(worker_time_minutes))
        proc_args.append('--mem=' + str(worker_mem_mb))

        if self.conf.worker_sbatch_args is not None:
            proc_args.extend(shlex.split(self.conf.worker_sbatch_args))

        if sbatch_args is not None:
            proc_args.extend(shlex.split(sbatch_args))

        mck_cmd = shlex.quote(self.conf.worker_mck_cmd)

        if self.conf.worker_mck_args is not None:
            mck_args = shlex.quote(self.conf.worker_mck_args)
        else:
            mck_args = ''

        os.makedirs(self.conf.general_chdir, exist_ok=True)
        os.makedirs(self.conf.general_chdir / self.WORKER_OUTPUT_DIR,
                    exist_ok=True)

        script = f'''
                #!/bin/bash

                export PYTHONUNBUFFERED=1
                exec {mck_cmd} {mck_args} worker run
                '''

        for _ in range(num):
            if self.mck.interrupted.is_set():
                break

            logger.debug('Spawning worker job.')

            proc = subprocess.run(proc_args, input=script.strip(),
                                  capture_output=True, text=True)

            if not check_proc(proc, log=logger.error):
                return

            if ';' in proc.stdout:
                # Ignore the cluster name.
                slurm_job_id = int(proc.stdout.split(';', maxsplit=1)[0])
            else:
                slurm_job_id = int(proc.stdout)

            worker_time = timedelta(hours=worker_time_hours)

            @self.db.tx
            def F(tx):
                tx.execute('''
                        INSERT INTO worker (id, state_id, num_cores,
                                            time_limit, mem_limit_mb)
                        VALUES (%s, %s, %s, %s, %s)
                        ''', (slurm_job_id, self._ws.rlookup('ws_queued'),
                              worker_cpus, worker_time, worker_mem_mb))

                tx.execute('''
                        INSERT INTO worker_history (worker_id, state_id,
                                                    reason_id)
                        VALUES (%s, %s, %s)
                        ''', (slurm_job_id, self._ws.rlookup('ws_queued'),
                              self._wr.rlookup('wr_worker_spawn')))

            logger.debug(f'Releasing worker job {slurm_job_id}.')

            proc = subprocess.run(['scontrol', 'release', str(slurm_job_id)],
                                  capture_output=True, text=True)

            if not check_proc(proc, log=logger.error):
                return

            logger.info(slurm_job_id)
