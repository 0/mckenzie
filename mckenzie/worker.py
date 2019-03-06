from concurrent import futures
from datetime import datetime, timedelta
import logging
from math import ceil
import os
import shlex
import signal
import subprocess
import threading
from time import sleep

from .base import DatabaseReasonView, DatabaseStateView, Instance, Manager
from .task import TaskManager, TaskReason, TaskState
from .util import check_proc, check_scancel, print_table


logger = logging.getLogger(__name__)


# Worker state flow:
#
#    +-> cancelled
#    |
# queued -> running -> done
#              |
#              +-> failed


class WorkerState(DatabaseStateView):
    def __init__(self, *args, **kwargs):
        super().__init__('worker_state', 'ws_', *args, **kwargs)


class WorkerReason(DatabaseReasonView):
    def __init__(self, *args, **kwargs):
        super().__init__('worker_reason', *args, **kwargs)


class Worker(Instance):
    # 0.5 minutes
    TASK_WAIT_SECONDS = 30
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
        self.fut_pending = set()
        # PIDs currently executing.
        self.running_pids = set()

        # Task IDs.
        self.fut_ids = {}
        # Task names.
        self.fut_names = {}

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
                    UPDATE worker
                    SET quitting = TRUE
                    WHERE id = %s
                    ''', (self.slurm_job_id,))

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
        p = subprocess.Popen(args, stdout=subprocess.PIPE, text=True,
                             # If the child process spawns its own processes,
                             # we can reliably kill the entire process group
                             # when needed.
                             start_new_session=True)

        with self.lock:
            self.running_pids.add(p.pid)

        # Block until the process is done.
        wait_pid, wait_return, wait_usage = os.wait4(p.pid, 0)

        with self.lock:
            self.running_pids.remove(p.pid)

        success = wait_pid == p.pid and wait_return == 0

        # The units of ru_maxrss should be KB.
        return success, p.stdout.read(), int(wait_usage.ru_maxrss)

    def _run(self, pool):
        task_starts = {}

        while not self.done:
            if self.remaining_time.total_seconds() < self.GIVE_UP_SECONDS:
                # Too little time remaining in job, so we will not take on any
                # more tasks.
                self.quit()

            # Fill up with tasks.
            while (not self.quitting
                    and len(self.fut_pending) < self.worker_cpus):
                task = self._choose_task()

                if task is None:
                    break

                # Reset the idle timer.
                self.idle_start = None

                task_id, task_name, task_mem_mb = task

                task_starts[task_name] = datetime.now()
                self.task_mems_mb[task_name] = task_mem_mb

                logger.debug(f'Submitting task "{task_name}" to the pool.')
                fut = pool.submit(self._execute_task, task_name)

                self.fut_pending.add(fut)
                self.fut_ids[fut] = task_id
                self.fut_names[fut] = task_name

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

            if self.fut_pending:
                # Wait for running tasks.
                r = futures.wait(self.fut_pending,
                                 timeout=self.TASK_WAIT_SECONDS,
                                 return_when=futures.FIRST_COMPLETED)
                fut_done, self.fut_pending = r

                # Handle completed tasks.
                for fut in fut_done:
                    task_id = self.fut_ids.pop(fut)
                    task_name = self.fut_names.pop(fut)

                    success, output, max_mem_kb = fut.result()
                    max_mem_mb = max_mem_kb / 1024

                    if success:
                        output = output.split('\n')

                        while output and output[-1] == '':
                            output.pop()

                        ss = self.conf.worker_success_string

                        if not (output and output[-1] == ss):
                            success = False
                            reason_id = self._tr.rlookup('tr_failure_string')
                        else:
                            reason_id = self._tr.rlookup('tr_success')
                    else:
                        reason_id = self._tr.rlookup('tr_failure_exit_code')

                    if success:
                        state_id = self._ts.rlookup('ts_done')
                    else:
                        state_id = self._ts.rlookup('ts_failed')

                    logger.debug(f'"{task_name}": {success}')

                    duration = datetime.now() - task_starts[task_name]

                    @self.db.tx
                    def F(tx):
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

                        TaskManager._unclaim(tx, task_id, self.ident)

                    task_starts.pop(task_name)
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

                for pid in to_kill:
                    logger.info(f'Killing process {pid}.')

                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

                # Collect remaining futures.
                r = futures.wait(self.fut_pending,
                                 timeout=self.TASK_CLEANUP_WAIT_SECONDS,
                                 return_when=futures.ALL_COMPLETED)
                fut_done, self.fut_pending = r

                for fut in fut_done:
                    task_id = self.fut_ids.pop(fut)
                    task_name = self.fut_names.pop(fut)

                    logger.debug(f'Task "{task_name}" was aborted.')

                    @self.db.tx
                    def F(tx):
                        tx.execute('''
                                INSERT INTO task_history (task_id, state_id,
                                                          reason_id, worker_id)
                                VALUES (%s, %s, %s, %s)
                                ''', (task_id, self._ts.rlookup('ts_failed'),
                                      self._tr.rlookup('tr_failure_abort'),
                                      self.slurm_job_id))

                        TaskManager._unclaim(tx, task_id, self.ident)

        self.clean_exit = True
        logger.debug('Left pool normally.')


class WorkerManager(Manager):
    # 3 minutes
    EXIT_BUFFER_SECONDS = 180
    # 2 minutes
    END_SIGNAL_SECONDS = 120
    # 2 minutes
    HEARTBEAT_TIMEOUT_SECONDS = 120

    STATE_ORDER = ['queued', 'running', 'running (Q)', 'running (?)',
                   'failed (!)', 'failed', 'cancelled', 'done']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._ws = WorkerState(self.db)
        self._wr = WorkerReason(self.db)

        self._ts = TaskState(self.db)
        self._tr = TaskReason(self.db)

    def _format_state(self, state_id, quitting, timeout, failure_acknowledged):
        state = self._ws.lookup(state_id)
        state_user = self._ws.lookup(state_id, user=True)

        if state == 'ws_running' and timeout:
            state_user += ' (?)'
        elif state == 'ws_running' and quitting:
            state_user += ' (Q)'
        elif state == 'ws_failed' and not failure_acknowledged:
            state_user += ' (!)'

        return state, state_user

    def summary(self, args):
        @self.db.tx
        def workers(tx):
            td = timedelta(seconds=self.HEARTBEAT_TIMEOUT_SECONDS)

            return tx.execute('''
                    SELECT state_id, worker_timeout(worker, %s) AS timeout,
                           quitting, failure_acknowledged, SUM(num_cores),
                           SUM(num_cores * time_limit),
                           SUM(num_cores * (time_start + time_limit - NOW())),
                           COUNT(*)
                    FROM worker
                    GROUP BY state_id, timeout, quitting, failure_acknowledged
                    ''', (td,))

        if not workers:
            logger.info('No workers found.')

            return

        worker_data = []

        for (state_id, timeout, quitting, failure_acknowledged, num_tasks,
                total_time, remaining_time, count) in workers:
            state, state_user = self._format_state(state_id, quitting, timeout,
                                                   failure_acknowledged)

            if state == 'ws_queued':
                time = total_time
            elif state == 'ws_running':
                time = remaining_time
            else:
                num_tasks = None
                time = None

            worker_data.append([state_user, count, num_tasks, time])

        sorted_data = sorted(worker_data,
                             key=lambda row: self.STATE_ORDER.index(row[0]))
        print_table(['State', 'Count', 'Tasks', 'Remaining time'], sorted_data,
                    total=('Total', (1, 2, 3)))

    def ack_failed(self, args):
        @self.db.tx
        def workers(tx):
            return tx.execute('''
                    UPDATE worker
                    SET failure_acknowledged = TRUE
                    WHERE state_id = %s
                    AND failure_acknowledged = FALSE
                    RETURNING id
                    ''', (self._ws.rlookup('ws_failed'),))

        for slurm_job_id, in workers:
            logger.info(slurm_job_id)

    def clean(self, args):
        state_name = args.state

        if state_name is not None:
            try:
                state_id = self._ws.rlookup(state_name, user=True)
            except KeyError:
                logger.error(f'Invalid state "{state_name}".')

                return
        else:
            state_id = None

        ok_worker_ids = []

        while True:
            @self.db.tx
            def result(tx):
                query = '''
                        SELECT w.id, w.state_id
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

                slurm_job_id, worker_state_id = worker[0]
                ident = Worker.impersonate(slurm_job_id)
                state = self._ws.lookup(worker_state_id)
                state_user = self._ws.lookup(worker_state_id, user=True)

                if state == 'ws_queued':
                    reason_id = self._wr.rlookup('wr_worker_clean_queued')
                elif state == 'ws_running':
                    reason_id = self._wr.rlookup('wr_worker_clean_running')
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
                    if claimed_by != ident:
                        logger.warning(f'Task "{task_name}" was not claimed '
                                       f'by worker {slurm_job_id}.')

                    tx.execute('''
                            INSERT INTO task_history (task_id, state_id,
                                                      reason_id)
                            VALUES (%s, %s, %s)
                            ''', (task_id, self._ts.rlookup('ts_failed'),
                                  self._tr.rlookup('tr_failure_worker_clean')))

                    TaskManager._unclaim(tx, task_id, None, force=True)

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

        if state_name is not None:
            try:
                state_id = self._ws.rlookup(state_name, user=True)
            except KeyError:
                logger.error(f'Invalid state "{state_name}".')

                return
        else:
            state_id = None

        @self.db.tx
        def workers(tx):
            query = '''
                    SELECT w.id, w.state_id, ws.job_exists, w.num_cores,
                           w.time_limit, w.mem_limit_mb, w.node, w.time_start,
                           worker_timeout(w, %s), w.time_end, w.quitting,
                           w.failure_acknowledged, w.num_tasks,
                           w.num_tasks_active, w.cur_mem_usage_mb,
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
                query += '''
                        WHERE ws.job_exists
                        OR (w.state_id = %s
                            AND NOT w.failure_acknowledged)
                        '''
                query_args += (self._ws.rlookup('ws_failed'),)

            # Order by remaining time, then ID.
            query += ' ORDER BY w.time_start + w.time_limit - NOW(), w.id'

            return tx.execute(query, query_args)

        worker_data = []

        for (slurm_job_id, state_id, job_exists, num_cores, time_limit,
                mem_limit_mb, node, time_start, timeout, time_end, quitting,
                failure_acknowledged, num_tasks, num_tasks_active,
                cur_mem_usage_mb, elapsed_time) in workers:
            state, state_user = self._format_state(state_id, quitting, timeout,
                                                   failure_acknowledged)

            remaining_time = '-'

            if job_exists:
                if time_start is None:
                    remaining_time = time_limit
                elif time_end is None:
                    remaining_time = time_limit - elapsed_time

            if state == 'ws_running':
                tasks_running_show = num_tasks_active
                tasks_frac = num_tasks_active / num_cores
                tasks_percent = f'{ceil(tasks_frac * 100)}%'
            else:
                tasks_running_show = '-'
                tasks_percent = '-'

            mem_limit_gb = ceil(mem_limit_mb / 1024)

            if state == 'ws_running' and cur_mem_usage_mb is not None:
                cur_mem_usage_gb = ceil(cur_mem_usage_mb / 1024)
                cur_mem_usage_frac = cur_mem_usage_mb / mem_limit_mb
                cur_mem_usage_percent = f'{ceil(cur_mem_usage_frac * 100)}%'
            else:
                cur_mem_usage_gb = '-'
                cur_mem_usage_percent = '-'

            worker_data.append([str(slurm_job_id), state_user, remaining_time,
                                time_limit, tasks_running_show, num_cores,
                                tasks_percent, num_tasks, cur_mem_usage_gb,
                                mem_limit_gb, cur_mem_usage_percent])

        print_table(['Job ID', 'State', ('Time (R/T)', 2),
                     ('Tasks (R/C/%/T)', 4), ('Mem (GB;U/T/%)', 3)],
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

        if state_name is not None:
            try:
                state_id = self._ws.rlookup(state_name, user=True)
            except KeyError:
                logger.error(f'Invalid state "{state_name}".')

                return
        else:
            state_id = None

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
            # Try to cancel it before it gets a chance to run.
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
            print_table(['Time', 'Duration', 'State', 'Reason'], worker_data)
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
            print_table(['Task', 'Active at', 'Inactive at', 'Duration'],
                        task_data)
        else:
            print('No worker tasks.')

    def spawn(self, args):
        worker_cpus = args.cpus
        worker_time_hours = args.time
        worker_mem_gb = args.mem
        sbatch_args = args.sbatch_args
        num = args.num

        if num < 1:
            logger.error('Must spawn at least 1 worker.')

            return

        worker_time_minutes = ceil(worker_time_hours * 60)
        worker_mem_mb = ceil(worker_mem_gb * 1024)

        proc_args = ['sbatch']
        proc_args.append('--job-name=' + self.conf.worker_name)
        proc_args.append('--hold')
        proc_args.append('--parsable')
        proc_args.append('--signal=B:TERM@' + str(self.END_SIGNAL_SECONDS))
        proc_args.append('--chdir=' + self.conf.worker_chdir)
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

        os.makedirs(self.conf.worker_chdir, exist_ok=True)

        script = f'''
                #!/bin/bash

                export PYTHONUNBUFFERED=1
                exec {mck_cmd} {mck_args} worker run
                '''

        for _ in range(num):
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

            proc = subprocess.run(['scontrol', 'release', str(slurm_job_id)],
                                  capture_output=True, text=True)

            if not check_proc(proc, log=logger.error):
                return

            logger.info(slurm_job_id)
