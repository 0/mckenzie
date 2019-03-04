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

    def __init__(self, slurm_job_id, worker_cpus, worker_mem_mb,
                 time_end_projected, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.slurm_job_id = slurm_job_id
        self.worker_cpus = worker_cpus
        self.worker_mem_mb = worker_mem_mb
        self.time_end_projected = time_end_projected

        # 00000010 SSSSSSSS SSSSSSSS SSSSSSSS
        self._ident = (0x02 << 24) | (self.slurm_job_id & 0xffffff)

        self.lock = threading.Lock()

        # All the tasks have finished (or will be killed) and worker can exit.
        self.done = False
        # Worker will be done when all the running tasks have finished.
        self.quitting = False

        # Worker exited normally, not through an exception.
        self.clean_exit = False
        # Worker is exiting because it was asked to abort.
        self.done_due_to_abort = False

        # Futures currently executing.
        self.fut_pending = set()
        # PIDs currently executing.
        self.running_pids = set()

        # Task names.
        self.fut_names = {}

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

    def _execute_task(self, task_name):
        args = ['sleep', task_name]
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
        while not self.done:
            if self.remaining_time.total_seconds() < self.GIVE_UP_SECONDS:
                # Too little time remaining in job, so we will not take on any
                # more tasks.
                self.quit()

            # Fill up with tasks.
            while (not self.quitting
                    and len(self.fut_pending) < self.worker_cpus):
                # Generate a random amount of time to sleep.
                from random import randint
                task_name = str(randint(2, 120))

                logger.debug(f'Submitting task "{task_name}" to the pool.')
                fut = pool.submit(self._execute_task, task_name)

                self.fut_pending.add(fut)
                self.fut_names[fut] = task_name

            # Update the status.
            @self.db.tx
            def F(tx):
                tx.execute('''
                        UPDATE worker
                        SET heartbeat = NOW()
                        WHERE id = %s
                        ''', (self.slurm_job_id,))

            if self.fut_pending:
                # Wait for running tasks.
                r = futures.wait(self.fut_pending,
                                 timeout=self.TASK_WAIT_SECONDS,
                                 return_when=futures.FIRST_COMPLETED)
                fut_done, self.fut_pending = r

                # Handle completed tasks.
                for fut in fut_done:
                    task_name = self.fut_names.pop(fut)

                    success, output, max_mem_kb = fut.result()
                    logger.debug(f'"{task_name}": {success} "{output}" '
                                 f'({max_mem_kb} KB)')

            elif self.quitting:
                self.done = True
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
                    task_name = self.fut_names.pop(fut)

                    logger.debug(f'Task "{task_name}" was aborted.')

        self.clean_exit = True
        logger.debug('Left pool normally.')


class WorkerManager(Manager):
    # 3 minutes
    EXIT_BUFFER_SECONDS = 180
    # 2 minutes
    END_SIGNAL_SECONDS = 120

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._ws = WorkerState(self.db)
        self._wr = WorkerReason(self.db)

    def summary(self, args):
        @self.db.tx
        def workers(tx):
            return tx.execute('''
                    SELECT state_id, COUNT(*)
                    FROM worker
                    GROUP BY state_id
                    ''')

        if not workers:
            logger.info('No workers found.')

            return

        worker_data = []

        for state_id, count in workers:
            state_user = self._ws.lookup(state_id, user=True)

            worker_data.append([state_user, count])

        print_table(['State', 'Count'], worker_data,
                    total=('Total', (1, 2)))

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
                    SELECT id, state_id
                    FROM worker
                    '''
            query_args = ()

            if state_id is not None:
                query += ' WHERE state_id = %s'
                query_args += (state_id,)

            query += ' ORDER BY id'

            return tx.execute(query, query_args)

        worker_data = []

        for slurm_job_id, state_id in workers:
            state_user = self._ws.lookup(state_id, user=True)

            worker_data.append([str(slurm_job_id), state_user])

        print_table(['Job ID', 'State'], worker_data)

    def quit(self, args):
        abort = args.abort
        all_workers = args.all
        slurm_job_ids = set(args.slurm_job_id)

        if abort:
            signal = 'TERM'
        else:
            signal = 'INT'

        if all_workers:
            @self.db.tx
            def workers(tx):
                # All workers with jobs.
                return tx.execute('''
                        SELECT w.id
                        FROM worker w
                        JOIN worker_state ws ON ws.id = w.state_id
                        WHERE ws.job_exists
                        ''')

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
                        heartbeat = NOW()
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

    def spawn(self, args):
        worker_cpus = args.cpus
        worker_time_hours = args.time
        worker_mem_gb = args.mem
        sbatch_args = args.sbatch_args

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

        script = f'''
                #!/bin/bash

                export PYTHONUNBUFFERED=1
                exec {mck_cmd} {mck_args} worker run
                '''

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
                    INSERT INTO worker (id, state_id, num_cores, time_limit,
                                        mem_limit_mb)
                    VALUES (%s, %s, %s, %s, %s)
                    ''', (slurm_job_id, self._ws.rlookup('ws_queued'),
                          worker_cpus, worker_time, worker_mem_mb))

            tx.execute('''
                    INSERT INTO worker_history (worker_id, state_id, reason_id)
                    VALUES (%s, %s, %s)
                    ''', (slurm_job_id, self._ws.rlookup('ws_queued'),
                          self._wr.rlookup('wr_worker_spawn')))

        os.makedirs(self.conf.worker_chdir, exist_ok=True)

        proc = subprocess.run(['scontrol', 'release', str(slurm_job_id)],
                              capture_output=True, text=True)

        if not check_proc(proc, log=logger.error):
            return

        logger.info(slurm_job_id)
