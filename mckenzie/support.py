from datetime import datetime, timedelta
import logging
from math import ceil
import os
from pathlib import Path
import shlex

from . import slurm
from .arguments import argparsable, argument, description
from .base import Manager, preflight
from .util import assemble_command, humanize_datetime


logger = logging.getLogger(__name__)


@argparsable('support job management')
@preflight(database_init=False, database_current=False)
class SupportManager(Manager, name='support'):
    # Path to support output files, relative to support directory.
    SUPPORT_OUTPUT_DIR = Path('support_output')
    # Support output file name template.
    SUPPORT_OUTPUT_FILE_TEMPLATE = 'support-{}.out'

    NUM_WINDOWS = 9

    # 5 minutes
    END_SIGNAL_SECONDS = 300
    # 1.5 minutes
    INTERRUPT_WAIT_SECONDS = 90

    def _support_output_file(self):
        # Replacement symbol for sbatch.
        slurm_job_id = '%j'
        path = (self.SUPPORT_OUTPUT_DIR
                    / self.SUPPORT_OUTPUT_FILE_TEMPLATE.format(slurm_job_id))

        return path

    def summary(self, args):
        logger.info('No action specified.')

    @description('attach to support job')
    @argument('--print-command', action='store_true', help='only print the command that would be executed')
    @argument('--node', metavar='N', help='name of node on which job is running')
    @argument('slurm_job_id', type=int, help='Slurm job ID of support job')
    def attach(self, args):
        print_command = args.print_command
        node = args.node
        slurm_job_id = args.slurm_job_id

        if node is None:
            logger.debug(f'Getting node name from Slurm.')
            node = slurm.running_job_node(slurm_job_id, log=logger.error)
            logger.debug(f'Using node name "{node}".')
        else:
            logger.debug(f'Using supplied node name "{node}".')

        socket_dir = self.conf.support_socket_dir_template.format(slurm_job_id)
        socket_path = os.path.join(socket_dir, 'tmux')

        logger.debug(f'Using socket path: {socket_path}')

        proc_args = ['ssh']
        proc_args.append('-t')
        proc_args.append(node)
        proc_args.extend(['tmux', '-S', shlex.quote(socket_path), 'attach'])

        if print_command:
            print(assemble_command(proc_args))

            return

        logger.debug(f'Attaching to support job {slurm_job_id}.')

        os.execvp('ssh', proc_args)

    @description('list support jobs')
    def list(self, args):
        columns = ['%A', '%t', '%R', '%P', '%C', '%l', '%m', '%S', '%e']
        jobs = slurm.list_all_jobs(self.conf.support_job_name, columns,
                                   log=logger.error)

        raw_time_starts = []
        support_data = []

        for (jobid, state, reason, partition, cpus, time_total, mem,
                time_start, time_end) in jobs:
            raw_time_starts.append(time_start)
            now = datetime.now()

            cpus = int(cpus)
            time_total = slurm.parse_timedelta(time_total)
            mem_gb = ceil(slurm.parse_units_mb(mem) / 1024)

            try:
                dt = datetime.fromisoformat(time_start)
            except ValueError:
                pass
            else:
                time_start = humanize_datetime(dt, now)

            try:
                dt = datetime.fromisoformat(time_end)
            except ValueError:
                pass
            else:
                if state in ['PD', 'R']:
                    signal_offset = timedelta(seconds=self.END_SIGNAL_SECONDS)
                    time_end = humanize_datetime(dt - signal_offset, now)
                else:
                    time_end = humanize_datetime(dt, now)

            support_data.append([jobid, state, reason[:20], partition, cpus,
                                 time_total, mem_gb, time_start, time_end])

        # Sort by start time.
        sorted_data = [row for (s, row) in sorted(zip(raw_time_starts,
                                                      support_data),
                                                  reverse=True)]
        self.print_table(['Job ID', ('State', 2), 'Partition', 'Cores', 'Time',
                          'Mem (GB)', 'Start', 'End'],
                         sorted_data)

    @description('list completed support jobs')
    @argument('--last-hr', metavar='T', type=float, required=True, help='jobs completed in the last T hours')
    def list_completed(self, args):
        last = timedelta(hours=args.last_hr)

        columns = ['JobID', 'AllocCPUS', 'TotalCPU', 'CPUTime', 'MaxRSS',
                   'ReqMem', 'MaxDiskRead', 'MaxDiskWrite', 'End']
        jobs = slurm.list_completed_jobs(self.conf.support_job_name, columns,
                                         last, log=logger.error)

        support_data = []

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

            support_data.append([jobid, cpus, cpu_used, cpu_total,
                                 cpu_percent_str, mem_used_mb, mem_total_mb,
                                 mem_percent_str, disk_read_mb, disk_write_mb,
                                 time_end])

        self.print_table(['Job ID', 'Cores', ('CPU (U/T/%)', 3),
                          ('Mem (MB;U/T/%)', 3), ('Disk (MB;R/W)', 2), 'End'],
                         support_data)

    @description('signal support jobs to quit')
    @argument('--all', action='store_true', help='signal all support jobs')
    @argument('slurm_job_id', nargs='*', type=int, help='Slurm job ID of support job')
    def quit(self, args):
        all_jobs = args.all
        slurm_job_ids = set(args.slurm_job_id)

        if all_jobs:
            ids = slurm.get_all_job_ids(self.conf.support_job_name,
                                        log=logger.error)
            slurm_job_ids.update(ids)

        for slurm_job_id in slurm_job_ids:
            if self.mck.interrupted:
                break

            logger.debug(f'Attempting to cancel Slurm job {slurm_job_id}.')
            cancel_success, signalled_running \
                    = slurm.cancel_job(slurm_job_id,
                                       name=self.conf.support_job_name,
                                       signal='INT', log=logger.error)

            if cancel_success:
                logger.info(slurm_job_id)

    @description('spawn Slurm support job')
    @argument('--cpus', metavar='C', type=int, required=True, help='number of CPUs')
    @argument('--time-hr', metavar='T', type=float, required=True, help='time limit in hours')
    @argument('--mem-gb', metavar='M', type=float, required=True, help='amount of memory in GB')
    @argument('--sbatch-args', metavar='SA', help='additional arguments to pass to sbatch')
    @argument('--num', type=int, default=1, help='number of support jobs to spawn (default: 1)')
    def spawn(self, args):
        support_cpus = args.cpus
        support_time_hours = args.time_hr
        support_mem_gb = args.mem_gb
        sbatch_args = args.sbatch_args
        num = args.num

        if num < 1:
            logger.error('Must spawn at least 1 support job.')

            return

        execute_cmd = shlex.quote(self.conf.support_execute_cmd)
        socket_dir = self.conf.support_socket_dir_template.format('${SLURM_JOB_ID}')
        socket_path = os.path.join(socket_dir, 'tmux')

        script = f'''
                #!/bin/bash

                TMUX_SOCKET_DIR="{socket_dir}"
                TMUX_SOCKET="{socket_path}"
                echo "tmux socket dir: ${{TMUX_SOCKET_DIR}}"
                echo "tmux socket: ${{TMUX_SOCKET}}"
                mkdir -p "$TMUX_SOCKET_DIR"

                completed=0
                trap 'completed=1' INT

                cmd={execute_cmd}
                args="new-session -d ${{cmd}}"

                for i in $(seq 2 {self.NUM_WINDOWS}); do
                    args="${{args}} ; new-window ${{cmd}}"
                done

                tmux -S "$TMUX_SOCKET" $args

                while [[ "$completed" == 0 ]]; do
                    sleep 1
                done

                while true; do
                    tmux -S "$TMUX_SOCKET" ls >/dev/null 2>&1

                    if [[ "$?" != 0 ]]; then
                        break
                    fi

                    echo 'Interrupting all panes.'

                    for pane in $(tmux -S "$TMUX_SOCKET" list-panes -a -F '#{{pane_id}}'); do
                        echo "$pane"
                        tmux -S "$TMUX_SOCKET" send-keys -t "$pane" 'C-c'
                    done

                    sleep {self.INTERRUPT_WAIT_SECONDS}
                done
                '''

        submitter = slurm.JobSubmitter(name=self.conf.support_job_name,
                                       signal='INT',
                                       signal_seconds=self.END_SIGNAL_SECONDS,
                                       chdir_path=self.conf.support_path,
                                       output_file_path=self._support_output_file(),
                                       cpus=support_cpus,
                                       time_hours=support_time_hours,
                                       mem_gb=support_mem_gb,
                                       sbatch_argss=[self.conf.general_sbatch_args,
                                                     self.conf.support_sbatch_args,
                                                     sbatch_args],
                                       script=script)

        for _ in range(num):
            if self.mck.interrupted:
                break

            logger.debug('Spawning support job.')
            slurm_job_id = submitter.submit(log=logger.error)
            logger.info(slurm_job_id)
