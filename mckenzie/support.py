from datetime import datetime, timedelta
import logging
from math import ceil
import os
from pathlib import Path
import shlex
import subprocess

from .base import Manager
from .util import (cancel_slurm_job, check_proc, combine_shell_args,
                   humanize_datetime, parse_slurm_timedelta)


logger = logging.getLogger(__name__)


class SupportManager(Manager, name='support'):
    PREFLIGHT_DISABLED = frozenset({'database_init', 'database_current'})

    # Path to support output files, relative to support directory.
    SUPPORT_OUTPUT_DIR = Path('support_output')
    # Support output file name template.
    SUPPORT_OUTPUT_FILE_TEMPLATE = 'support-{}.out'

    NUM_WINDOWS = 9

    # 5 minutes
    END_SIGNAL_SECONDS = 300
    # 1.5 minutes
    INTERRUPT_WAIT_SECONDS = 90

    @classmethod
    def add_cmdline_parser(cls, p_sub):
        # support
        p_support = p_sub.add_parser('support', help='support job management')
        p_support_sub = p_support.add_subparsers(dest='subcommand')

        # support attach
        p_support_attach = p_support_sub.add_parser('attach', help='attach to support job')
        p_support_attach.add_argument('slurm_job_id', type=int, help='Slurm job ID of support job')

        # support list
        p_support_list = p_support_sub.add_parser('list', help='list support jobs')

        # support quit
        p_support_quit = p_support_sub.add_parser('quit', help='signal support jobs to quit')
        p_support_quit.add_argument('--all', action='store_true', help='signal all support jobs')
        p_support_quit.add_argument('slurm_job_id', nargs='*', type=int, help='Slurm job ID of support job')

        # support spawn
        p_support_spawn = p_support_sub.add_parser('spawn', help='spawn Slurm support job')
        p_support_spawn.add_argument('--cpus', metavar='C', type=int, required=True, help='number of CPUs')
        p_support_spawn.add_argument('--time-hr', metavar='T', type=float, required=True, help='time limit in hours')
        p_support_spawn.add_argument('--mem-gb', metavar='M', type=float, required=True, help='amount of memory in GB')
        p_support_spawn.add_argument('--sbatch-args', metavar='SA', help='additional arguments to pass to sbatch')
        p_support_spawn.add_argument('--num', type=int, default=1, help='number of support jobs to spawn (default: 1)')

    def _support_output_file(self):
        # Replacement symbol for sbatch.
        slurm_job_id = '%j'
        path = (self.SUPPORT_OUTPUT_DIR
                    / self.SUPPORT_OUTPUT_FILE_TEMPLATE.format(slurm_job_id))

        return path

    def summary(self, args):
        logger.info('No action specified.')

    def attach(self, args):
        slurm_job_id = args.slurm_job_id

        proc = subprocess.run(['squeue', '--noheader',
                               '--user=' + os.environ['USER'],
                               '--name=' + self.conf.support_job_name,
                               '--format=%t\t%R',
                               '--jobs=' + str(slurm_job_id)],
                              capture_output=True, text=True)

        if not check_proc(proc, log=logger.error):
            return

        state, node = proc.stdout.strip().split('\t')

        if state != 'R':
            logger.error(f'Job is in state "{state}".')

            return

        socket_dir = self.conf.support_socket_dir_template.format(slurm_job_id)
        socket_path = os.path.join(socket_dir, 'tmux')

        logger.debug(f'Job is running on: {node}')
        logger.debug(f'Using socket path: {socket_path}')

        proc_args = ['ssh']
        proc_args.append('-t')
        proc_args.append(node)
        proc_args.append(f"tmux -S '{socket_path}' attach")

        logger.debug(f'Attaching to support job {slurm_job_id}.')

        os.execvp('ssh', proc_args)

    def list(self, args):
        columns = ['%A', '%t', '%R', '%P', '%C', '%l', '%m', '%S', '%e']
        format_str = '\t'.join(columns)

        proc = subprocess.run(['squeue', '--noheader', '--noconvert',
                               '--user=' + os.environ['USER'],
                               '--name=' + self.conf.support_job_name,
                               '--format=' + format_str],
                              capture_output=True, text=True)

        if not check_proc(proc, log=logger.error):
            return

        raw_time_starts = []
        support_data = []
        # Only output the warning once.
        mem_format_warn = False

        for line in proc.stdout.split('\n')[:-1]:
            (jobid, state, reason, partition, cpus, time_total, mem,
                    time_start, time_end) = line.split('\t')
            raw_time_starts.append(time_start)
            now = datetime.now()

            cpus = int(cpus)
            time_total = parse_slurm_timedelta(time_total)

            if mem[-1] == 'M' and mem[:-1].isdigit():
                mem = ceil(int(mem[:-1]) / 1024)
            else:
                if not mem_format_warn:
                    mem_format_warn = True
                    logger.warning('Invalid memory format.')

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

            support_data.append([jobid, state, reason, partition, cpus,
                                 time_total, mem, time_start, time_end])

        # Sort by start time.
        sorted_data = [row for (s, row) in sorted(zip(raw_time_starts,
                                                      support_data))]
        self.print_table(['Job ID', ('State', 2), 'Partition', 'Cores', 'Time',
                          'Mem (GB)', 'Start', 'End'],
                         sorted_data)

    def quit(self, args):
        all_jobs = args.all
        slurm_job_ids = set(args.slurm_job_id)

        if all_jobs:
            proc = subprocess.run(['squeue', '--noheader',
                                   '--user=' + os.environ['USER'],
                                   '--name=' + self.conf.support_job_name,
                                   '--format=%A'],
                                  capture_output=True, text=True)

            if not check_proc(proc, log=logger.error):
                return

            for slurm_job_id in proc.stdout.split('\n')[:-1]:
                slurm_job_ids.add(int(slurm_job_id))

        for slurm_job_id in slurm_job_ids:
            if self.mck.interrupted:
                break

            logger.debug(f'Attempting to cancel Slurm job {slurm_job_id}.')
            cancel_result = cancel_slurm_job(slurm_job_id,
                                             name=self.conf.support_job_name,
                                             signal='INT', log=logger.error)

            if cancel_result is None:
                return

            cancel_success, signalled_running = cancel_result

            if cancel_success:
                logger.info(slurm_job_id)

    def spawn(self, args):
        support_cpus = args.cpus
        support_time_hours = args.time_hr
        support_mem_gb = args.mem_gb
        sbatch_args = args.sbatch_args
        num = args.num

        if num < 1:
            logger.error('Must spawn at least 1 support job.')

            return

        support_time_minutes = ceil(support_time_hours * 60)
        support_mem_mb = ceil(support_mem_gb * 1024)

        proc_args = ['sbatch']
        proc_args.append('--parsable')
        proc_args.append('--job-name=' + self.conf.support_job_name)
        proc_args.append('--signal=B:INT@' + str(self.END_SIGNAL_SECONDS))
        proc_args.append('--chdir=' + str(self.conf.support_path))
        proc_args.append('--output=' + str(self._support_output_file()))
        proc_args.append('--cpus-per-task=' + str(support_cpus))
        proc_args.append('--time=' + str(support_time_minutes))
        proc_args.append('--mem=' + str(support_mem_mb))

        proc_args.extend(combine_shell_args(self.conf.general_sbatch_args,
                                            self.conf.support_sbatch_args,
                                            sbatch_args))

        execute_cmd = shlex.quote(self.conf.support_execute_cmd)
        socket_dir = self.conf.support_socket_dir_template.format('${SLURM_JOB_ID}')
        socket_path = os.path.join(socket_dir, 'tmux')

        os.makedirs(self.conf.support_path, exist_ok=True)
        os.makedirs(self.conf.support_path / self.SUPPORT_OUTPUT_DIR,
                    exist_ok=True)

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

        for _ in range(num):
            if self.mck.interrupted:
                break

            logger.debug('Spawning support job.')

            proc = subprocess.run(proc_args, input=script.strip(),
                                  capture_output=True, text=True)

            if not check_proc(proc, log=logger.error):
                return

            if ';' in proc.stdout:
                # Ignore the cluster name.
                slurm_job_id = int(proc.stdout.split(';', maxsplit=1)[0])
            else:
                slurm_job_id = int(proc.stdout)

            logger.info(slurm_job_id)
