from datetime import datetime, timedelta
from math import ceil
import os
import subprocess

from .util import (HandledException, check_proc, combine_shell_args,
                   format_datetime)


def parse_timedelta(s):
    # Input is of the form "days-hours:minutes:seconds", where we assume that
    # each piece except the last is optional. Sometimes there may also be a
    # ".microseconds" component, which we always remove.
    pieces = reversed(s.split('.')[0].replace('-', ':').split(':'))
    multipliers = [60, 60, 24, 0]

    total = 0

    for piece, multiplier in reversed(list(zip(pieces, multipliers))):
        total *= multiplier
        total += int(piece)

    return timedelta(seconds=total)


def parse_units_mb(s):
    if s[-1] == 'K':
        multiplier = 1/1024
        s = s[:-1]
    elif s[-1] == 'M':
        multiplier = 1
        s = s[:-1]
    elif s[-1] == 'G':
        multiplier = 1024
        s = s[:-1]
    elif s[-1] == 'T':
        multiplier = 1024 * 1024
        s = s[:-1]
    elif s[-1] == 'P':
        multiplier = 1024 * 1024 * 1024
        s = s[:-1]
    else:
        multiplier = 1/(1024*1024)

    return multiplier * float(s)


def check_scancel(proc, *, log, stacklevel=1):
    if 'scancel: Terminating job' in proc.stderr:
        return True
    elif 'scancel: Signal 2 to batch job' in proc.stderr:
        return True
    elif 'scancel: Signal 15 to batch job' in proc.stderr:
        return True
    elif 'scancel: error: No active jobs match' in proc.stderr:
        return False
    elif proc.returncode == 0:
        return False

    log(f'Encountered an error ({proc.returncode}).', stacklevel=stacklevel+1)

    if proc.stdout:
        log(proc.stdout.strip(), stacklevel=stacklevel+1)

    if proc.stderr:
        log(proc.stderr.strip(), stacklevel=stacklevel+1)

    return None

def cancel_job(slurm_job_id, *, name, signal, log, pending=True, running=True,
               stacklevel=1):
    if pending:
        # Try to cancel it before it gets a chance to run.
        proc = subprocess.run(['scancel', '--verbose', '--state=PENDING',
                               f'--jobname={name}', str(slurm_job_id)],
                              capture_output=True, text=True)
        cancel_success = check_scancel(proc, log=log, stacklevel=stacklevel+1)

        if cancel_success is None:
            # We encountered an error, so give up.
            raise HandledException()
        elif cancel_success:
            # Successfully cancelled pending job.
            return True, False

    if running:
        # It's already running (or finished), so try to send a signal.
        proc = subprocess.run(['scancel', '--verbose', '--state=RUNNING',
                               '--batch', f'--signal={signal}',
                               f'--jobname={name}', str(slurm_job_id)],
                              capture_output=True, text=True)
        signal_success = check_scancel(proc, log=log, stacklevel=stacklevel+1)

        if signal_success is None:
            # We encountered an error, so give up.
            raise HandledException()
        elif signal_success:
            # Successfully signalled running job.
            return True, True

    return False, None


def check_squeue(slurm_job_id, proc, *, log, stacklevel=1):
    if proc.stdout.strip() == str(slurm_job_id):
        return True
    elif 'slurm_load_jobs error: Invalid job id specified' in proc.stderr:
        return False
    elif proc.returncode == 0 and not proc.stdout:
        return False

    log(f'Encountered an error ({proc.returncode}).', stacklevel=stacklevel+1)

    if proc.stdout:
        log(proc.stdout.strip(), stacklevel=stacklevel+1)

    if proc.stderr:
        log(proc.stderr.strip(), stacklevel=stacklevel+1)

    return None

def does_job_exist(slurm_job_id, *, log, stacklevel=1):
    proc = subprocess.run(['squeue', '--noheader', '--format=%A',
                           '--jobs=' + str(slurm_job_id)],
                          capture_output=True, text=True)
    squeue_success = check_squeue(slurm_job_id, proc, log=log,
                                  stacklevel=stacklevel+1)

    if squeue_success is None:
        # We encountered an error, so give up.
        raise HandledException()

    return squeue_success


def list_all_jobs(name, columns, *, log, stacklevel=1):
    format_str = '\t'.join(columns)

    proc = subprocess.run(['squeue', '--noheader', '--noconvert',
                           '--user=' + os.environ['USER'],
                           '--name=' + name,
                           '--format=' + format_str],
                          capture_output=True, text=True)

    if not check_proc(proc, log=log, stacklevel=stacklevel+1):
        raise HandledException()

    result = []

    for line in proc.stdout.split('\n')[:-1]:
        result.append(line.split('\t'))

    return result


def list_completed_jobs(name, columns, last, *, log, stacklevel=1):
    format_str = ','.join(columns)

    now = datetime.now()

    proc = subprocess.run(['sacct', '--noheader', '--noconvert',
                           '--parsable2', '--state=CD',
                           '--starttime=' + format_datetime(now - last),
                           '--endtime=now',
                           '--name=' + name,
                           '--format=' + format_str],
                          capture_output=True, text=True)

    if not check_proc(proc, log=log, stacklevel=stacklevel+1):
        return

    result = []

    for line in proc.stdout.split('\n')[:-1]:
        result.append(line.split('|'))

    return result


def get_all_job_ids(name, *, log, stacklevel=1):
    jobs = list_all_jobs(name, ['%A'], log=log, stacklevel=stacklevel+1)

    return [int(job[0]) for job in jobs]


def get_job_id(*, log, stacklevel=1):
    try:
        return int(os.getenv('SLURM_JOB_ID'))
    except TypeError:
        log('Not running in a Slurm job.', stacklevel=stacklevel+1)

        raise HandledException()

def get_job_variables():
    job_node = os.getenv('SLURMD_NODENAME')
    job_cpus = int(os.getenv('SLURM_JOB_CPUS_PER_NODE'))
    job_mem_mb = int(os.getenv('SLURM_MEM_PER_NODE'))

    return job_node, job_cpus, job_mem_mb


def running_job_node(slurm_job_id, *, log, stacklevel=1):
    proc = subprocess.run(['squeue', '--noheader',
                           '--format=%t\t%R',
                           '--jobs=' + str(slurm_job_id)],
                          capture_output=True, text=True)

    if not check_proc(proc, log=log, stacklevel=stacklevel+1):
        raise HandledException()

    state, node = proc.stdout.strip().split('\t')

    if state != 'R':
        log(f'Job is in state "{state}".', stacklevel=stacklevel+1)

        raise HandledException()

    return node


class JobSubmitter:
    def __init__(self, *, name, signal, signal_seconds, chdir_path,
            output_file_path, cpus, time_hours, mem_gb, sbatch_argss, script,
            hold=False):
        time_minutes = ceil(time_hours * 60)
        mem_mb = ceil(mem_gb * 1024)

        self.proc_args = ['sbatch']
        self.proc_args.append('--parsable')
        self.proc_args.append('--job-name=' + name)
        self.proc_args.append('--signal=B:' + signal + '@' + str(signal_seconds))
        self.proc_args.append('--chdir=' + str(chdir_path))
        self.proc_args.append('--output=' + str(output_file_path))
        self.proc_args.append('--cpus-per-task=' + str(cpus))
        self.proc_args.append('--time=' + str(time_minutes))
        self.proc_args.append('--mem=' + str(mem_mb))

        if hold:
            self.proc_args.append('--hold')

        self.proc_args.extend(combine_shell_args(*sbatch_argss))

        self.script = script.strip()

        os.makedirs(chdir_path, exist_ok=True)
        os.makedirs(chdir_path / os.path.dirname(output_file_path),
                    exist_ok=True)

    def submit(self, *, log, stacklevel=1):
        proc = subprocess.run(self.proc_args, input=self.script,
                              capture_output=True, text=True)

        if not check_proc(proc, log=log, stacklevel=stacklevel+1):
            raise HandledException()

        if ';' in proc.stdout:
            # Ignore the cluster name.
            slurm_job_id = int(proc.stdout.split(';', maxsplit=1)[0])
        else:
            slurm_job_id = int(proc.stdout)

        return slurm_job_id


def release_job(slurm_job_id, *, log, stacklevel=1):
    proc = subprocess.run(['scontrol', 'release', str(slurm_job_id)],
                          capture_output=True, text=True)

    if not check_proc(proc, log=log, stacklevel=stacklevel+1):
        raise HandledException()
