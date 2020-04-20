from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
import fcntl
import subprocess


class HandledException(Exception):
    """
    Raised when an exception has been handled (likely by logging a message),
    but execution still needs to abort.
    """

    pass


def foreverdict():
    return defaultdict(foreverdict)


def format_datetime(dt):
    # Convert to the local time zone, then forget the time zone.
    dt_local = dt.astimezone().replace(tzinfo=None)

    # Format as YYYY-MM-DD HH:MM:SS.
    return dt_local.isoformat(sep=' ', timespec='seconds')

def format_int(n):
    pieces = []

    for i, c in enumerate(reversed(str(n))):
        if i % 3 == 0:
            pieces.append(c)
        else:
            pieces[-1] = c + pieces[-1]

    return ','.join(reversed(pieces))

def format_timedelta(td):
    hms = int(td.total_seconds())
    sign = '-' if hms < 0 else ''
    hm, s = divmod(abs(hms), 60)
    h, m = divmod(hm, 60)

    # Format as [-]H...HH:MM:SS.
    return f'{sign}{h}:{m:02}:{s:02}'

FORMAT_FUNCTIONS = defaultdict(lambda: str, {
    datetime: format_datetime,
    int: format_int,
    timedelta: format_timedelta,
    type(None): lambda x: '',
})

def format_object(x):
    return FORMAT_FUNCTIONS[type(x)](x)


RIGHT_ALIGNS = {
    datetime,
    int,
    timedelta,
}

def print_table(pre_header, pre_data, *, reset_str, total=None):
    # Convert all header entries to tuples.
    header = []

    for heading in pre_header:
        if isinstance(heading, tuple):
            header.append(heading)
        else:
            header.append((heading, 1))

    # Convert all data entries to tuples.
    data = []

    for row in pre_data:
        data_row = []

        for elem in row:
            if isinstance(elem, tuple):
                data_row.append(elem)
            else:
                data_row.append((elem, None))

        data.append(data_row)

    num_rows = len(data)
    num_cols = sum(heading[1] for heading in header)

    # Format data.
    data_str = []
    align_types = [None for _ in range(num_cols)]

    for row in data:
        row_str = []

        for col_idx, (elem, color) in enumerate(row):
            row_str.append((format_object(elem), color))

            if align_types[col_idx] is None and elem is not None:
                align_types[col_idx] = type(elem)

        data_str.append(row_str)

    # Compute column totals.
    if total is not None:
        total_row = []

        for col_idx in range(num_cols):
            tot = None

            if col_idx == 0:
                # Label.
                tot = total[0]
            else:
                try:
                    tot_idx = total[1].index(col_idx)
                except ValueError:
                    pass
                else:
                    # Initial value.
                    tot = total[2][tot_idx]

                    for row in data:
                        elem, color = row[col_idx]

                        if elem is None or isinstance(elem, str):
                            continue

                        tot += elem

            total_row.append(format_object(tot))

            if align_types[col_idx] is None and tot is not None:
                align_types[col_idx] = type(tot)

    # Determine alignments.
    aligns = ['>' if t in RIGHT_ALIGNS else '<' for t in align_types]

    # Compute column widths and separators.
    max_widths = [0 for _ in range(num_cols)]

    for row in data_str:
        for col_idx, (elem, color) in enumerate(row):
            width = len(elem)

            if max_widths[col_idx] < width:
                max_widths[col_idx] = width

    if total is not None:
        for col_idx, elem in enumerate(total_row):
            width = len(elem)

            if max_widths[col_idx] < width:
                max_widths[col_idx] = width

    first_subcols = []
    paddings = []
    max_widths_header = []
    idx = 0

    for heading, cols in header:
        data_width = sum(max_widths[idx:(idx+cols)]) + 3 * (cols - 1)

        if data_width >= len(heading):
            max_widths_header.append(data_width)
            paddings.append(0)
        else:
            max_widths_header.append(len(heading))
            paddings.append(len(heading) - data_width)

        first_subcols.append(True)
        first_subcols.extend([False] * (cols-1))
        paddings.extend([0] * (cols-1))

        idx += cols

    # Output table.
    for (t, _), w in zip(header, max_widths_header):
        print('| {{:<{}}} '.format(w).format(t), end='')

    print('|')

    for w in max_widths_header:
        print('+-{{:<{}}}-'.format(w).format('-'*w), end='')

    print('+')

    for row in data_str:
        for (t, clr), f, p, a, w in zip(row, first_subcols, paddings, aligns,
                                        max_widths):
            s = '|' if f else '/'

            if clr is None:
                clr = ''
                clr_reset = ''
            else:
                clr_reset = reset_str

            fmt = '{} {}{}{{:{}{}}}{} '.format(s, ' ' * p, clr, a, w,
                                               clr_reset)
            print(fmt.format(t), end='')

        print('|')

    if total is not None:
        for w in max_widths_header:
            print('+-{{:<{}}}-'.format(w).format('-'*w), end='')

        print('+')

        for t, f, p, a, w in zip(total_row, first_subcols, paddings, aligns,
                                 max_widths):
            s = '|' if f else '/'
            print('{} {}{{:{}{}}} '.format(s, ' ' * p, a, w).format(t), end='')

        print('|')


def humanize_datetime(dt, now):
    if dt <= now:
        delta = now - dt
        template = '{} ago'
    else:
        delta = dt - now
        template = 'in {}'

    if delta >= timedelta(hours=24):
        return format_datetime(dt)

    delta_fmt = format_timedelta(delta)

    if delta_fmt == '0:00:00':
        return 'now'

    return template.format(delta_fmt)


def parse_slurm_timedelta(s):
    # Input is of the form "days-hours:minutes:seconds", where we assume that
    # each piece except the last is optional.
    pieces = reversed(s.replace('-', ':').split(':'))
    multipliers = [60, 60, 24, 0]

    total = 0

    for piece, multiplier in reversed(list(zip(pieces, multipliers))):
        total *= multiplier
        total += int(piece)

    return timedelta(seconds=total)


def check_proc(proc, *, log):
    if proc.returncode != 0:
        log(f'Encountered an error ({proc.returncode}).')

        if proc.stdout:
            log(proc.stdout.strip())

        if proc.stderr:
            log(proc.stderr.strip())

        return False

    return True


def check_scancel(proc, *, log):
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

    log(f'Encountered an error ({proc.returncode}).')

    if proc.stdout:
        log(proc.stdout.strip())

    if proc.stderr:
        log(proc.stderr.strip())

    return None


def cancel_slurm_job(slurm_job_id, *, name, signal, log):
    # Try to cancel it before it gets a chance to run.
    proc = subprocess.run(['scancel', '--verbose', '--state=PENDING',
                           f'--jobname={name}', str(slurm_job_id)],
                          capture_output=True, text=True)
    cancel_success = check_scancel(proc, log=log)

    if cancel_success is None:
        # We encountered an error, so give up.
        return None
    elif cancel_success:
        # Successfully cancelled pending job.
        return True, False

    # It's already running (or finished), so try to send a signal.
    proc = subprocess.run(['scancel', '--verbose', '--state=RUNNING',
                           '--batch', f'--signal={signal}',
                           f'--jobname={name}', str(slurm_job_id)],
                          capture_output=True, text=True)
    signal_success = check_scancel(proc, log=log)

    if signal_success is None:
        # We encountered an error, so give up.
        return None
    elif signal_success:
        # Successfully signalled running job.
        return True, True

    return False, None


def check_squeue(slurm_job_id, proc, *, log):
    if proc.stdout.strip() == str(slurm_job_id):
        return True
    elif 'slurm_load_jobs error: Invalid job id specified' in proc.stderr:
        return False
    elif proc.returncode == 0 and not proc.stdout:
        return False

    log(f'Encountered an error ({proc.returncode}).')

    if proc.stdout:
        log(proc.stdout.strip())

    if proc.stderr:
        log(proc.stderr.strip())

    return None


def mem_rss_mb(pid, *, log):
    try:
        # Get the memory usage of each process in the session.
        proc = subprocess.run(['ps', '-o', 'rss=', '--sid', str(pid)],
                              capture_output=True, text=True)
    except OSError:
        return None

    if not check_proc(proc, log=log):
        return None

    result = 0

    for line in proc.stdout.split():
        try:
            # The units of rss should be KB.
            result += int(line) // 1024
        except ValueError:
            return None

    return result


@contextmanager
def flock(path):
    # Open for writing so that it works over NFS as well.
    with open(path, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)

        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


class DirectedAcyclicGraphIterator:
    def __init__(self, root):
        self.root = root

        self.chain = [self.root]
        self.seen = set(self.chain)

    def __iter__(self):
        return self

    def __next__(self):
        if not self.chain:
            raise StopIteration()

        while True:
            for child in self.chain[-1].children:
                if child in self.seen:
                    continue

                self.chain.append(child)
                self.seen.add(child)

                break
            else:
                return self.chain.pop().data


class DirectedAcyclicGraphNode:
    def __init__(self, data):
        self.data = data

        self.children = set()

    def add(self, child):
        self.children.add(child)

    def __iter__(self):
        return DirectedAcyclicGraphIterator(self)
