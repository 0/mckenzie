from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
import fcntl
import shlex
import signal
import subprocess
from threading import Event


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
    max_widths_header = []
    idx = 0

    for heading, cols in header:
        data_width = sum(max_widths[idx:(idx+cols)]) + 3 * (cols - 1)

        if data_width >= len(heading):
            max_widths_header.append(data_width)
        else:
            max_widths_header.append(len(heading))

            if aligns[idx] == '>':
                # Extend the left-most column if it's right-aligned.
                col_idx = idx
            else:
                # Extend the right-most column.
                col_idx = idx + cols - 1

            max_widths[col_idx] += len(heading) - data_width

        first_subcols.append(True)
        first_subcols.extend([False] * (cols-1))

        idx += cols

    # Output table.
    for (t, _), w in zip(header, max_widths_header):
        print('| {{:<{}}} '.format(w).format(t), end='')

    print('|')

    for w in max_widths_header:
        print('+-{{:<{}}}-'.format(w).format('-'*w), end='')

    print('+')

    for row in data_str:
        for (t, clr), f, a, w in zip(row, first_subcols, aligns, max_widths):
            s = '|' if f else '/'

            if clr is None:
                clr = ''
                clr_reset = ''
            else:
                clr_reset = reset_str

            fmt = '{} {}{{:{}{}}}{} '.format(s, clr, a, w, clr_reset)
            print(fmt.format(t), end='')

        print('|')

    if total is not None:
        for w in max_widths_header:
            print('+-{{:<{}}}-'.format(w).format('-'*w), end='')

        print('+')

        for t, f, a, w in zip(total_row, first_subcols, aligns, max_widths):
            s = '|' if f else '/'
            print('{} {{:{}{}}} '.format(s, a, w).format(t), end='')

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


def combine_shell_args(*argss):
    result = []

    for args in argss:
        if args is None:
            continue

        lst = shlex.split(args)

        if lst and lst[0] == '+':
            result.extend(lst[1:])
        else:
            result = lst

    return result


def check_proc(proc, *, log, stacklevel=1):
    if proc.returncode != 0:
        log(f'Encountered an error ({proc.returncode}).',
            stacklevel=stacklevel+1)

        if proc.stdout:
            log(proc.stdout.strip(), stacklevel=stacklevel+1)

        if proc.stderr:
            log(proc.stderr.strip(), stacklevel=stacklevel+1)

        return False

    return True


def mem_rss_mb(pid, *, log, stacklevel=1):
    try:
        # Get the memory usage of each process in the session.
        proc = subprocess.run(['ps', '-o', 'rss=', '--sid', str(pid)],
                              capture_output=True, text=True)
    except OSError:
        return None

    if not check_proc(proc, log=log, stacklevel=stacklevel+1):
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


def event_on_sigint(*, log, stacklevel=1):
    event = Event()

    def _interrupt(signum=None, frame=None):
        log('Interrupted.', stacklevel=stacklevel+1)
        event.set()
        # If we recieve the signal again, abort in the usual fashion.
        signal.signal(signal.SIGINT, signal.default_int_handler)

    signal.signal(signal.SIGINT, _interrupt)

    return event


@contextmanager
def without_sigint():
    # Store the real handler and temporarily install the one that raises
    # KeyboardInterrupt.
    real_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, signal.default_int_handler)

    try:
        yield
    finally:
        # Put back the real handler.
        signal.signal(signal.SIGINT, real_handler)


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
