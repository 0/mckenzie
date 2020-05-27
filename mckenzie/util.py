from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
import fcntl
from itertools import zip_longest
from math import ceil
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
    type(Ellipsis): lambda x: '...',
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

    num_cols = sum(heading[1] for heading in header)

    # Convert all data entries to tuples and expand ellipses.
    data = []

    for row in pre_data:
        data_row = []

        if row is Ellipsis:
            row = [Ellipsis] * num_cols

        for elem in row:
            if isinstance(elem, tuple):
                data_row.append(elem)
            else:
                data_row.append((elem, None))

        data.append(data_row)

    num_rows = len(data)

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

def print_histograms(headers, pre_datas):
    S = 10

    # Format data and determine alignment.
    datas = []
    aligns = []

    for pre_data in pre_datas:
        data = []

        for _, left, right, count in pre_data:
            data.append((format_object(left), count))

        data.append((format_object(right), None))

        datas.append(data)
        aligns.append('>' if type(right) in RIGHT_ALIGNS else '<')

    # Compute widths.
    max_edge_widths = []
    max_counts = []

    for data_idx, data in enumerate(datas):
        max_edge_width = len(headers[data_idx])
        max_count = -1

        for edge, count in data:
            formatted = format_object(edge)
            edge_width = len(formatted)

            if max_edge_width < edge_width:
                max_edge_width = edge_width

            if count is not None and max_count < count:
                max_count = count

        max_edge_widths.append(max_edge_width)
        max_counts.append(max_count)

    # Output histograms.
    for data_idx, header in enumerate(headers):
        bar = ' ' * S
        fmt = '| {{:{}}} {{}} '.format(max_edge_widths[data_idx])
        print(fmt.format(header, bar), end='')

    print('|')

    for width in max_edge_widths:
        fmt = '+-{{:-<{}}}-{{:-<{}}}-'.format(width, S)
        print(fmt.format('', ''), end='')

    print('+')

    for items in zip_longest(*datas):
        for data_idx, item in enumerate(items):
            if item is None:
                edge = ''
                count = None
            else:
                edge, count = item

            if count is not None:
                proportion = ceil(count / max_counts[data_idx] * S)
            else:
                proportion = 0

            bar = ('#' * proportion) + (' ' * (S - proportion))
            fmt = '| {{:{}{}}} {{}} '.format(aligns[data_idx],
                                             max_edge_widths[data_idx])

            print(fmt.format(edge, bar), end='')

        print('|')


def print_time_series(data, *, reset_str):
    S = 10

    # Find the maximum size of a single bar.
    max_total = -1

    for items in data:
        total = sum(x[1] for x in items)

        if max_total < total:
            max_total = total

    # Format the bars.
    bars = []
    # We always leave enough room for S segments, even if there are no bars to
    # be shown. However, it's possible to have more than S segments due to
    # rounding, so we must count them.
    max_bar_length = S

    for items in data:
        bar = []

        for color, count in items:
            proportion = ceil(count / max_total * S)
            bar.extend([f'{color}#{reset_str}'] * proportion)

        bars.append(bar)

        if max_bar_length < len(bar):
            max_bar_length = len(bar)

    # Output time series.
    for i in range(max_bar_length, 0, -1):
        for bar in bars:
            try:
                c = bar[i-1]
            except IndexError:
                c = ' '

            print(c, end='')

        print()

    for i in range(len(bars), 0, -1):
        print(i % 10, end='')

    print()


def humanize_datetime(dt, now, *, force=False):
    if dt <= now:
        delta = now - dt
        template = '{} ago'
    else:
        delta = dt - now
        template = 'in {}'

    if not force and delta >= timedelta(hours=24):
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


SENSITIVE_VARIABLES = {
    'PGPASSWORD': '********',
}

def assemble_command(proc_args, proc_env=None):
    envs = []

    if proc_env is not None:
        for name, value in proc_env.items():
            try:
                value = SENSITIVE_VARIABLES[name]
            except KeyError:
                pass

            envs.append(f'{name}={shlex.quote(value)}')

    return ' '.join([*envs, shlex.join(proc_args)])


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
