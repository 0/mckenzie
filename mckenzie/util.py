from collections import defaultdict
from datetime import datetime, timedelta


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


RIGHT_ALIGNS = set([
    datetime,
    int,
    timedelta,
])


def print_table(header, data, *, total=None):
    # Format data.
    data_str = []

    for row in data:
        row_str = []

        for elem in row:
            format_f = FORMAT_FUNCTIONS[type(elem)]
            row_str.append(format_f(elem))

        data_str.append(row_str)

    # Determine alignments.
    if len(data) > 0:
        row = data[0]
        aligns = ['>' if type(elem) in RIGHT_ALIGNS else '<' for elem in row]
    else:
        aligns = []

    # Compute column totals.
    if total is not None:
        total_row = []

        for col_idx in range(len(header)):
            tot = None

            if col_idx == 0:
                # Label.
                tot = total[0]
            elif col_idx in total[1]:
                for row in data:
                    elem = row[col_idx]

                    if elem is None:
                        continue

                    if tot is None:
                        tot = elem
                    else:
                        tot += elem

            format_f = FORMAT_FUNCTIONS[type(tot)]
            total_row.append(format_f(tot))

    # Compute column widths.
    max_widths = [len(x) for x in header]

    for row in data_str:
        for col_idx, elem in enumerate(row):
            width = len(elem)

            if max_widths[col_idx] < width:
                max_widths[col_idx] = width

    if total is not None:
        for col_idx, elem in enumerate(total_row):
            width = len(elem)

            if max_widths[col_idx] < width:
                max_widths[col_idx] = width

    # Output table.
    for t, w in zip(header, max_widths):
        print('| {{:{}}} '.format(w).format(t), end='')

    print('|')

    for w in max_widths:
        print('+-{{:{}}}-'.format(w).format('-'*w), end='')

    print('+')

    for row in data_str:
        for t, a, w in zip(row, aligns, max_widths):
            print('| {{:{}{}}} '.format(a, w).format(t), end='')

        print('|')

    if total is not None:
        for w in max_widths:
            print('+-{{:{}}}-'.format(w).format('-'*w), end='')

        print('+')

        for t, a, w in zip(total_row, aligns, max_widths):
            print('| {{:{}{}}} '.format(a, w).format(t), end='')

        print('|')
