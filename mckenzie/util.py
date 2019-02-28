from collections import defaultdict
from datetime import datetime


class HandledException(Exception):
    """
    Raised when an exception has been handled (likely by logging a message),
    but execution still needs to abort.
    """

    pass


def format_datetime(dt):
    # Convert to the local time zone, then forget the time zone.
    dt_local = dt.astimezone().replace(tzinfo=None)

    # Format as YYYY-MM-DD HH:MM:SS.
    return dt_local.isoformat(sep=' ', timespec='seconds')


FORMAT_FUNCTIONS = defaultdict(lambda: str, {
    datetime: format_datetime,
    type(None): lambda x: '',
})


def print_table(header, data, *, total=None):
    # Format data.
    data_str = []

    for row in data:
        row_str = []

        for elem in row:
            format_f = FORMAT_FUNCTIONS[type(elem)]
            row_str.append(format_f(elem))

        data_str.append(row_str)

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
        for t, w in zip(row, max_widths):
            print('| {{:{}}} '.format(w).format(t), end='')

        print('|')

    if total is not None:
        for w in max_widths:
            print('+-{{:{}}}-'.format(w).format('-'*w), end='')

        print('+')

        for t, w in zip(total_row, max_widths):
            print('| {{:{}}} '.format(w).format(t), end='')

        print('|')
