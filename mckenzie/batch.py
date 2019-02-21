import logging
from pathlib import Path
import shlex

from .base import Manager
from .util import HandledException


logger = logging.getLogger(__name__)


class BatchManager(Manager):
    def summary(self, args):
        logger.info('No action specified.')

    def run(self, args):
        progress = args.progress
        paths = args.path

        lines = []

        for pre_path in paths:
            path = Path(pre_path)

            try:
                logger.debug(f'Reading commands from "{path}".')

                with open(path) as f:
                    lines.extend(f.readlines())
            except OSError as e:
                logger.error(f'Failed to read commands from "{path}" '
                             f'({e.strerror}).')

                raise HandledException()

        parser = self.mck._cmdline_parser()

        try:
            for i, pre_line in enumerate(lines):
                line = pre_line.rstrip()
                batch_args = parser.parse_args(shlex.split(line))

                if progress:
                    print(f'\r{i+1}/{len(lines)}', end='')

                self.mck.call_manager(batch_args)
        finally:
            if progress:
                print()
