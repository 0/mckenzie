import logging
from pathlib import Path
import shlex
import traceback

from .base import Manager
from .util import HandledException


logger = logging.getLogger(__name__)


class BatchManager(Manager):
    PROMPT = 'mck> '

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

        parser = self.mck._cmdline_parser(name='', global_args=False,
                                          add_help=False)

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

    def shell(self, args):
        logger.debug('Starting batch shell.')
        logger.info('Use "exit" or ^D to quit.')

        parser = self.mck._cmdline_parser(name='', global_args=False,
                                          add_help=False)
        commands = self.mck._parser_commands(parser)

        try:
            import readline
        except ModuleNotFoundError as e:
            logger.warning(f'Could not import "{e.name}"; expect degraded '
                           'experience.')
        else:
            def completer(text, state):
                context = readline.get_line_buffer()[:readline.get_begidx()]

                cmd_dict = commands

                for word in context.split():
                    cmd_dict = cmd_dict[word]

                applicable_commands = []

                for cmd in cmd_dict:
                    if not cmd.startswith(text):
                        continue

                    applicable_commands.append(cmd)

                if applicable_commands == [text]:
                    # We've selected the only possibility.
                    if cmd_dict[text]:
                        # There are subcommands.
                        applicable_commands = [text + ' ']
                    else:
                        # We've reached the end of the line.
                        applicable_commands = []

                try:
                    return sorted(applicable_commands)[state]
                except IndexError:
                    return None

            readline.set_completer_delims(' ')
            readline.set_completer(completer)
            readline.parse_and_bind('tab: complete')

        try:
            while True:
                try:
                    pre_line = input(self.PROMPT)
                except EOFError:
                    logger.debug('EOF received.')

                    break
                except KeyboardInterrupt:
                    logger.debug('Keyboard interrupt received.')
                    print()

                    continue

                line = pre_line.rstrip()

                if not line:
                    continue

                if line == 'exit':
                    break

                try:
                    batch_args = parser.parse_args(shlex.split(line))
                except SystemExit:
                    continue

                try:
                    self.mck.call_manager(batch_args)
                except Exception:
                    traceback.print_exc()
        finally:
            print()

        logger.debug('Exiting batch shell.')
