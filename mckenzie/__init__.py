import argparse
import logging
import os

from .base import Manager
from .color import Colorizer
from .conf import Conf
from .util import event_on_sigint, foreverdict

# To register the managers.
from . import batch, database, debug, support, task, worker

# For export.
from .util import HandledException


logger = logging.getLogger(__name__)


class McKenzie:
    ENV_COLOR = 'MCKENZIE_COLOR'
    ENV_CONF = 'MCKENZIE_CONF'

    @staticmethod
    def _parser_commands(p):
        parsers = [((), p)]
        commands = foreverdict()

        while parsers:
            pieces, parser = parsers.pop(0)

            for action in parser._actions:
                if not isinstance(action, argparse._SubParsersAction):
                    continue

                for name, subparser in action.choices.items():
                    pieces_new = pieces + (name,)
                    parsers.append((pieces_new, subparser))

                    d = commands

                    for piece in pieces_new:
                        d = d[piece]

        return commands

    @classmethod
    def _cmdline_parser(cls, *, name='mck', global_args=True, add_help=True):
        p = argparse.ArgumentParser(prog=name, add_help=add_help)

        if global_args:
            p.add_argument('--conf', help=f'path to configuration file (overrides {cls.ENV_CONF} environment variable)')
            p.add_argument('--unsafe', action='store_true', help='skip safety checks')
            p.add_argument('-q', '--quiet', action='count', help='decrease log verbosity (may be used multiple times)')
            p.add_argument('-v', '--verbose', action='count', help='increase log verbosity (may be used multiple times)')
            p.add_argument('--color', action='store_true', help=f'use color in output (overrides {cls.ENV_COLOR} environment variable)')

        subparsers = {None: p.add_subparsers(dest='command')}

        for M in Manager.all_managers():
            parts = M.name.rsplit('.', maxsplit=1)

            if len(parts) == 1:
                prefix = None
            else:
                prefix = parts[0]

            subparser = M.add_cmdline_parser(subparsers[prefix],
                                             level=M.name.count('.') + 1)
            subparsers[M.name] = subparser

        return p

    @classmethod
    def from_args(cls, argv):
        parser = cls._cmdline_parser()
        args = parser.parse_args(argv)

        verbosity_change = 0

        if args.quiet is not None:
            verbosity_change += 10 * args.quiet

        if args.verbose is not None:
            verbosity_change -= 10 * args.verbose

        root_logger = logging.getLogger()
        new_level = max(1, root_logger.getEffectiveLevel() + verbosity_change)
        root_logger.setLevel(new_level)

        if args.conf is not None:
            conf_path = args.conf
        else:
            try:
                conf_path = os.environ[cls.ENV_CONF]
            except KeyError:
                logger.error('Path to configuration file must be specified '
                             f'via either --conf option or {cls.ENV_CONF} '
                             'environment variable.')

                return

        if args.command is None:
            parser.print_usage()

            return

        if args.color:
            use_colors = True
        else:
            try:
                use_colors = bool(os.environ[cls.ENV_COLOR])
            except KeyError:
                use_colors = False

        mck = McKenzie(conf_path, unsafe=args.unsafe, use_colors=use_colors)
        mck.call_manager(args)

    def __init__(self, conf_path, *, unsafe=False, use_colors=False):
        self.conf = Conf(conf_path)
        self.unsafe = True if unsafe else self.conf.general_unsafe
        self.colorizer = Colorizer(use_colors)

        self._reset_interrupt()

    def _preflight(self, *, database_init=True, database_current=True):
        if self.unsafe:
            log = logger.warning
        else:
            log = logger.error

        if database_init and not self.conf.db.is_initialized(log=log):
            if not self.unsafe:
                return False

        if database_current and not self.conf.db.is_current(log=log):
            if not self.unsafe:
                return False

        return True

    def _reset_interrupt(self):
        self._interrupt_event = event_on_sigint(log=logger.debug)

    @property
    def interrupted(self):
        return self._interrupt_event.is_set()

    def call_manager(self, args):
        mgr_name = args.command
        subcmd = args.subcommand
        sub_level = 1

        while subcmd is not None:
            sub_level += 1
            label = 'sub'*sub_level + 'command'

            try:
                subcmd_new = getattr(args, label)
            except AttributeError:
                break

            mgr_name += '.' + subcmd
            subcmd = subcmd_new

        if subcmd is None:
            subcmd = 'summary'
        else:
            subcmd = subcmd.replace('-', '_')

        M = Manager.get_manager(mgr_name)
        mgr = M(self)
        cmd_f = getattr(mgr, subcmd)

        preflight_kwargs = {}

        try:
            preflight_kwargs.update(mgr._preflight_kwargs)
        except AttributeError:
            pass

        try:
            preflight_kwargs.update(cmd_f._preflight_kwargs)
        except AttributeError:
            pass

        if not self._preflight(**preflight_kwargs):
            return

        cmd_f(args)
