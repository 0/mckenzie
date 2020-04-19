import argparse
from collections import defaultdict
from contextlib import contextmanager
import logging
import os
import signal
from threading import Event

from .batch import BatchManager
from .color import Colorizer
from .conf import Conf
from .database import DatabaseManager
from .task import TaskManager
from .util import foreverdict
from .worker import WorkerManager

# For export.
from .util import HandledException


logger = logging.getLogger(__name__)


class McKenzie:
    ENV_COLOR = 'MCKENZIE_COLOR'
    ENV_CONF = 'MCKENZIE_CONF'

    MANAGERS = {
        'batch': BatchManager,
        'database': DatabaseManager,
        'task': TaskManager,
        'worker': WorkerManager,
    }

    PREFLIGHT_ARGS = defaultdict(dict, {
        'batch': {
            'database_init': False,
            'database_current': False,
        },
        'database': {
            'database_init': False,
            'database_current': False,
        },
    })

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
        p_sub = p.add_subparsers(dest='command')

        if global_args:
            p.add_argument('--conf', help=f'path to configuration file (overrides {cls.ENV_CONF} environment variable)')
            p.add_argument('--unsafe', action='store_true', help='skip safety checks')
            p.add_argument('-q', '--quiet', action='count', help='decrease log verbosity (may be used multiple times)')
            p.add_argument('-v', '--verbose', action='count', help='increase log verbosity (may be used multiple times)')
            p.add_argument('--color', action='store_true', help=f'use color in output (overrides {cls.ENV_COLOR} environment variable)')

        # batch
        p_batch = p_sub.add_parser('batch', help='batch command execution')
        p_batch_sub = p_batch.add_subparsers(dest='subcommand')

        # batch run
        p_batch_run = p_batch_sub.add_parser('run', help='run commands from a file')
        p_batch_run.add_argument('--progress', action='store_true', help='show progress')
        p_batch_run.add_argument('path', nargs='*', help='path to file of commands')

        # batch shell
        p_batch_shell = p_batch_sub.add_parser('shell', help='run commands interactively')

        # database
        p_database = p_sub.add_parser('database', help='database management')
        p_database_sub = p_database.add_subparsers(dest='subcommand')

        # database backup
        p_database_backup = p_database_sub.add_parser('backup', help='back up database')

        # database client
        p_database_client = p_database_sub.add_parser('client', help='connect to database')

        # database list
        p_database_list = p_database_sub.add_parser('list', help='list database jobs')

        # database load-schema
        p_database_load_schema = p_database_sub.add_parser('load-schema', help='load schema')

        # database quit
        p_database_quit = p_database_sub.add_parser('quit', help='signal database jobs to quit')
        p_database_quit.add_argument('--current', action='store_true', help='signal currently active database')
        p_database_quit.add_argument('--all', action='store_true', help='signal all database jobs')
        p_database_quit.add_argument('slurm_job_id', nargs='*', type=int, help='Slurm job ID of database')

        # database run
        p_database_run = p_database_sub.add_parser('run', help='run database')

        # database spawn
        p_database_spawn = p_database_sub.add_parser('spawn', help='spawn Slurm database job')
        p_database_spawn.add_argument('--cpus', metavar='C', type=int, required=True, help='number of CPUs')
        p_database_spawn.add_argument('--time-hr', metavar='T', type=float, required=True, help='time limit in hours')
        p_database_spawn.add_argument('--mem-gb', metavar='M', type=float, required=True, help='amount of memory in GB')
        p_database_spawn.add_argument('--sbatch-args', metavar='SA', help='additional arguments to pass to sbatch')

        # task
        p_task = p_sub.add_parser('task', help='task management')
        p_task_sub = p_task.add_subparsers(dest='subcommand')

        # task add
        p_task_add = p_task_sub.add_parser('add', help='create a new task')
        p_task_add.add_argument('--time-hr', metavar='T', type=float, required=True, help='time limit in hours')
        p_task_add.add_argument('--mem-gb', metavar='M', type=float, required=True, help='memory limit in GB')
        p_task_add.add_argument('--priority', metavar='P', type=int, default=0, help='priority (default: 0)')
        p_task_add.add_argument('--depends-on', metavar='DEP', action='append', help='task DEP is a dependency')
        p_task_add.add_argument('--soft-depends-on', metavar='DEP', action='append', help='task DEP is a soft dependency')
        p_task_add.add_argument('name', help='task name')

        # task cancel
        p_task_cancel = p_task_sub.add_parser('cancel', help='change task state to "cancelled"')
        p_task_cancel.add_argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
        p_task_cancel.add_argument('name', nargs='*', help='task name')

        # task clean
        p_task_clean = p_task_sub.add_parser('clean', help='run clean command for "cleanable" tasks')
        p_task_clean.add_argument('--ignore-pending-dependents', action='store_true', help='include tasks that have pending direct dependents')

        # task cleanablize
        p_task_cleanablize = p_task_sub.add_parser('cleanablize', help='change task state from "synthesized" to "cleanable"')
        p_task_cleanablize.add_argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
        p_task_cleanablize.add_argument('name', nargs='*', help='task name')

        # task hold
        p_task_hold = p_task_sub.add_parser('hold', help='change task state to "held"')
        p_task_hold.add_argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
        p_task_hold.add_argument('name', nargs='*', help='task name')

        # task list
        p_task_list = p_task_sub.add_parser('list', help='list tasks')
        p_task_list.add_argument('--state', metavar='S', help='only tasks in state S')
        p_task_list.add_argument('--name-pattern', metavar='P', help='only tasks with names matching the SQL LIKE pattern P')
        p_task_list.add_argument('--allow-all', action='store_true', help='allow all tasks to be listed')

        # task list-claimed
        p_task_list_claimed = p_task_sub.add_parser('list-claimed', help='list claimed tasks')
        p_task_list_claimed.add_argument('--state', metavar='S', help='only tasks in state S')
        p_task_list_claimed.add_argument('--name-pattern', metavar='P', help='only tasks with names matching the SQL LIKE pattern P')
        p_task_list_claimed.add_argument('--longer-than-hr', metavar='T', type=float, help='only tasks claimed for longer than T hours')

        # task release
        p_task_release = p_task_sub.add_parser('release', help='change "held" task state to "waiting"')
        p_task_release.add_argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
        p_task_release.add_argument('name', nargs='*', help='task name')

        # task rerun
        p_task_rerun = p_task_sub.add_parser('rerun', help='rerun a task and all its dependents')
        p_task_rerun.add_argument('name', help='task name')

        # task reset-claimed
        p_task_reset_claimed = p_task_sub.add_parser('reset-claimed', help='unclaim abandoned tasks')
        p_task_reset_claimed.add_argument('name', nargs='*', help='task name')

        # task reset-failed
        p_task_reset_failed = p_task_sub.add_parser('reset-failed', help='reset all "failed" tasks to "waiting"')

        # task show
        p_task_show = p_task_sub.add_parser('show', help='show task details')
        p_task_show.add_argument('name', help='task name')

        # task synthesize
        p_task_synthesize = p_task_sub.add_parser('synthesize', help='synthesize completed tasks')

        # task uncancel
        p_task_uncancel = p_task_sub.add_parser('uncancel', help='change "cancelled" task state to "waiting"')
        p_task_uncancel.add_argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
        p_task_uncancel.add_argument('name', nargs='*', help='task name')

        # task uncleanablize
        p_task_uncleanablize = p_task_sub.add_parser('uncleanablize', help='change task state from "cleanable" to "synthesized"')
        p_task_uncleanablize.add_argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
        p_task_uncleanablize.add_argument('name', nargs='*', help='task name')

        # worker
        p_worker = p_sub.add_parser('worker', help='worker management')
        p_worker_sub = p_worker.add_subparsers(dest='subcommand')

        # worker ack-failed
        p_worker_ack_failed = p_worker_sub.add_parser('ack-failed', help='acknowledge all failed workers')

        # worker clean
        p_worker_clean = p_worker_sub.add_parser('clean', help='clean up dead workers')
        p_worker_clean.add_argument('--state', metavar='S', help='only workers in state S')

        # worker list
        p_worker_list = p_worker_sub.add_parser('list', help='list workers')
        p_worker_list.add_argument('--state', metavar='S', help='only workers in state S')

        # worker list-queued
        p_worker_list_queued = p_worker_sub.add_parser('list-queued', help='list queued workers')

        # worker quit
        p_worker_quit = p_worker_sub.add_parser('quit', help='signal worker job to quit')
        p_worker_quit.add_argument('--abort', action='store_true', help='quit immediately, killing running tasks')
        p_worker_quit.add_argument('--all', action='store_true', help='signal all worker jobs')
        p_worker_quit.add_argument('--state', metavar='S', help='only workers in state S for "--all"')
        p_worker_quit.add_argument('slurm_job_id', nargs='*', type=int, help='Slurm job ID of worker')

        # worker run
        p_worker_run = p_worker_sub.add_parser('run', help='run worker inside Slurm job')

        # worker show
        p_worker_show = p_worker_sub.add_parser('show', help='show worker details')
        p_worker_show.add_argument('slurm_job_id', type=int, help='Slurm job ID of worker')

        # worker spawn
        p_worker_spawn = p_worker_sub.add_parser('spawn', help='spawn Slurm worker job')
        p_worker_spawn.add_argument('--cpus', metavar='C', type=int, required=True, help='number of CPUs')
        p_worker_spawn.add_argument('--time-hr', metavar='T', type=float, required=True, help='time limit in hours')
        p_worker_spawn.add_argument('--mem-gb', metavar='M', type=float, required=True, help='amount of memory in GB')
        p_worker_spawn.add_argument('--sbatch-args', metavar='SA', help='additional arguments to pass to sbatch')
        p_worker_spawn.add_argument('--num', type=int, default=1, help='number of workers to spawn (default: 1)')

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

    def _interrupt(self, signum=None, frame=None):
        logger.debug('Interrupted.')
        self.interrupted.set()
        # If we recieve the signal again, abort in the usual fashion.
        signal.signal(signal.SIGINT, signal.default_int_handler)

    def __init__(self, conf_path, *, unsafe=False, use_colors=False):
        self.conf = Conf(conf_path)
        self.unsafe = True if unsafe else self.conf.general_unsafe
        self.colorizer = Colorizer(use_colors)

        self.interrupted = Event()
        signal.signal(signal.SIGINT, self._interrupt)

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

        if not self._preflight(**self.PREFLIGHT_ARGS[mgr_name]):
            return

        mgr = self.MANAGERS[mgr_name](self)
        getattr(mgr, subcmd)(args)

    @contextmanager
    def without_sigint(self):
        # Store the real handler and temporarily install the one that raises
        # KeyboardInterrupt.
        real_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal.default_int_handler)

        try:
            yield
        finally:
            # Put back the real handler.
            signal.signal(signal.SIGINT, real_handler)
