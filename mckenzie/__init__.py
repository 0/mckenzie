import argparse
from collections import defaultdict
import logging
import os

from .batch import BatchManager
from .color import Colorizer
from .conf import Conf
from .database import DatabaseManager, DatabaseMigrationManager
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
        'database.migration': DatabaseMigrationManager,
        'task': TaskManager,
        'worker': WorkerManager,
    }

    PREFLIGHT_ARGS = defaultdict(dict, {
        'batch': {
            'database_init': False,
            'database_update': False,
        },
        'database': {
            'database_init': False,
            'database_update': False,
        },
        'database.migration': {
            'database_update': False,
        },
    })

    @staticmethod
    def _cmdline_parser(*, name='mck', global_args=True, add_help=True):
        p = argparse.ArgumentParser(prog=name, add_help=add_help)
        p_sub = p.add_subparsers(dest='command')

        if global_args:
            p.add_argument('--conf', help='path to configuration file')
            p.add_argument('--unsafe', action='store_true', help='skip safety checks')
            p.add_argument('-q', '--quiet', action='count', help='decrease verbosity')
            p.add_argument('-v', '--verbose', action='count', help='increase verbosity')
            p.add_argument('--color', action='store_true', help='use color in output')

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

        # database migration
        p_database_migration = p_database_sub.add_parser('migration', help='migration management')
        p_database_migration_sub = p_database_migration.add_subparsers(dest='subsubcommand')

        # database migration check
        p_database_migration_check = p_database_migration_sub.add_parser('check', help='check migrations')

        # database migration list
        p_database_migration_list = p_database_migration_sub.add_parser('list', help='list migrations')

        # database migration update
        p_database_migration_update = p_database_migration_sub.add_parser('update', help='apply all pending migrations')
        p_database_migration_update.add_argument('--insane', action='store_true', help='skip sanity check')

        # database show
        p_database_show = p_database_sub.add_parser('show', help='show entity details')
        p_database_show.add_argument('--diff', action='store_const', dest='diff', const=True, default=None, help='show successive diffs')
        p_database_show.add_argument('--no-diff', action='store_const', dest='diff', const=False, default=None, help='show file contents')
        p_database_show.add_argument('--full', action='store_true', help='show all context in diff')
        p_database_show.add_argument('--latest', action='store_true', help='only show latest migration')
        p_database_show.add_argument('--pending', action='store_true', help='include pending migrations')
        p_database_show.add_argument('name', nargs='?', help='name of entity')

        # task
        p_task = p_sub.add_parser('task', help='task management')
        p_task_sub = p_task.add_subparsers(dest='subcommand')

        # task add
        p_task_add = p_task_sub.add_parser('add', help='create a new task')
        p_task_add.add_argument('--held', action='store_true', help='create task in "held" state')
        p_task_add.add_argument('--time', metavar='T', type=float, required=True, help='time limit in hours')
        p_task_add.add_argument('--mem', metavar='M', type=float, required=True, help='memory limit in GB')
        p_task_add.add_argument('--priority', metavar='P', type=int, default=0, help='task priority (default: 0)')
        p_task_add.add_argument('--depends-on', metavar='DEP', action='append', help='make DEP a dependency of the new task')
        p_task_add.add_argument('--soft-depends-on', metavar='DEP', action='append', help='make DEP a soft dependency of the new task')
        p_task_add.add_argument('name', help='name for the new task')

        # task cancel
        p_task_cancel = p_task_sub.add_parser('cancel', help='change task state to "cancelled"')
        p_task_cancel.add_argument('--skip-clean', action='store_true', help='skip the cleanup step')
        p_task_cancel.add_argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
        p_task_cancel.add_argument('name', nargs='*', help='task name')

        # task clean
        p_task_clean = p_task_sub.add_parser('clean', help='run cleanup command')
        p_task_clean.add_argument('--allow-unsynthesized', action='store_true', help='clean tasks that are done but not synthesized')
        p_task_clean.add_argument('--ignore-pending-dependents', action='store_true', help='clean tasks that have pending direct dependents')
        p_task_clean.add_argument('--partial', action='store_true', help='only clean data which does not affect dependent tasks')
        p_task_clean.add_argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
        p_task_clean.add_argument('--state', metavar='S', help='only tasks in state S')
        p_task_clean.add_argument('name', nargs='*', help='task name')

        # task clean-all-partial
        p_task_clean_all_partial = p_task_sub.add_parser('clean-all-partial', help='run partial cleanup command for eligible tasks')
        p_task_clean_all_partial.add_argument('--forever', action='store_true', help='wait for more tasks when done')

        # task clean-marked
        p_task_clean_marked = p_task_sub.add_parser('clean-marked', help='run cleanup command for marked tasks')

        # task hold
        p_task_hold = p_task_sub.add_parser('hold', help='change task state to "held"')
        p_task_hold.add_argument('--all', action='store_true', help='try to hold all possible tasks in addition to named tasks')
        p_task_hold.add_argument('name', nargs='*', help='task name')

        # task release
        p_task_release = p_task_sub.add_parser('release', help='change "held" task state to "waiting"')
        p_task_release.add_argument('--all', action='store_true', help='try to release all held tasks in addition to named tasks')
        p_task_release.add_argument('name', nargs='*', help='task name')

        # task reset-claimed
        p_task_reset_claimed = p_task_sub.add_parser('reset-claimed', help='unclaim abandoned tasks')
        p_task_reset_claimed.add_argument('name', nargs='*', help='task name')

        # task list
        p_task_list = p_task_sub.add_parser('list', help='list tasks')
        p_task_list.add_argument('--state', metavar='S', help='only tasks in state S')
        p_task_list.add_argument('--name-pattern', metavar='P', help='only tasks with names matching the SQL LIKE pattern P')

        # task list-claimed
        p_task_list_claimed = p_task_sub.add_parser('list-claimed', help='list claimed tasks')

        # task mark-for-clean
        p_task_mark_for_clean = p_task_sub.add_parser('mark-for-clean', help='mark tasks as requiring cleaning')
        p_task_mark_for_clean.add_argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
        p_task_mark_for_clean.add_argument('--state', metavar='S', help='only tasks in state S')
        p_task_mark_for_clean.add_argument('name', nargs='*', help='task name')

        # task rerun
        p_task_rerun = p_task_sub.add_parser('rerun', help='rerun a task and all its dependents')
        p_task_rerun.add_argument('--allow-no-cleanup', action='store_true', help='proceed even if "task.cleanup_cmd" is not set')
        p_task_rerun.add_argument('--allow-no-unsynthesize', action='store_true', help='proceed even if "task.unsynthesize_cmd" is not set')
        p_task_rerun.add_argument('name', help='name of task')

        # task reset-failed
        p_task_reset_failed = p_task_sub.add_parser('reset-failed', help='reset all failed tasks to "waiting"')
        p_task_reset_failed.add_argument('--skip-clean', action='store_true', help='skip the cleanup step')

        # task show
        p_task_show = p_task_sub.add_parser('show', help='show task details')
        p_task_show.add_argument('name', help='task name')

        # task synthesize
        p_task_synthesize = p_task_sub.add_parser('synthesize', help='synthesize completed tasks')
        p_task_synthesize.add_argument('--forever', action='store_true', help='wait for more tasks when done')

        # task uncancel
        p_task_uncancel = p_task_sub.add_parser('uncancel', help='change "cancelled" task state to "waiting"')
        p_task_uncancel.add_argument('--name-pattern', metavar='P', help='include tasks with names matching the SQL LIKE pattern P')
        p_task_uncancel.add_argument('name', nargs='*', help='task name')

        # task unsynthesize
        p_task_unsynthesize = p_task_sub.add_parser('unsynthesize', help='unsynthesize tasks')
        p_task_unsynthesize.add_argument('name', nargs='*', help='task name')

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
        p_worker_quit.add_argument('--state', metavar='S', help='only workers in state S when using --all')
        p_worker_quit.add_argument('slurm_job_id', nargs='*', type=int, help='Slurm job ID of worker')

        # worker run
        p_worker_run = p_worker_sub.add_parser('run', help='run worker inside Slurm job')

        # worker show
        p_worker_show = p_worker_sub.add_parser('show', help='show worker details')
        p_worker_show.add_argument('slurm_job_id', type=int, help='Slurm job ID of worker')

        # worker spawn
        p_worker_spawn = p_worker_sub.add_parser('spawn', help='spawn Slurm worker job')
        p_worker_spawn.add_argument('--cpus', metavar='C', type=int, required=True, help='number of cpus')
        p_worker_spawn.add_argument('--time', metavar='T', type=float, required=True, help='time limit in hours')
        p_worker_spawn.add_argument('--mem', metavar='M', type=float, required=True, help='amount of memory in GB')
        p_worker_spawn.add_argument('--sbatch-args', metavar='SA', help='additional arguments to pass to sbatch')
        p_worker_spawn.add_argument('--num', type=int, default=1, help='number of workers to spawn (default: 1)')

        return p

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

        mck = McKenzie(conf_path)

        if args.unsafe:
            mck.unsafe = True

        if args.color:
            mck.colorizer.use_colors = True
        else:
            try:
                mck.colorizer.use_colors = bool(os.environ[cls.ENV_COLOR])
            except KeyError:
                pass

        mck.call_manager(args)

    def __init__(self, conf_path):
        self.conf = Conf(conf_path)

        self.colorizer = Colorizer()

        self.unsafe = self.conf.general_unsafe

    def _preflight(self, *, database_init=True, database_update=True):
        if self.unsafe:
            log = logger.warning
        else:
            log = logger.error

        if database_init and not self.conf.db.is_initialized(log=log):
            if not self.unsafe:
                return False

        if database_update and not self.conf.db.is_updated(log=log):
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
