import argparse
from collections import defaultdict
import logging
import os

from .conf import Conf
from .database import DatabaseManager, DatabaseMigrationManager
from .task import TaskManager

# For export.
from .util import HandledException


logger = logging.getLogger(__name__)


class McKenzie:
    ENV_CONF = 'MCKENZIE_CONF'

    MANAGERS = {
        'database': DatabaseManager,
        'database.migration': DatabaseMigrationManager,
        'task': TaskManager,
    }

    PREFLIGHT_ARGS = defaultdict(dict, {
        'database': {
            'database_init': False,
            'database_update': False,
        },
        'database.migration': {
            'database_update': False,
        },
    })

    @staticmethod
    def _cmdline_parser():
        p = argparse.ArgumentParser(prog='mck')
        p.add_argument('--conf', help='path to configuration file')
        p.add_argument('-q', '--quiet', action='count', help='decrease verbosity')
        p.add_argument('-v', '--verbose', action='count', help='increase verbosity')
        p_sub = p.add_subparsers(dest='command')

        # database
        p_database = p_sub.add_parser('database', help='database management')
        p_database_sub = p_database.add_subparsers(dest='subcommand')

        # database migration
        p_database_migration = p_database_sub.add_parser('migration', help='migration management')
        p_database_migration_sub = p_database_migration.add_subparsers(dest='subsubcommand')

        # database migration list
        p_database_migration_list = p_database_migration_sub.add_parser('list', help='list migrations')

        # database migration update
        p_database_migration_update = p_database_migration_sub.add_parser('update', help='apply all pending migrations')

        # task
        p_task = p_sub.add_parser('task', help='task management')
        p_task_sub = p_task.add_subparsers(dest='subcommand')

        # task add
        p_task_add = p_task_sub.add_parser('add', help='create a new task')
        p_task_add.add_argument('--time', metavar='T', type=float, required=True, help='time limit in hours')
        p_task_add.add_argument('--mem', metavar='M', type=float, required=True, help='memory limit in GB')
        p_task_add.add_argument('--priority', metavar='P', type=int, default=0, help='task priority (default: 0)')
        p_task_add.add_argument('name', help='name for the new task')

        # task list
        p_task_list = p_task_sub.add_parser('list', help='list tasks')
        p_task_list.add_argument('--state', metavar='S', help='only tasks in state S')
        p_task_list.add_argument('--name-pattern', metavar='P', help='only tasks with names matching the SQL LIKE pattern P')

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

        mck = McKenzie(conf_path)
        mck.call_manager(args)

    def __init__(self, conf_path):
        self.conf = Conf(conf_path)

    def _preflight(self, *, database_init=True, database_update=True):
        if database_init and not self.conf.db.is_initialized(log=logger.error):
            return False

        if database_update and not self.conf.db.is_updated(log=logger.error):
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

        if not self._preflight(**self.PREFLIGHT_ARGS[mgr_name]):
            return

        mgr = self.MANAGERS[mgr_name](self)
        getattr(mgr, subcmd)(args)
