from enum import IntEnum
import os
from random import randint

from .util import format_object, print_table, print_time_series


class DatabaseEnum(IntEnum):
    @classmethod
    def validate(cls, table_name, tx):
        db_values = tx.execute(f'''
                SELECT id, name
                FROM {table_name}
                ''')

        lookup = {}

        for db_id, db_name in db_values:
            lookup[db_name] = db_id

            try:
                expected_name = cls(db_id).name
            except ValueError:
                print(f'Enum {cls.__name__} is missing {db_id}, expected '
                      f'"{db_name}".')
            else:
                if expected_name != db_name:
                    print(f'Table {table_name} {db_id} is "{db_name}", but '
                          f'expected "{expected_name}".')

        for item in cls:
            try:
                expected_value = lookup[item.name]
            except KeyError:
                print(f'Table {table_name} is missing "{item.name}", expected '
                      f'{item.value}.')
            else:
                if item.value != expected_value:
                    print(f'Enum {cls.__name__} "{item.name}" is '
                          f'{item.value}, but expected {expected_value}')


def preflight(**kwargs):
    def wrapped(f):
        f._preflight_kwargs = kwargs

        return f

    return wrapped


class Agent:
    def __init__(self, mck):
        self.mck = mck

        self.conf = mck.conf
        self.db = mck.conf.db


class Instance(Agent):
    pass


class Manager(Agent):
    _registry = {}

    def __init_subclass__(cls, *, name, **kwargs):
        super().__init_subclass__(**kwargs)

        cls._registry[name] = cls

        cls.name = name

    @classmethod
    def all_managers(cls):
        return [x[1] for x in sorted(cls._registry.items())]

    @classmethod
    def get_manager(cls, name):
        return cls._registry[name]

    @classmethod
    def add_cmdline_parser(cls, p_sub, *, level):
        p_mgr = p_sub.add_parser(cls.name.rsplit('.', maxsplit=1)[-1],
                                 help=cls._argparse_desc)
        p_mgr_sub = p_mgr.add_subparsers(dest='sub' * level + 'command')

        for name, (desc, args) in cls._argparse_subcommands.items():
            p_mgr_cmd = p_mgr_sub.add_parser(name, help=desc)

            for args, kwargs in args:
                p_mgr_cmd.add_argument(*args, **kwargs)

        return p_mgr_sub

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.c = self.mck.colorizer

        pid = os.getpid()
        rand = randint(0, 0xff)
        # 00000001 RRRRRRRR PPPPPPPP PPPPPPPP
        ident = (0x01 << 24) | (rand << 16) | (pid & 0xffff)
        self.db.set_session_parameter('mck.ident', ident)

    def print_table(self, *args, **kwargs):
        print_table(*args, reset_str=self.c('reset'), **kwargs)

    def print_time_series(self, *args, **kwargs):
        print_time_series(*args, reset_str=self.c('reset'), **kwargs)
