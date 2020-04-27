import os
from random import randint

from .util import format_object, print_table


class DatabaseView:
    def __init__(self, db):
        self.db = db


class DatabaseNoteView(DatabaseView):
    def __init__(self, table_name, history_table_name, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.history_table_name = history_table_name

        # Mapping from IDs to description formats.
        self._dict_d = {}
        # Mapping from IDs to argument types.
        self._dict_t = {}

        @self.db.tx
        def notes(tx):
            return tx.execute(f'''
                    SELECT id, description_format, arg_types
                    FROM {table_name}
                    ORDER BY id
                    ''')

        for note_id, description_format, arg_types in notes:
            self._dict_d[note_id] = description_format
            self._dict_t[note_id] = arg_types

    def format(self, history_id, note_id):
        arg_strings = []

        for i, arg_type in enumerate(self._dict_t[note_id]):
            arg_strings.append(f'note_args[{i+1}]::{arg_type}')

        arg_string = ', '.join(arg_strings)

        @self.db.tx
        def args(tx):
            return tx.execute(f'''
                    SELECT {arg_string}
                    FROM {self.history_table_name}
                    WHERE id = %s
                    ''', (history_id,))

        return self._dict_d[note_id].format(*map(format_object, args[0]))


class DatabaseReasonView(DatabaseView):
    def __init__(self, table_name, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Mapping from names to IDs.
        self._dict_r = {}
        # Mapping from IDs to descriptions.
        self._dict_d = {}

        @self.db.tx
        def reasons(tx):
            return tx.execute(f'''
                    SELECT id, name, description
                    FROM {table_name}
                    ORDER BY id
                    ''')

        for reason_id, name, description in reasons:
            self._dict_r[name] = reason_id
            self._dict_d[reason_id] = description

    def rlookup(self, name):
        return self._dict_r[name]

    def dlookup(self, reason_id):
        return self._dict_d[reason_id]


class DatabaseStateView(DatabaseView):
    def __init__(self, table_name, prefix, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.prefix = prefix

        # Mapping from IDs to names.
        self._dict_f = {}
        # Mapping from names to IDs.
        self._dict_r = {}

        @self.db.tx
        def states(tx):
            return tx.execute(f'''
                    SELECT id, name
                    FROM {table_name}
                    ORDER BY id
                    ''')

        for state_id, name in states:
            self._dict_f[state_id] = name
            self._dict_r[name] = state_id

    def lookup(self, state_id, *, user=False):
        name = self._dict_f[state_id]

        if user:
            name = name[len(self.prefix):]

        return name

    def rlookup(self, name, *, user=False):
        if user:
            name = self.prefix + name

        return self._dict_r[name]


class Agent:
    def __init__(self, mck):
        self.mck = mck

        self.conf = mck.conf
        self.db = mck.conf.db


class Instance(Agent):
    pass


class Manager(Agent):
    _registry = {}

    PREFLIGHT_DISABLED = frozenset()

    def __init_subclass__(cls, *, name, **kwargs):
        super().__init_subclass__(**kwargs)

        cls._registry[name] = cls

        cls.name = name

    @classmethod
    def all_managers(cls):
        return cls._registry.values()

    @classmethod
    def get_manager(cls, name):
        return cls._registry[name]

    @classmethod
    def add_cmdline_parser(cls, p_sub):
        p_mgr = p_sub.add_parser(cls.name, help=cls._argparse_desc)
        p_mgr_sub = p_mgr.add_subparsers(dest='subcommand')

        for name, (desc, args) in cls._argparse_subcommands.items():
            p_mgr_cmd = p_mgr_sub.add_parser(name, help=desc)

            for args, kwargs in args:
                p_mgr_cmd.add_argument(*args, **kwargs)

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
