import os
from random import randint


class DatabaseView:
    def __init__(self, db):
        self.db = db

    def lookup(self, state_id):
        raise NotImplementedError()

    def rlookup(self, name):
        raise NotImplementedError()


class DatabaseReasonView(DatabaseView):
    def __init__(self, table_name, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Mapping from names to IDs.
        self._dict_r = {}

        @self.db.tx
        def reasons(tx):
            return tx.execute(f'''
                    SELECT id, name
                    FROM {table_name}
                    ORDER BY id
                    ''')

        for reason_id, name in reasons:
            self._dict_r[name] = reason_id

    def rlookup(self, name):
        return self._dict_r[name]


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

    @property
    def ident(self):
        return self._ident


class Instance(Agent):
    pass


class Manager(Agent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        pid = os.getpid()
        rand = randint(0, 0xff)
        # 00000001 RRRRRRRR PPPPPPPP PPPPPPPP
        self._ident = (0x01 << 24) | (rand << 16) | (pid & 0xffff)
