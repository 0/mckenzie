from contextlib import contextmanager
from enum import Enum, IntEnum
import logging
import os

import pkg_resources
import psycopg2

from .base import Manager
from .util import HandledException


logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    pass


class CheckViolation(DatabaseError):
    def __init__(self, constraint_name):
        super().__init__()

        self.constraint_name = constraint_name


class AdvisoryKey(IntEnum):
    """
    Advisory lock key values.
    """

    # Accessing the task_dependency table. Shared lock for reading, exclusive
    # lock for writing. Second key must be the ID of the dependency (not the
    # dependent task), modulo 2^31. Should be acquired in decreasing order to
    # reduce the number of deadlocks.
    TASK_DEPENDENCY_ACCESS = 1001


class Transaction:
    def __init__(self, curs):
        self.curs = curs

    def _execute(self, f, *args, **kwargs):
        logger.debug('Executing query.')

        try:
            f(*args, **kwargs)
        except psycopg2.IntegrityError as e:
            if e.pgcode == '23514':
                raise CheckViolation(e.diag.constraint_name)
            else:
                raise

        try:
            return self.curs.fetchall()
        except psycopg2.ProgrammingError as e:
            if e.args != ('no results to fetch',):
                raise

    def callproc(self, *args, **kwargs):
        return self._execute(self.curs.callproc, *args, **kwargs)

    def execute(self, *args, **kwargs):
        return self._execute(self.curs.execute, *args, **kwargs)

    def savepoint(self, name):
        self.execute(f'SAVEPOINT {name}')

    def release(self, name):
        self.execute(f'RELEASE SAVEPOINT {name}')

    def rollback(self, name=None):
        logger.debug('Rolling back transaction.')

        if name is not None:
            self.execute(f'ROLLBACK TO SAVEPOINT {name}')
        else:
            self.curs.connection.rollback()

    def _advisory_do(self, action, key, key2=None, *, xact=False, shared=False):
        name_pieces = ['pg', 'advisory']

        if xact:
            name_pieces.append('xact')

        name_pieces.append('{}')

        if shared:
            name_pieces.append('shared')

        name_template = '_'.join(name_pieces)

        if isinstance(key, Enum):
            key = key.value

        if key2 is None:
            keyss = [(key,)]
        elif isinstance(key2, list):
            keyss = [(key, k) for k in key2]
        else:
            keyss = [(key, key2)]

        for keys in keyss:
            self.callproc(name_template.format(action), keys)

    def advisory_lock(self, *args, **kwargs):
        self._advisory_do('lock', *args, **kwargs)

    def advisory_unlock(self, *args, **kwargs):
        self._advisory_do('unlock', *args, **kwargs)

    @contextmanager
    def advisory(self, *args, **kwargs):
        self.advisory_lock(*args, **kwargs)

        try:
            yield
        finally:
            self.advisory_unlock(*args, **kwargs)


class Database:
    # Current schema version. This number must match the schema_version value
    # in the metadata table, and it must be increased each time the schema is
    # modified.
    SCHEMA_VERSION = 1

    # How many times to retry in case of deadlock.
    NUM_RETRIES = 16

    @staticmethod
    def schema_version(tx):
        tx.savepoint('metadata_schema_version')

        try:
            db_version = tx.execute('''
                    SELECT value
                    FROM metadata
                    WHERE key = 'schema_version'
                    ''')
        except psycopg2.ProgrammingError as e:
            e_msg = 'relation "metadata" does not exist'

            if len(e.args) < 1 or not e.args[0].startswith(e_msg):
                raise

            tx.rollback('metadata_schema_version')

            return None
        finally:
            tx.release('metadata_schema_version')

        if not db_version:
            return False

        return int(db_version[0][0])

    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

        self._conn = None

    @property
    def conn(self):
        if self._conn is None:
            logger.debug('Connecting to database.')
            self._conn = psycopg2.connect(dbname=self.dbname, user=self.user,
                                          password=self.password,
                                          host=self.host, port=self.port)

        return self._conn

    def tx(self, f):
        logger.debug('Starting transaction.')

        with self.conn:
            with self.conn.cursor() as curs:
                tx = Transaction(curs)
                retries_left = self.NUM_RETRIES

                while True:
                    try:
                        return f(tx)
                    except psycopg2.extensions.TransactionRollbackError:
                        if retries_left <= 0:
                            logger.warning('Out of retries!')

                            raise

                        logger.debug('Retrying transaction.')
                        tx.rollback()
                        retries_left -= 1

    @contextmanager
    def advisory(self, *args, **kwargs):
        @self.tx
        def F(tx):
            tx.advisory_lock(*args, **kwargs)

        try:
            yield
        finally:
            @self.tx
            def F(tx):
                tx.advisory_unlock(*args, **kwargs)

    def is_initialized(self, *, log=logger.info):
        try:
            @self.tx
            def result(tx):
                return tx.execute('SELECT 1')
        except psycopg2.OperationalError:
            success = False
        else:
            success = result == [(1,)]

        if not success:
            log(f'Database "{self.dbname}" at {self.host}:{self.port} has not '
                'been initialized.')

        return success

    def is_current(self, *, log=logger.info):
        db_version = self.tx(self.schema_version)

        if db_version is None:
            log('Schema not loaded.')

            return False

        if db_version == False:
            log('Schema version missing.')

            return False

        if db_version != self.SCHEMA_VERSION:
            log(f'Schema version "{db_version}" does not match "{self.SCHEMA_VERSION}".')

            return False

        return True


class DatabaseManager(Manager):
    def summary(self, args):
        if not self.db.is_initialized():
            return

        if not self.db.is_current():
            return

        logger.info(f'Database "{self.db.dbname}" at '
                    f'{self.db.host}:{self.db.port} is OK.')


class DatabaseSchemaManager(Manager):
    @staticmethod
    def _get_schema_files(typ):
        d = pkg_resources.resource_filename(__name__, f'schema/{typ}')

        return sorted(os.path.join(d, name) for name in os.listdir(d))

    def summary(self, args):
        logger.info('No action specified.')

    def load(self, args):
        @self.db.tx
        def paths(tx):
            db_version = self.db.schema_version(tx)

            if db_version is not None:
                if db_version == False:
                    logger.error('Schema already loaded, but version is missing.')
                elif db_version != Database.SCHEMA_VERSION:
                    logger.error(f'Schema already loaded, but version "{db_version}" does not match "{Database.SCHEMA_VERSION}".')
                else:
                    logger.error('Schema already loaded, and up to date.')

                raise HandledException()

            paths = []

            for path in (self._get_schema_files('trigger') +
                         self._get_schema_files('table') +
                         self._get_schema_files('function')):
                logger.debug(f'Executing "{path}".')

                with open(path) as f:
                    schema_file_data = f.read()

                tx.execute(schema_file_data)
                paths.append(path)

            return paths

        for path in paths:
            logger.info(path)
