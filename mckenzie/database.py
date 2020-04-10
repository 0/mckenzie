from contextlib import contextmanager
from enum import Enum, IntEnum
import logging
import os
import time

import pkg_resources
import psycopg2
from psycopg2 import errorcodes

from .base import Manager
from .util import HandledException


logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    pass


class RaisedException(DatabaseError):
    def __init__(self, message):
        super().__init__()

        self.message = message


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
        except psycopg2.InternalError as e:
            if e.pgcode == errorcodes.RAISE_EXCEPTION:
                raise RaisedException(e.diag.message_primary)
            else:
                raise
        except psycopg2.IntegrityError as e:
            if e.pgcode == errorcodes.CHECK_VIOLATION:
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


class Database:
    # Current schema version. This number must match the schema_version value
    # in the metadata table, and it must be increased each time the schema is
    # modified.
    SCHEMA_VERSION = 4

    # How many times to retry in case of deadlock.
    NUM_RETRIES = 16

    # 0.1 minutes
    RECONNECT_WAIT_SECONDS = 6

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

    def __init__(self, *, dbname, user, password, host_file_path, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host_file_path = host_file_path
        self.port = port

        self._host = None
        self._conn = None

        self.try_to_reconnect = True

    @property
    def host(self):
        if self._host is None:
            logger.debug('Reading host.')

            with open(self.host_file_path) as f:
                self._host = f.readline().strip()

            logger.debug(f'Host set to "{self._host}".')

        return self._host

    @property
    def conn(self):
        if self._conn is None:
            logger.debug('Connecting to database.')
            self._conn = psycopg2.connect(dbname=self.dbname, user=self.user,
                                          password=self.password,
                                          host=self.host, port=self.port)

        return self._conn

    def close(self):
        try:
            if self._conn is not None:
                self._conn.close()
        except psycopg2.InterfaceError:
            pass

        self._conn = None
        self._host = None

    def tx(self, f):
        logger.debug('Starting transaction.')

        retries_left = self.NUM_RETRIES

        while True:
            try:
                with self.conn:
                    with self.conn.cursor() as curs:
                        tx = Transaction(curs)

                        return f(tx)
            except Exception as e:
                if retries_left <= 0:
                    logger.warning('Out of retries!')

                    raise

                if isinstance(e, psycopg2.extensions.TransactionRollbackError):
                    # Simply retry.
                    pass
                elif isinstance(e, (psycopg2.InterfaceError,
                                    psycopg2.OperationalError)):
                    if not self.try_to_reconnect:
                        raise

                    # Reconnect before retrying.
                    logger.debug('Forcing disconnect.')
                    self.close()
                    logger.debug('Taking a break.')
                    time.sleep(self.RECONNECT_WAIT_SECONDS)
                else:
                    raise

            logger.debug('Retrying transaction.')
            retries_left -= 1

    @contextmanager
    def without_reconnect(self):
        value = self.try_to_reconnect
        self.try_to_reconnect = False

        yield

        self.try_to_reconnect = value

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
            with self.without_reconnect():
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
            log(f'Schema version "{db_version}" does not match '
                f'"{self.SCHEMA_VERSION}".')

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
                    logger.error('Schema already loaded, but version is '
                                 'missing.')
                elif db_version != Database.SCHEMA_VERSION:
                    logger.error('Schema already loaded, but version '
                                 f'"{db_version}" does not match '
                                 f'"{Database.SCHEMA_VERSION}".')
                else:
                    logger.info('Schema already loaded, and up to date.')

                raise HandledException()

            paths = []

            for typ in ['trigger', 'table', 'function']:
                for path in self._get_schema_files(typ):
                    logger.debug(f'Executing "{path}".')

                    try:
                        with open(path) as f:
                            tx.execute(f.read())
                    except Exception as e:
                        logger.error(path)

                        if isinstance(e, RaisedException):
                            print(f'Exception was raised: {e.message}')
                        elif isinstance(e, CheckViolation):
                            print('Constraint was violated: '
                                  f'{e.constraint_name}')
                        elif isinstance(e, psycopg2.ProgrammingError):
                            print(f'Programming error: {" ".join(e.args)}')
                        else:
                            raise

                        raise HandledException()

                    paths.append(path)

            return paths

        for path in paths:
            logger.info(path)

        logger.info('Schema loaded successfully.')
