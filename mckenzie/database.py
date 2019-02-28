import hashlib
import logging
import os

import pkg_resources
import psycopg2

from .base import Manager
from .util import print_table


logger = logging.getLogger(__name__)


class Transaction:
    def __init__(self, curs):
        self.curs = curs

    def execute(self, *args, **kwargs):
        logger.debug('Executing query.')
        self.curs.execute(*args, **kwargs)

        try:
            return self.curs.fetchall()
        except psycopg2.ProgrammingError as e:
            if e.args != ('no results to fetch',):
                raise

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


class Database:
    # How many times to retry in case of deadlock.
    NUM_RETRIES = 16

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

    def is_initialized(self, *, log=logger.info):
        def F(tx):
            return tx.execute('SELECT 1')

        try:
            success = self.tx(F) == [(1,)]
        except psycopg2.OperationalError:
            success = False

        if not success:
            log(f'Database "{self.dbname}" at {self.host}:{self.port} has not '
                'been initialized.')

        return success

    def is_updated(self, *, log=logger.info):
        success_pending = True
        success_extra = True

        # Migrations.
        def F(tx):
            return DatabaseMigrationManager.get_applied_migrations(tx)

        applied_migrations = self.tx(F)

        all_migrations = DatabaseMigrationManager.get_all_migrations()
        pending_migrations = all_migrations.keys() - applied_migrations.keys()
        extra_migrations = applied_migrations.keys() - all_migrations.keys()

        if len(pending_migrations):
            if len(pending_migrations) == 1:
                log('There is 1 pending migration.')
            else:
                log(f'There are {len(pending_migrations)} pending migrations.')

            success_pending = False

        if len(extra_migrations):
            if len(extra_migrations) == 1:
                log('There is 1 extra migration.')
            else:
                log(f'There are {len(extra_migrations)} extra migrations.')

            success_extra = False

        # Report overall state.
        if not success_extra:
            log(f'Database "{self.dbname}" at {self.host}:{self.port} is not '
                'in a good place.')
        elif not success_pending:
            log(f'Database "{self.dbname}" at {self.host}:{self.port} is not '
                'up to date.')

        return success_pending and success_extra


class DatabaseManager(Manager):
    def summary(self, args):
        if not self.db.is_initialized():
            return

        if not self.db.is_updated():
            return

        logger.info(f'Database "{self.db.dbname}" at '
                    f'{self.db.host}:{self.db.port} is OK.')


class DatabaseMigrationManager(Manager):
    @staticmethod
    def get_all_migrations():
        migration_dir = pkg_resources.resource_filename(__name__, 'migrations')
        all_migrations = {}

        for pre_name in os.listdir(migration_dir):
            # Migration file names must start with a date.
            if not ('0' <= pre_name[0] <= '9'):
                continue

            # Migration file names must include a type flag.
            if pre_name[13] not in ['F', 'T']:
                continue

            # Remove file extension.
            name = pre_name.rsplit('.', maxsplit=1)[0]
            path = os.path.join(migration_dir, pre_name)
            all_migrations[name] = path

        return all_migrations

    @staticmethod
    def get_applied_migrations(tx, *, lock=False):
        migrations = []

        tx.savepoint('get_applied_migrations')

        try:
            if lock:
                tx.execute('''
                        LOCK TABLE database_migration
                        ''')

            migrations.extend(tx.execute('''
                        SELECT name, applied_at
                        FROM database_migration
                        '''))
        except psycopg2.ProgrammingError as e:
            e_msg = 'relation "database_migration" does not exist'

            if len(e.args) < 1 or not e.args[0].startswith(e_msg):
                raise

            tx.rollback('get_applied_migrations')
        finally:
            tx.release('get_applied_migrations')

        applied_migrations = {}

        for name, applied_at in migrations:
            applied_migrations[name] = applied_at

        return applied_migrations

    def summary(self, args):
        def F(tx):
            return self.get_applied_migrations(tx)

        applied_migrations = self.db.tx(F)

        all_migrations = self.get_all_migrations()

        migration_data = [
            ['applied', 0],
            ['pending (!)', 0],
            ['extra (!)', 0],
        ]

        for name in all_migrations:
            if name in applied_migrations:
                migration_data[0][1] += 1
            else:
                migration_data[1][1] += 1

        for name in applied_migrations:
            if name not in all_migrations:
                migration_data[2][1] += 1

        # Don't display empty states.
        for idx in reversed(range(len(migration_data))):
            if migration_data[idx][1] == 0:
                migration_data.pop(idx)

        print_table(['State', 'Count'], migration_data, total=('Total', (1,)))

    def list(self, args):
        def F(tx):
            return self.get_applied_migrations(tx)

        applied_migrations = self.db.tx(F)

        migration_data = []

        for name in sorted(self.get_all_migrations()):
            try:
                applied_at = applied_migrations[name]
            except KeyError:
                applied_at = ''

            migration_data.append([name, applied_at])

        print_table(['Name', 'Applied at'], migration_data)

    def update(self, args):
        all_migrations = self.get_all_migrations()

        def F(tx):
            applied_migrations = self.get_applied_migrations(tx, lock=True)

            new_migrations = []

            for name, path in sorted(all_migrations.items()):
                if name in applied_migrations:
                    continue

                logger.debug(f'Executing "{name}" at "{path}".')

                with open(path) as f:
                    migration_file_data = f.read()

                migration_encoded = migration_file_data.encode('utf-8')
                sha256_hash = hashlib.sha256(migration_encoded)
                sha256_digest = sha256_hash.hexdigest()

                tx.execute(migration_file_data)

                tx.execute('''
                        INSERT INTO database_migration (name, sha256_digest)
                        VALUES (%s, %s)
                        ''', (name, sha256_digest))

                new_migrations.append(name)

            return new_migrations

        for name in self.db.tx(F):
            logger.info(name)
