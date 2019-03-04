from collections import defaultdict
import hashlib
import logging
import os

import pkg_resources
import psycopg2

from .base import Manager
from .util import print_table


logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    pass


class CheckViolation(DatabaseError):
    def __init__(self, constraint_name):
        super().__init__()

        self.constraint_name = constraint_name


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

    def is_updated(self, *, log=logger.info):
        success_pending = True
        success_extra = True

        # Migrations.
        @self.tx
        def applied_migrations(tx):
            return DatabaseMigrationManager.get_applied_migrations(tx)

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
    ENTITY_TYPES = {
        'F': 'function',
        'T': 'table',
    }

    def _entities(self):
        all_migrations = DatabaseMigrationManager.get_all_migrations()

        @self.db.tx
        def applied_migrations(tx):
            return DatabaseMigrationManager.get_applied_migrations(tx)

        entity_types = {}
        entity_paths = defaultdict(list)

        for pre_name, path in sorted(all_migrations.items()):
            if pre_name not in applied_migrations:
                continue

            # Remove file extension.
            name = pre_name.rsplit('.', maxsplit=1)[0]
            # Remove prefix.
            entity_name = name[15:]

            entity_types[entity_name] = self.ENTITY_TYPES[name[13]]
            entity_paths[entity_name].append(path)

        return entity_types, entity_paths

    @staticmethod
    def _is_trigger(name):
        return name.startswith('aftins_') or name.startswith('aftupd_')

    def summary(self, args):
        if not self.db.is_initialized():
            return

        if not self.db.is_updated():
            return

        logger.info(f'Database "{self.db.dbname}" at '
                    f'{self.db.host}:{self.db.port} is OK.')

    def show(self, args):
        target = args.name

        entity_types, entity_paths = self._entities()

        if target is None:
            es = sorted(entity_types)
            es = sorted(es,
                        key=lambda x: x[7:] if self._is_trigger(x) else x)

            entity_data = []

            for entity_name in es:
                entity_data.append([entity_name, entity_types[entity_name],
                                    len(entity_paths[entity_name])])

            print_table(['Name', 'Type', 'Migrations'], entity_data)

            return

        if target not in entity_types:
            logger.info(f'No entity named "{target}".')

            return

        for path in entity_paths[target]:
            print('==>', path, '<==')

            with open(path) as f:
                print(f.read())

            print()


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

    @staticmethod
    def _sanity_check(tx, log=logger.error):
        # Check for claimed tasks.
        tx.savepoint('sanity_check_task')

        try:
            tx.execute('''
                    LOCK TABLE task
                    ''')

            tasks = tx.execute('''
                    SELECT COUNT(*)
                    FROM task
                    WHERE claimed_by IS NOT NULL
                    ''')
        except psycopg2.ProgrammingError as e:
            e_msgs = (
                'relation "task" does not exist',
                'column "claimed_by" does not exist',
            )

            if len(e.args) < 1 or not e.args[0].startswith(e_msgs):
                raise

            tx.rollback('sanity_check_task')
        else:
            num_claimed = tasks[0][0]

            if num_claimed != 0:
                log('Claimed tasks present.')

                return False
        finally:
            tx.release('sanity_check_task')

        # Check for workers with jobs.
        tx.savepoint('sanity_check_worker')

        try:
            tx.execute('''
                    LOCK TABLE worker
                    ''')

            workers = tx.execute('''
                    SELECT COUNT(*)
                    FROM worker w
                    JOIN worker_state ws ON ws.id = w.state_id
                    WHERE ws.job_exists
                    ''')
        except psycopg2.ProgrammingError as e:
            e_msgs = (
                'relation "worker" does not exist',
                'relation "worker_state" does not exist',
            )

            if len(e.args) < 1 or not e.args[0].startswith(e_msgs):
                raise

            tx.rollback('sanity_check_worker')
        else:
            num_with_job = workers[0][0]

            if num_with_job != 0:
                log('Workers with jobs present.')

                return False
        finally:
            tx.release('sanity_check_worker')

        return True

    def summary(self, args):
        @self.db.tx
        def applied_migrations(tx):
            return self.get_applied_migrations(tx)

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
        @self.db.tx
        def applied_migrations(tx):
            return self.get_applied_migrations(tx)

        migration_data = []

        for name in sorted(self.get_all_migrations()):
            try:
                applied_at = applied_migrations[name]
            except KeyError:
                applied_at = ''

            migration_data.append([name, applied_at])

        print_table(['Name', 'Applied at'], migration_data)

    def update(self, args):
        insane = args.insane

        if not insane:
            sanity_log = logger.error
        else:
            sanity_log = logger.warning

        all_migrations = self.get_all_migrations()

        @self.db.tx
        def new_migrations(tx):
            if not self._sanity_check(tx, log=sanity_log):
                if not insane:
                    tx.rollback()

                    return []

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

                if path.endswith('.py'):
                    from .task import TaskReason, TaskState

                    g = {
                        'tx': tx,
                        'tr': TaskReason(self.db),
                        'ts': TaskState(self.db),
                    }

                    exec(migration_file_data, g)
                elif path.endswith('.sql'):
                    tx.execute(migration_file_data)

                tx.execute('''
                        INSERT INTO database_migration (name, sha256_digest)
                        VALUES (%s, %s)
                        ''', (name, sha256_digest))

                new_migrations.append(name)

            return new_migrations

        for name in new_migrations:
            logger.info(name)
