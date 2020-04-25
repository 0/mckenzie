from contextlib import contextmanager
from datetime import datetime, timedelta
from enum import Enum, IntEnum
import logging
from math import ceil
import os
from pathlib import Path
import shlex
import subprocess
import time

import pkg_resources
import psycopg2
from psycopg2 import errorcodes

from . import slurm
from .arguments import argparsable, argument, description
from .base import Manager
from .util import HandledException, check_proc, flock, humanize_datetime


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

    def __init__(self, *, dbpath, dbname, dbuser, dbpassword, dbport,
                 dbschema):
        if dbschema is not None:
            schema_valid = True

            for i, c in enumerate(dbschema):
                if 'A' <= c <= 'Z':
                    continue
                elif 'a' <= c <= 'z':
                    continue
                elif i >= 1 and '0' <= c <= '9':
                    continue
                elif i >= 1 and c == '_':
                    continue

                schema_valid = False

                break

            if not (dbschema and schema_valid):
                logger.error('Schema name must match '
                             '"^[A-Za-z][A-Za-z0-9_]*$".')

                raise HandledException()

        self.dbpath = dbpath
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbpassword = dbpassword
        self.dbport = dbport
        self.dbschema = dbschema

        self._dbhost = None
        self._conn = None

        self.try_to_reconnect = True

    @property
    def dbhost(self):
        if self._dbhost is None:
            logger.debug('Reading host.')

            try:
                with open(self.dbpath / 'host') as f:
                    self._dbhost = f.readline().strip()
            except FileNotFoundError:
                logger.error('Database host file not found.')

                raise HandledException()

            logger.debug(f'Host set to "{self._dbhost}".')

        return self._dbhost

    @property
    def conn(self):
        if self._conn is None:
            logger.debug('Connecting to database.')
            kwargs = {'dbname': self.dbname, 'user': self.dbuser,
                      'password': self.dbpassword, 'host': self.dbhost,
                      'port': self.dbport}

            if self.dbschema is not None:
                kwargs['options'] = f'-c search_path={self.dbschema}'

            self._conn = psycopg2.connect(**kwargs)

        return self._conn

    def close(self):
        try:
            if self._conn is not None:
                self._conn.close()
        except psycopg2.InterfaceError:
            pass

        self._conn = None
        self._dbhost = None

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

        try:
            yield
        finally:
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
            log(f'Database "{self.dbname}" at {self.dbhost}:{self.dbport} has '
                'not been initialized.')

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


@argparsable('database management')
class DatabaseManager(Manager, name='database'):
    PREFLIGHT_DISABLED = frozenset({'database_init', 'database_current'})

    # Path to database output files, relative to database directory.
    DATABASE_OUTPUT_DIR = Path('database_output')
    # Database output file name template.
    DATABASE_OUTPUT_FILE_TEMPLATE = 'database-{}.out'

    # 5 minutes
    END_SIGNAL_SECONDS = 300
    # 0.05 minutes
    CANCEL_ACTIVE_DATABASE_SECONDS = 3

    @staticmethod
    def _get_schema_files(typ):
        d = pkg_resources.resource_filename(__name__, f'schema/{typ}')

        return sorted(os.path.join(d, name) for name in os.listdir(d))

    def _database_output_file(self):
        # Replacement symbol for sbatch.
        slurm_job_id = '%j'
        path = (self.DATABASE_OUTPUT_DIR
                    / self.DATABASE_OUTPUT_FILE_TEMPLATE.format(slurm_job_id))

        return path

    def summary(self, args):
        if not self.db.is_initialized():
            return

        if not self.db.is_current():
            return

        logger.info(f'Database "{self.db.dbname}" at '
                    f'{self.db.dbhost}:{self.db.dbport} is OK.')

    @description('back up database')
    def backup(self, args):
        if not self.db.is_initialized(log=logger.error):
            return

        timestamp = datetime.now().isoformat(timespec='seconds')
        output_path = self.db.dbpath / f'backup_{timestamp}'

        proc_args = ['pg_basebackup']
        proc_args.append('--pgdata=' + str(output_path))
        proc_args.append('--checkpoint=fast')
        proc_args.append('--wal-method=stream')
        proc_args.append('--format=tar')
        proc_args.append('--gzip')
        proc_args.append('--compress=9')
        proc_args.append('--no-sync')
        proc_args.append('--progress')
        proc_args.append('--verbose')
        proc_args.append('--no-password')
        proc_args.append('--host=' + self.db.dbhost)
        proc_args.append('--port=' + str(self.db.dbport))
        proc_args.append('--username=' + self.db.dbuser)

        proc_env = {'PGPASSWORD': self.db.dbpassword}

        logger.debug(f'Starting backup to {output_path}.')

        proc = subprocess.run(proc_args, env=proc_env)

        if not check_proc(proc, log=logger.error):
            return

        logger.debug('Backup completed.')

    @description('connect to database')
    def client(self, args):
        if not self.db.is_initialized(log=logger.error):
            return

        proc_args = ['psql']
        proc_args.append('--dbname=' + str(self.db.dbname))
        proc_args.append('--no-password')
        proc_args.append('--host=' + self.db.dbhost)
        proc_args.append('--port=' + str(self.db.dbport))
        proc_args.append('--username=' + self.db.dbuser)

        proc_env = {'PGPASSWORD': self.db.dbpassword}

        if self.db.dbschema is not None:
            proc_env['PGOPTIONS'] = f'-c search_path={self.db.dbschema}'

        logger.debug('Starting database client.')

        os.execvpe('psql', proc_args, {**os.environ, **proc_env})

    @description('list database jobs')
    def list(self, args):
        columns = ['%A', '%t', '%R', '%P', '%C', '%l', '%m', '%S', '%e']
        jobs = slurm.list_all_jobs(self.conf.database_job_name, columns,
                                   log=logger.error)

        raw_time_starts = []
        database_data = []
        # Only output the warning once.
        mem_format_warn = False

        for (jobid, state, reason, partition, cpus, time_total, mem,
                time_start, time_end) in jobs:
            raw_time_starts.append(time_start)
            now = datetime.now()

            cpus = int(cpus)
            time_total = slurm.parse_timedelta(time_total)

            if mem[-1] == 'M' and mem[:-1].isdigit():
                mem = ceil(int(mem[:-1]) / 1024)
            else:
                if not mem_format_warn:
                    mem_format_warn = True
                    logger.warning('Invalid memory format.')

            try:
                dt = datetime.fromisoformat(time_start)
            except ValueError:
                pass
            else:
                time_start = humanize_datetime(dt, now)

            try:
                dt = datetime.fromisoformat(time_end)
            except ValueError:
                pass
            else:
                if state in ['PD', 'R']:
                    signal_offset = timedelta(seconds=self.END_SIGNAL_SECONDS)
                    time_end = humanize_datetime(dt - signal_offset, now)
                else:
                    time_end = humanize_datetime(dt, now)

            database_data.append([jobid, state, reason, partition, cpus,
                                  time_total, mem, time_start, time_end])

        # Sort by start time.
        sorted_data = [row for (s, row) in sorted(zip(raw_time_starts,
                                                      database_data))]
        self.print_table(['Job ID', ('State', 2), 'Partition', 'Cores', 'Time',
                          'Mem (GB)', 'Start', 'End'],
                         sorted_data)

    @description('list completed database jobs')
    @argument('--last-hr', metavar='T', type=float, required=True, help='jobs completed in the last T hours')
    def list_completed(self, args):
        last = timedelta(hours=args.last_hr)

        columns = ['JobID', 'AllocCPUS', 'TotalCPU', 'CPUTime', 'MaxRSS',
                   'ReqMem', 'MaxDiskRead', 'MaxDiskWrite', 'End']
        jobs = slurm.list_completed_jobs(self.conf.database_job_name, columns,
                                         last, log=logger.error)

        database_data = []
        # Only output the warning once.
        format_warn = False

        for (jobid, cpus, cpu_used, cpu_total, mem_used, mem_total, disk_read,
                disk_write, time_end) in jobs:
            now = datetime.now()

            if not jobid.endswith('.batch'):
                continue

            jobid = jobid[:-6]

            cpu_used = slurm.parse_timedelta(cpu_used)
            cpu_total = slurm.parse_timedelta(cpu_total)

            cpus = int(cpus)

            if mem_used[-1] == 'K' and mem_used[:-1].isdigit():
                mem_used = ceil(int(mem_used[:-1]) / 1024 / 1024)
            else:
                if not format_warn:
                    format_warn = True
                    logger.warning('Invalid format.')

            if mem_total[-2:] == 'Mn' and mem_total[:-2].isdigit():
                mem_total = ceil(int(mem_total[:-2]) / 1024)
            else:
                if not format_warn:
                    format_warn = True
                    logger.warning('Invalid format.')

            if disk_read[-1] == 'M':
                try:
                    disk_read = ceil(float(disk_read[:-1]) / 1024)
                except ValueError:
                    if not format_warn:
                        format_warn = True
                        logger.warning('Invalid format.')
            else:
                if not format_warn:
                    format_warn = True
                    logger.warning('Invalid format.')

            if disk_write[-1] == 'M':
                try:
                    disk_write = ceil(float(disk_write[:-1]) / 1024)
                except ValueError:
                    if not format_warn:
                        format_warn = True
                        logger.warning('Invalid format.')
            else:
                if not format_warn:
                    format_warn = True
                    logger.warning('Invalid format.')

            try:
                dt = datetime.fromisoformat(time_end)
            except ValueError:
                pass
            else:
                time_end = humanize_datetime(dt, now)

            database_data.append([jobid, cpus, cpu_used, cpu_total, mem_used,
                                  mem_total, disk_read, disk_write, time_end])

        self.print_table(['Job ID', 'Cores', ('CPU (U/T)', 2),
                          ('Mem (GB;U/T)', 2), ('Disk (GB;R/W)', 2), 'End'],
                         database_data)

    @description('load schema')
    def load_schema(self, args):
        if not self.db.is_initialized(log=logger.error):
            return

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

            if self.db.dbschema is not None:
                tx.execute(f'CREATE SCHEMA IF NOT EXISTS {self.db.dbschema}')

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

    @description('signal database jobs to quit')
    @argument('--current', action='store_true', help='signal currently active database')
    @argument('--all', action='store_true', help='signal all database jobs')
    @argument('slurm_job_id', nargs='*', type=int, help='Slurm job ID of database')
    def quit(self, args):
        current = args.current
        all_databases = args.all
        slurm_job_ids = set(args.slurm_job_id)

        if current:
            logger.debug('Waiting for database lock.')

            with flock(self.db.dbpath / 'lock'):
                try:
                    with open(self.db.dbpath / 'jobid') as f:
                        slurm_job_ids.add(int(f.readline()))
                except FileNotFoundError:
                    logger.warning('No currently active database found.')

        if all_databases:
            ids = slurm.get_all_job_ids(self.conf.database_job_name,
                                        log=logger.error)
            slurm_job_ids.update(ids)

        for slurm_job_id in slurm_job_ids:
            if self.mck.interrupted:
                break

            logger.debug(f'Attempting to cancel Slurm job {slurm_job_id}.')
            cancel_success, signalled_running = \
                    slurm.cancel_job(slurm_job_id,
                                     name=self.conf.database_job_name,
                                     signal='INT', log=logger.error)

            if cancel_success:
                logger.info(slurm_job_id)

    @description('run database')
    def run(self, args):
        slurm_job_id = slurm.get_job_id(log=logger.error)
        database_node, *_ = slurm.get_job_variables()

        logger.debug('Waiting for database lock.')

        with flock(self.db.dbpath / 'lock'):
            try:
                with open(self.db.dbpath / 'jobid') as f:
                    current_job_id = int(f.readline())
            except FileNotFoundError:
                logger.debug('No currently active database found.')
                current_job_id = None

            while current_job_id is not None:
                logger.debug('Checking for currently active database.')

                if not slurm.does_job_exist(current_job_id, log=logger.error):
                    logger.debug('Currently active database is not running.')
                    current_job_id = None

                    break

                logger.debug('Attempting to cancel Slurm job '
                             f'{current_job_id}.')
                slurm.cancel_job(current_job_id,
                                 name=self.conf.database_job_name,
                                 signal='INT', log=logger.error)

                logger.debug('Taking a break.')
                time.sleep(self.CANCEL_ACTIVE_DATABASE_SECONDS)

            logger.debug('Recording new information.')

            with open(self.db.dbpath / 'jobid', 'w') as f:
                f.write(str(slurm_job_id))

            with open(self.db.dbpath / 'host', 'w') as f:
                f.write(database_node)

        logger.info(f'Starting database in job {slurm_job_id} on '
                    f'{database_node}.')

        os.execlp('postgres', 'postgres', '-D',
                  self.db.dbpath / 'pgdata')

    @description('spawn Slurm database job')
    @argument('--cpus', metavar='C', type=int, required=True, help='number of CPUs')
    @argument('--time-hr', metavar='T', type=float, required=True, help='time limit in hours')
    @argument('--mem-gb', metavar='M', type=float, required=True, help='amount of memory in GB')
    @argument('--sbatch-args', metavar='SA', help='additional arguments to pass to sbatch')
    def spawn(self, args):
        database_cpus = args.cpus
        database_time_hours = args.time_hr
        database_mem_gb = args.mem_gb
        sbatch_args = args.sbatch_args

        mck_cmd = shlex.quote(self.conf.database_mck_cmd)

        if self.conf.database_mck_args is not None:
            mck_args = self.conf.database_mck_args
        else:
            mck_args = ''

        script = f'''
                #!/bin/bash

                export PYTHONUNBUFFERED=1
                exec {mck_cmd} {mck_args} database run
                '''

        submitter = slurm.JobSubmitter(name=self.conf.database_job_name,
                                       signal='INT',
                                       signal_seconds=self.END_SIGNAL_SECONDS,
                                       chdir_path=self.db.dbpath,
                                       output_file_path=self._database_output_file(),
                                       cpus=database_cpus,
                                       time_hours=database_time_hours,
                                       mem_gb=database_mem_gb,
                                       sbatch_argss=[self.conf.general_sbatch_args,
                                                     self.conf.database_sbatch_args,
                                                     sbatch_args],
                                       script=script)

        logger.debug('Spawning database job.')
        slurm_job_id = submitter.submit(log=logger.error)
        logger.info(slurm_job_id)
