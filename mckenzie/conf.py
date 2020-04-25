from configparser import (ConfigParser, NoOptionError, NoSectionError,
                          ParsingError)
import logging
import os
from pathlib import Path
import stat as s

from .database import Database
from .util import HandledException


logger = logging.getLogger(__name__)


class Conf:
    @staticmethod
    def _get(f, section, option, **kwargs):
        try:
            return f(section, option, **kwargs)
        except (NoOptionError, NoSectionError):
            logger.error(f'Entry "{section}.{option}" must be present.')

            raise HandledException()
        except ValueError:
            logger.error(f'Value for "{section}.{option}" is invalid.')

            raise HandledException()

    def getbool(self, *args, **kwargs):
        return self._get(self.parser.getboolean, *args, **kwargs)

    def getint(self, *args, **kwargs):
        return self._get(self.parser.getint, *args, **kwargs)

    def getstr(self, *args, **kwargs):
        return self._get(self.parser.get, *args, **kwargs)

    def __init__(self, conf_path):
        self.conf_path = Path(conf_path).resolve()

        self.parser = ConfigParser()

        # Check permissions.
        try:
            mode = os.stat(self.conf_path).st_mode
        except OSError as e:
            logger.error('Failed to stat configuration file '
                         f'"{self.conf_path}" ({e.strerror}).')

            raise HandledException()

        if mode & (s.S_IRWXG | s.S_IRWXO):
            logger.warning(f'Configuration file "{self.conf_path}" has '
                           'permissions for group or other.')

        # Read configuration.
        try:
            logger.debug(f'Using configuration path "{self.conf_path}".')

            with open(self.conf_path) as f:
                self.parser.read_file(f)
        except OSError as e:
            logger.error('Failed to read configuration from '
                         f'"{self.conf_path}" ({e.strerror}).')

            raise HandledException()
        except ParsingError:
            logger.error('Failed to parse configuration from '
                         f'"{self.conf_path}".')

            raise HandledException()

        # Extract data.
        self.general_work_path = Path(self.getstr('general', 'work_path'))
        self.general_sbatch_args = self.getstr('general', 'sbatch_args',
                                               fallback=None)
        self.general_unsafe = self.getbool('general', 'unsafe',
                                           fallback=False)

        self.db = Database(dbpath=Path(self.getstr('database', 'path')),
                           dbname=self.getstr('database', 'dbname'),
                           dbuser=self.getstr('database', 'user'),
                           dbpassword=self.getstr('database', 'password'),
                           dbport=self.getint('database', 'port'),
                           dbschema=self.getstr('database', 'schema',
                                                fallback=None))
        self.database_mck_cmd = self.getstr('database', 'mck_cmd')
        self.database_mck_args = self.getstr('database', 'mck_args',
                                             fallback=None)
        self.database_job_name = self.getstr('database', 'job_name')
        self.database_sbatch_args = self.getstr('database', 'sbatch_args',
                                                fallback=None)

        self.support_path = Path(self.getstr('support', 'path'))
        self.support_execute_cmd = self.getstr('support', 'execute_cmd')
        self.support_socket_dir_template = self.getstr('support', 'socket_dir_template')
        self.support_job_name = self.getstr('support', 'job_name')
        self.support_sbatch_args = self.getstr('support', 'sbatch_args',
                                               fallback=None)

        self.task_clean_cmd = self.getstr('task', 'clean_cmd')
        self.task_scrub_cmd = self.getstr('task', 'scrub_cmd')
        self.task_synthesize_cmd = self.getstr('task', 'synthesize_cmd')
        self.task_unsynthesize_cmd = self.getstr('task', 'unsynthesize_cmd')

        self.worker_mck_cmd = self.getstr('worker', 'mck_cmd')
        self.worker_mck_args = self.getstr('worker', 'mck_args',
                                           fallback=None)
        self.worker_execute_cmd = self.getstr('worker', 'execute_cmd')
        self.worker_success_string = self.getstr('worker', 'success_string')
        self.worker_job_name = self.getstr('worker', 'job_name')
        self.worker_sbatch_args = self.getstr('worker', 'sbatch_args',
                                              fallback=None)

        job_names = [self.database_job_name, self.support_job_name,
                     self.worker_job_name]

        if sorted(job_names) != sorted(set(job_names)):
            logger.error('Job names (*.job_name) must be unique.')

            raise HandledException()
