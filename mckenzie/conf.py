from configparser import (ConfigParser, NoOptionError, NoSectionError,
                          ParsingError)
import logging
from pathlib import Path

from .database import Database
from .util import HandledException


logger = logging.getLogger(__name__)


class Conf:
    @staticmethod
    def _get(f, section, option):
        try:
            return f(section, option)
        except (NoOptionError, NoSectionError):
            logger.error(f'Entry "{section}.{option}" must be present.')

            raise HandledException()
        except ValueError:
            logger.error(f'Value for "{section}.{option}" is invalid.')

            raise HandledException()

    def getint(self, *args, **kwargs):
        return self._get(self.parser.getint, *args, **kwargs)

    def getstr(self, *args, **kwargs):
        return self._get(self.parser.get, *args, **kwargs)

    def __init__(self, conf_path):
        self.conf_path = Path(conf_path).resolve()

        self.parser = ConfigParser()

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

        self.db = Database(self.getstr('database', 'dbname'),
                           self.getstr('database', 'user'),
                           self.getstr('database', 'password'),
                           self.getstr('database', 'host'),
                           self.getint('database', 'port'))
