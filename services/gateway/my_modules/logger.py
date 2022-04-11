import copy
import logging


class LoggerInitError(Exception): pass


class Logger:
    def __init__(self, name, log_file_name='log.txt', level: int = logging.INFO, formatter: logging.Formatter = None,
                 parent=None, print_start_message=False, encoding='utf-8'):
        if parent:
            self._name = f'{parent.name}->{self._check_name(name)}'
            self._log_file_name = parent.log_file_name
            self._level = parent.level
            self._formatter = parent.formatter
            self._encoding = parent.encoding
        else:
            self._name = self._check_name(name)
            self._log_file_name = log_file_name
            self._level = self._check_level(level)
            self._formatter = self._check_formatter(formatter)
            self._encoding = encoding

        self._logger = logging.getLogger(self._name)
        self._logger.setLevel(self._level)

        fh = logging.FileHandler(self._log_file_name, encoding=self.encoding)
        fh.setFormatter(self._formatter)

        self._logger.addHandler(fh)

        if print_start_message:
            self.info(f'{"="*20} Logger {self._name} initialized {"="*20}')

    @property
    def name(self):
        return self._name

    @property
    def log_file_name(self):
        return self._log_file_name

    @property
    def formatter(self):
        return self._formatter

    @property
    def level(self):
        return self._level

    @property
    def encoding(self):
        return self._encoding

    def _check_name(self, name):
        if isinstance(name, str):
            return name
        else:
            raise LoggerInitError('Wrong logger name')

    def _check_level(self, level):
        if level in {0, 10, 20, 30, 40, 50}:
            return level
        else:
            raise LoggerInitError('Wrong logging level')

    def _check_formatter(self, formatter):
        if isinstance(formatter, logging.Formatter):
            return formatter
        else:
            return logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    def info(self, message, *args):
        self._logger.info(message, *args)

    def child(self, name):
        return Logger(name, parent=self)
