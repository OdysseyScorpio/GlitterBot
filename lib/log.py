import logging
import logging.handlers
import os
import sys

import settings


class Logger(object):
    instance = None

    def __new__(cls):
        if not Logger.instance:
            Logger.instance = Logger.__Logger()
        return Logger.instance

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def __setattr__(self, name, **kwargs):
        return setattr(self.instance, name, kwargs)

    class __Logger:
        def __init__(self):
            if os.name == 'nt':
                log_path = './output.log'
            else:
                log_path = '/var/log/glitterbot/output'

            # Set up a specific logger with our desired output level

            self._log_handle = logging.getLogger('GlitterBot')
            self._log_handle.setLevel(settings.LOG_LEVEL)

            # Add the log message handler to the logger
            handler = logging.handlers.TimedRotatingFileHandler(log_path,
                                                                when='h',
                                                                interval=1,
                                                                backupCount=48,
                                                                encoding='utf8',
                                                                delay=False,
                                                                utc=True)

            self._log_handle.addHandler(handler)

            handler = logging.StreamHandler(sys.stdout)

            self._log_handle.addHandler(handler)

        @property
        def log(self):
            return self._log_handle
