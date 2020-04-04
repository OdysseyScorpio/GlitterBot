import json
from datetime import datetime, timezone, timedelta

from lib.database import Database
from lib.gwpcc import consts
from lib.gwpcc.consts import KEY_THING_LOCALE_KNOWN_LANGUAGES
from lib.log import Logger


class RuntimeConfig(object):
    instance = None

    def __new__(cls):
        if not RuntimeConfig.instance:
            RuntimeConfig.instance = RuntimeConfig.__RuntimeConfig()
        return RuntimeConfig.instance

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def __setattr__(self, name, value):
        return setattr(self.instance, name, value)

    class __RuntimeConfig:

        def __init__(self):
            self._dry_run = False
            self._config_data = {}
            self._api_config = {}
            self.context = None

        def switch_context(self, api_version):

            self.context = api_version

            if api_version not in self._api_config:
                self.__read_config_from_db()

            self._config_data = self._api_config[api_version]

        def __read_config_from_db(self):
            self._api_config[self.context] = Database().connection.hgetall(consts.KEY_GLITTERBOT_DATA)

            # Parse JSON lists/dicts
            # self._api_config[self.context][consts.KEY_GLITTERBOT_IGNORE_THINGS] = json.loads(
            #   self._api_config[self.context][consts.KEY_GLITTERBOT_IGNORE_THINGS])

        @property
        def dry_run(self) -> bool:
            return self._dry_run

        @dry_run.setter
        def dry_run(self, value):
            self._dry_run = value

        @property
        def ignored_thing_id_list(self) -> list:
            return self._config_data[consts.KEY_GLITTERBOT_IGNORE_THINGS]

        @property
        def sell_price_multiplier(self) -> float:
            return self._config_data[consts.KEY_GLITTERBOT_SELL_PRICE_MULTIPLIER]

        @property
        def buy_price_multiplier(self) -> float:
            return self._config_data[consts.KEY_GLITTERBOT_BUY_PRICE_MULTIPLIER]

        @property
        def price_stock_break_points(self) -> list:
            return self._config_data[consts.KEY_GLITTERBOT_PRICEBREAKS]

        @property
        def minimum_sell_price_multiplier(self) -> float:
            return self._config_data[consts.KEY_GLITTERBOT_MIN_SELL_PRICE_MULTIPLIER]

        @property
        def maintenance_mode(self) -> bool:

            return Database().connection.get(consts.KEY_API_MAINTENANCE_MODE)

        def update_maintenance_window(self):

            # Check if we're in maintenance mode.
            maintenance_already_set = self._config_data[consts.KEY_GLITTERBOT_MTIME_SET]

            # If we are, just return, nothing to do.
            if self.maintenance_mode or maintenance_already_set:
                Logger().log.debug('Already in maintenance mode or maintenance window already set.')
                return

            # Calculate how long until the maintenance window begins.
            current_time = datetime.utcnow()
            delta = int(self._config_data[consts.KEY_GLITTERBOT_MTIME_NEXT] - current_time.timestamp())

            # Is the current time less than X seconds (Preamble) before the maintenance window begins?
            if delta < (self._config_data[consts.KEY_GLITTERBOT_MTIME_PREABMLE]):
                # If we're less than the Preamble time, set the maintenance window in advance.

                Logger().log.info('Creating maintenance window')

                # Create the Start Time datetime object
                maintenance_window_start = datetime(current_time.year,
                                                    current_time.month,
                                                    current_time.day,
                                                    self._config_data[consts.KEY_GLITTERBOT_MTIME_START],
                                                    tzinfo=timezone.utc)

                # Create a timedelta with the length in seconds
                delta = timedelta(seconds=self._config_data[consts.KEY_GLITTERBOT_MTIME_LENGTH])

                # Add the delta to the start time to get the end time
                maintenance_window_end = maintenance_window_start + delta

                # Create a dictionary with start stop times
                window = {'Start': int(maintenance_window_start.timestamp()),
                          'Stop': int(maintenance_window_end.timestamp())}

                # Set the window in the self._db
                Database().connection.hmset(consts.KEY_API_MAINTENANCE_WINDOW, window)

                # Store the maintenance window in GlitterBot's Data so it knows when to run.
                Database().connection.hset(consts.KEY_GLITTERBOT_DATA,
                                           consts.KEY_GLITTERBOT_MTIME_NEXT,
                                           int(maintenance_window_start.timestamp()))

                # Set a flag to say we've already done the maintenance Window
                Database().connection.hset(consts.KEY_GLITTERBOT_DATA,
                                           consts.KEY_GLITTERBOT_MTIME_SET,
                                           "true")

        def should_run(self) -> bool:

            # Are we at the right time?
            current_time = datetime.utcnow().timestamp()
            is_time = (current_time >= self._config_data[consts.KEY_GLITTERBOT_MTIME_NEXT])

            delta = int(self._config_data[consts.KEY_GLITTERBOT_MTIME_NEXT] - current_time)
            next_run_date = datetime.utcfromtimestamp(self._config_data[consts.KEY_GLITTERBOT_MTIME_NEXT])

            Logger().log.debug('Time until next run: {0:0.0f} seconds ({1})'.format(delta, next_run_date))

            if not self.maintenance_mode and is_time:
                return True
            else:
                return False

        def verify_schema(self):
            Logger().log.debug('Checking/updating schema')

            pipe = Database().pipeline

            pipe.set(consts.KEY_CONFIGURATION_PRIME_COST, 300, nx=True)

            pipe.hsetnx(consts.KEY_GLITTERBOT_DATA, consts.KEY_GLITTERBOT_MTIME_START, 23)
            pipe.hsetnx(consts.KEY_GLITTERBOT_DATA, consts.KEY_GLITTERBOT_MTIME_LENGTH, 3600)
            pipe.hsetnx(consts.KEY_GLITTERBOT_DATA, consts.KEY_GLITTERBOT_MTIME_PREABMLE, 7200)
            pipe.hsetnx(consts.KEY_GLITTERBOT_DATA, consts.KEY_GLITTERBOT_MTIME_NEXT, 0)
            pipe.hsetnx(consts.KEY_GLITTERBOT_DATA, consts.KEY_GLITTERBOT_MTIME_SET, "false")
            pipe.hsetnx(consts.KEY_GLITTERBOT_DATA, consts.KEY_GLITTERBOT_BUY_PRICE_MULTIPLIER, "0.20")
            pipe.hset(consts.KEY_GLITTERBOT_DATA,
                      consts.KEY_GLITTERBOT_IGNORE_THINGS, json.dumps(
                    ['8697f432058b914ba2b20c5bd6f0678548126e21', 'cdf9187a28bcb1b219a3a4aeaf3c99a65e7eb882'])
                      )
            pipe.hsetnx(consts.KEY_GLITTERBOT_DATA, consts.KEY_GLITTERBOT_SELL_PRICE_MULTIPLIER, "0.75")
            pipe.hsetnx(consts.KEY_GLITTERBOT_DATA, consts.KEY_GLITTERBOT_MIN_SELL_PRICE_MULTIPLIER, "0.2")
            pipe.sadd(KEY_THING_LOCALE_KNOWN_LANGUAGES, 'english')
            self.__create_price_breaks(pipe)

            pipe.execute()

            # Force re-read of config
            self.__read_config_from_db()

        @staticmethod
        def enable_maintenance_mode():
            Database().connection.set(consts.KEY_API_MAINTENANCE_MODE, "true")

        def exit_maintenance_mode(self):
            pipe = Database().pipeline

            # Set next maintenance time BEFORE we unset the "Has Run" flags to prevent it running again.
            hour_to_run = Database().connection.hget(consts.KEY_GLITTERBOT_DATA, consts.KEY_GLITTERBOT_MTIME_START)

            # Remove the "Has run" and "Maintenance window Set" flags.
            pipe.hset(consts.KEY_GLITTERBOT_DATA, consts.KEY_GLITTERBOT_MTIME_SET, "false")

            pipe.set(consts.KEY_API_MAINTENANCE_MODE, "false")

            current_time = datetime.utcnow()

            current_time_dt = datetime(current_time.year,
                                       current_time.month,
                                       current_time.day,
                                       hour_to_run,
                                       tzinfo=timezone.utc)

            # Create a timedelta to add one day to current day.
            delta = timedelta(days=1)

            next_time_dt = current_time_dt + delta

            pipe.hset(consts.KEY_GLITTERBOT_DATA,
                      consts.KEY_GLITTERBOT_MTIME_NEXT,
                      int(next_time_dt.timestamp())
                      )

            pipe.execute()

            self.__read_config_from_db()

        @staticmethod
        def __create_price_breaks(pipe):
            breakpoints = [
                {'PriceStart': 0, 'PriceStop': 5, 'StockMin': 1000, 'StockMax': 100000, 'CapBuyPrice': 2.5},
                {'PriceStart': 5, 'PriceStop': 25, 'StockMin': 200, 'StockMax': 250, 'CapBuyPrice': 2.75},
                {'PriceStart': 25, 'PriceStop': 50, 'StockMin': 100, 'StockMax': 150, 'CapBuyPrice': 3.0},
                {'PriceStart': 50, 'PriceStop': 100, 'StockMin': 10, 'StockMax': 15, 'CapBuyPrice': 3.5},
                {'PriceStart': 100, 'PriceStop': 100000, 'StockMin': 1, 'StockMax': 15, 'CapBuyPrice': 4.0},
                {'PriceStart': 100000, 'PriceStop': 200000, 'StockMin': 1, 'StockMax': 5, 'CapBuyPrice': 4.5}
            ]

            pipe.hset(consts.KEY_GLITTERBOT_DATA, consts.KEY_GLITTERBOT_PRICEBREAKS, json.dumps(breakpoints))
