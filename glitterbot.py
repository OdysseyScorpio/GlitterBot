from time import sleep

import sentry_sdk

import settings
from core.routines import market_values, stock_management
from core.routines.indices import thing_name_index, colony_name_index, verify_thing_index
from lib.database import Database
from lib.log import Logger
from lib.runtime_config import RuntimeConfig


def main():
    sentry_sdk.init("https://e73e3574f35744afa77647d7170b3c10@sentry.thecodecache.net/5")

    Logger().log.debug('Glitterbot Starting Up...')

    runtime_config = RuntimeConfig()

    # Bootstrap
    for version in settings.API_DB_CONFIG.keys():
        Database().connect_db(version)
        runtime_config.switch_context(version)
        runtime_config.verify_schema()
    runtime_config.dry_run = settings.DEBUG_DRY_RUN

    if settings.DEBUG_FORCE_RUN_NOW:
        Logger().log.debug('Force run flag set')

    while True:

        Logger().log.debug('Glitterbot Sleeping...')
        sleep(5)

        try:

            for version in settings.API_DB_CONFIG.keys():

                # Connect to DB
                Database().connect_db(version)

                # Re-read bot settings from DB
                runtime_config.switch_context(version)

                # Check if it's time to set up a maintenance window.
                runtime_config.update_maintenance_window()

                if not settings.DEBUG_FORCE_RUN_NOW and not runtime_config.should_run():
                    continue

                Logger().log.info('Starting maintenance on {}'.format(version))

                # Enable maintenance mode and set has_run flag to prevent multiple instances.
                runtime_config.enable_maintenance_mode()

                ###
                # Thing Stuff
                ###

                verify_thing_index.check_integrity()

                # Update Prices
                market_values.perform_market_price_analysis()

                # Update GWP Inventory
                stock_management.perform_stock_analysis()

                # Update the indices
                thing_name_index.update()

                ###
                # Colony Stuff
                ###

                # Update the indices
                colony_name_index.update()

                ###
                # Done
                ###

                Logger().log.info('Glitterbot Exiting Maintenance Mode')

                # Exit Maintenance Mode
                runtime_config.exit_maintenance_mode()

                Logger().log.info('Glitterbot Maintenance Done')

            if settings.DEBUG_FORCE_RUN_NOW:
                Logger().log.info('Force run flag was set, exiting.')
                quit()

        except Exception:
            Logger().log.exception('Fatal error in Main Loop')
            quit()


main()
