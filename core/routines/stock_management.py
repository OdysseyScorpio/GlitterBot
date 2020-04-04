import random

from lib.database import Database
from lib.gwpcc import consts
from lib.gwpcc.date_utils import get_today_date_string
from lib.gwpcc.things.thing import Thing
from lib.log import Logger
from lib.runtime_config import RuntimeConfig


def perform_stock_analysis():
    connection = Database().connection
    pipe = connection.pipeline()

    all_thing_hashes = Database().connection.smembers(consts.KEY_THING_INDEX)

    all_things = Thing.get_many_from_database_by_hash(all_thing_hashes, connection, get_today_date_string())

    # Remove excluded items.
    for hash_key in RuntimeConfig().ignored_thing_id_list:
        if hash_key in all_things:
            del all_things[hash_key]

    for thing in all_things.values():
        __ensure_minimum_stock_level(thing)
        __trim_stock_level(thing)
        thing.save_to_database(pipe)

    if not RuntimeConfig().dry_run:
        pipe.execute()


def __ensure_minimum_stock_level(thing: Thing):
    price_breaks = RuntimeConfig().price_stock_break_points
    bracket = next((pp for pp in price_breaks if pp['PriceStart'] <= thing.BaseMarketValue <= pp['PriceStop']),
                   None)

    if bracket is None:
        Logger().log.debug(
            'Unable to set minimum stock amount for Thing {0}, No price bracket matched.'.format(thing))
        return

    original_qty = thing.Quantity

    if thing.Quantity < 0:
        thing.Quantity = 0

    if thing.Quantity < bracket['StockMin']:
        upper_bound = int(bracket['StockMax'] - ((bracket['StockMax'] - bracket['StockMin']) / 2))
        amount = random.randint(bracket['StockMin'], upper_bound)
        thing.Quantity = amount

    if original_qty != thing.Quantity:
        Logger().log.debug('Setting stock of {0} to {1}'.format(thing, thing.Quantity))

    return


def __trim_stock_level(thing: Thing):
    price_breaks = RuntimeConfig().price_stock_break_points
    bracket = next((pp for pp in price_breaks if pp['PriceStart'] <= thing.BaseMarketValue <= pp['PriceStop']),
                   None)

    if bracket is None:
        Logger().log.debug(
            'Unable to get max stock amount for Thing {0}, No price bracket matched.'.format(thing))
        return

    original_qty = thing.Quantity

    if thing.Quantity > bracket['StockMax']:
        upper_bound = int(thing.Quantity * 0.25)
        lower_bound = int(thing.Quantity * 0.05)
        amount = random.randint(lower_bound, upper_bound)
        thing.Quantity = thing.Quantity - amount

    if original_qty != thing.Quantity:
        Logger().log.debug(
            'Reducing stock of {} from {} to {}, overstocked: {}'.format(thing, original_qty, thing.Quantity,
                                                                         bracket['StockMax']))

    return
