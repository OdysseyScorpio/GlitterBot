import csv
import os
import random
import time
from datetime import datetime, timedelta

from lib.database import Database
from lib.gwpcc import consts
from lib.gwpcc.date_utils import get_today_date_string
from lib.gwpcc.enums import TradeDirection
from lib.gwpcc.qevent import event
from lib.gwpcc.qevent.messages.sale import SaleMessage
from lib.gwpcc.things.thing import Thing
from lib.log import Logger
from lib.runtime_config import RuntimeConfig


def perform_market_price_analysis():
    connection = Database().connection
    pipe = connection.pipeline()

    all_thing_hashes = Database().connection.smembers(consts.KEY_THING_INDEX)

    all_things = Thing.get_many_from_database_by_hash(all_thing_hashes, connection, get_today_date_string())

    # Remove excluded items.
    for hash_key in RuntimeConfig().ignored_thing_id_list:
        if hash_key in all_things:
            del all_things[hash_key]

    __trim_prices(all_things)

    __update_initial_prices(all_things)

    __update_item_buy_prices(all_things)

    __update_item_sell_prices(all_things)

    __do_sale(all_things)

    __write_data_to_CSV(all_things)

    # Commit the data to the DB.
    for thing in all_things.values():
        thing.save_to_database(pipe)

    if not RuntimeConfig().dry_run:
        pipe.execute()


def __write_data_to_CSV(all_things):
    headers = ['thing_hash',
               'bmv',
               'buy_price_new',
               'buy_price_old',
               'buy_price_delta',
               'sell_price_new',
               'sell_price_old',
               'sell_price_delta',
               'qty_sold',
               'qty_bought']

    if os.name == 'nt':
        csv_path = './'
    else:
        csv_path = '/var/log/glitterbot/'

    csv_file = csv_path + '{}-{}-market-data.csv'.format(datetime.now().date().isoformat(), Database().market)

    f = open(csv_file, "w", newline='')
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()

    for thing in all_things.values():
        if thing.UseServerPrice:
            row = {'thing_hash': thing.Hash,
                   'bmv': thing.BaseMarketValue,
                   'buy_price_delta': 0,
                   'sell_price_delta': 0,
                   'qty_sold': thing.TradeHistory[TradeDirection.ToPlayer],
                   'qty_bought': thing.TradeHistory[TradeDirection.ToGWP]}

            if thing.has_changed('CurrentBuyPrice'):
                row['buy_price_old'] = round(thing.get_change('CurrentBuyPrice'), 2)
                row['buy_price_delta'] = round(thing.CurrentBuyPrice - thing.get_change('CurrentBuyPrice'), 2)
            else:
                row['buy_price_old'] = round(thing.CurrentBuyPrice, 2)
            row['buy_price_new'] = round(thing.CurrentBuyPrice, 2)

            if thing.has_changed('CurrentSellPrice'):
                row['sell_price_old'] = round(thing.get_change('CurrentSellPrice'), 2)
                row['sell_price_delta'] = round(thing.CurrentSellPrice - thing.get_change('CurrentSellPrice'), 2)
            else:
                row['sell_price_old'] = round(thing.CurrentSellPrice, 2)
            row['sell_price_new'] = round(thing.CurrentSellPrice, 2)

            writer.writerow(row)

    f.close()


def __trim_prices(all_things):
    pipe = Database().pipeline

    for thing_hash, thing in all_things.items():

        # If we have more than 5 different prices, remove the ones with low votes (<=2).
        # This may well end up being all of them.
        if len(thing.BMVVotes) >= 5:
            pipe.zremrangebyscore(consts.KEY_THING_BASE_MARKET_VALUE_DATA.format(thing_hash), 0, 2)


def __update_initial_prices(all_things):
    for thing in all_things.values():

        # Check if we can enable server side pricing.
        if not thing.UseServerPrice:
            __calculate_initial_price(thing)


def __update_item_buy_prices(all_things):
    price_breaks = RuntimeConfig().price_stock_break_points

    # Get max price point
    max_break = max(price_breaks, key=lambda x: x['PriceStop'])

    for thing in all_things.values():

        # If we can, calculate new price based on trade activity.
        if thing.UseServerPrice:

            # Look up the price point that matches the BMV
            price_point = next(
                (pp for pp in price_breaks if pp['PriceStart'] <= thing.BaseMarketValue <= pp['PriceStop']), None)

            # If we didn't find one, the just skip it
            if price_point is None:
                # Default to 100 items per unit if we don't know how to classify the price.
                Logger().log.info(
                    'Defaulting to max break for {0}, Outside of known price ranges.'.format(thing))
                price_point = max_break

            __adjust_buy_price_based_on_trade_activity(thing, price_point)

            if thing.has_changed('CurrentBuyPrice'):
                Logger().log.info('{0} Final result: Adjusting buy price of {1} from {2} to {3}'.format(
                    '\u25B2' if thing.CurrentBuyPrice > thing.get_change('CurrentBuyPrice') else '\u25BC',
                    thing,
                    thing.get_change('CurrentBuyPrice'),
                    thing.CurrentBuyPrice
                ))


def __update_item_sell_prices(all_things):
    price_breaks = RuntimeConfig().price_stock_break_points

    # Get max price point
    max_break = max(price_breaks, key=lambda x: x['PriceStop'])

    for thing in all_things.values():
        if thing.UseServerPrice:

            # Look up the price point that matches the BMV
            price_point = next(
                (pp for pp in price_breaks if pp['PriceStart'] <= thing.BaseMarketValue <= pp['PriceStop']), None)

            # If we didn't find one, the just skip it
            if price_point is None:
                # Default to 100 items per unit if we don't know how to classify the price.
                Logger().log.info(
                    'Defaulting to max break to maximum for {0}, Outside of known price ranges.'.format(thing))
                price_point = max_break

            __adjust_sell_price_based_on_stock_quantity(thing, price_point)

            if thing.has_changed('CurrentSellPrice'):
                Logger().log.info('{0} Final result: Adjusting sell price of {1} from {2} to {3}'.format(
                    '\u25B2' if thing.CurrentSellPrice > thing.get_change('CurrentSellPrice') else '\u25BC',
                    thing,
                    thing.get_change(
                        'CurrentSellPrice'),
                    thing.CurrentSellPrice
                ))


def __adjust_buy_price_based_on_trade_activity(thing: Thing, price_point):
    quantity_sold = thing.TradeHistory[TradeDirection.ToPlayer]

    # Copy starting price.
    original_price = thing.CurrentBuyPrice

    # We add 1% of Base Market Value for every 1 unit sold.
    one_percent_base_market_value = (thing.BaseMarketValue / 100)

    total_stock = thing.Quantity + quantity_sold  # 1% of Stock should cause a price increase.

    # Calculate the price cap
    max_price = thing.BaseMarketValue * price_point['CapBuyPrice']

    # Fix overpriced items
    if original_price > max_price:
        Logger().log.info(
            'Max buy price reached for {}, Currently: {}, Resetting to: {}'.format(thing, original_price, max_price))
        thing.CurrentBuyPrice = max_price
        original_price = max_price

    if total_stock <= 0:
        # Skip DIV/0 if nothing sold and there's no stock.
        units_sold = 0
    else:
        # Calculate how many units we sold
        units_sold = quantity_sold / total_stock

    # Less than 10% sold?
    if units_sold <= 0.1:

        # Take off 10%
        calculated_price = original_price - (one_percent_base_market_value * 10)

        # Reduce the price but never lower than base market price.
        if calculated_price < thing.BaseMarketValue:
            Logger().log.info('Lowest buy price reached for {}, clamped to {}'.format(thing, thing.BaseMarketValue))
            calculated_price = thing.BaseMarketValue

    else:

        increase = max(10, (units_sold - 1.10) * 100)

        # Multiple the 1% price by amount of units sold and add that to current price, Hard cap at 10 units Max.
        calculated_price = original_price + (one_percent_base_market_value * increase)

        if calculated_price > max_price:
            Logger().log.info(
                'Max buy price reached for {}, clamped to {}'.format(thing, max_price))

        # Use the smaller of the two, but no smaller than 1 silver
        calculated_price = round(max(1, min(calculated_price, max_price)), 2)

    if original_price != calculated_price:
        Logger().log.info('Changing buy price {0} from {1:.2f} to {2:.2f} due to {3:.2f} units sold'.format(
            thing,
            original_price,
            calculated_price,
            units_sold)
        )

        thing.CurrentBuyPrice = round(calculated_price, 2)


def __adjust_sell_price_based_on_stock_quantity(thing: Thing, price_point):
    # We decrease the Sell Value for every 1% over the wanted quantity in stock .
    one_percent_base_market_value = thing.BaseMarketValue * 0.01

    # Define maximum reduced price for item.
    min_value = thing.BaseMarketValue * RuntimeConfig().minimum_sell_price_multiplier

    # Copy starting price.
    original_price = thing.CurrentSellPrice

    # Calculate how many percent of max stock we are
    units_in_stock = round(thing.Quantity / price_point['StockMax'])

    # Less than 50% of StockMax?
    if units_in_stock <= 0.50:

        Logger().log.info('Low stock of {}, Increasing sell price'.format(thing))

        # Raise the sell price 10% to encourage selling to GWP
        calculated_price = original_price + (one_percent_base_market_value * 10)

        # Never more than 150% market value
        max_sell_price = 1.5 * thing.BaseMarketValue

        # Check max sell price.
        if calculated_price > max_sell_price:
            Logger().log.info('Max sell price reached for {}, clamped to {}.'.format(
                thing,
                max_sell_price)
            )
            calculated_price = max_sell_price

    elif units_in_stock > 1:

        # Multiply the 1% price by amount of units over the amount of stock and add that to current price,
        # Hard cap at 10 percent reduction max.

        reduction = min(10, (units_in_stock - 1) * 100)

        calculated_price = original_price - (one_percent_base_market_value * reduction)

        # Clamp value to a maximum 80% reduction in price.
        if calculated_price < min_value:
            Logger().log.info('Minimum sell price reached for {}, clamped to {}'.format(thing, min_value))
            calculated_price = min_value

    else:
        # Make sure we always check current selling price.
        calculated_price = thing.CurrentSellPrice

    # Check current buy price.
    if calculated_price > thing.CurrentBuyPrice:
        Logger().log.info(
            'Sell price can not be higher than buy price for {}, currently {}, clamped to {}.'.format(
                thing,
                thing.CurrentSellPrice,
                thing.CurrentBuyPrice)
        )
        calculated_price = thing.CurrentBuyPrice

    if original_price != calculated_price:
        Logger().log.info('Changing sell price for {0} from {1} to {2}'.format
                          (thing,
                           original_price,
                           calculated_price)
                          )
        thing.CurrentSellPrice = round(calculated_price, 2)


def __calculate_initial_price(thing: Thing):
    # Do we have at least one price?
    if len(thing.BMVVotes) < 1:
        Logger().log.debug('No price samples for {}'.format(thing))
        return

    # Find the two top ranked prices.
    common = thing.BMVVotes.most_common(2)

    # Did we get two or more?
    if len(common) >= 2:
        # Do the top two have the same number of votes?
        if common[0][1] == common[1][1]:
            # Stalemate
            Logger().log.debug('Unanimous vote required for {}'.format(thing))
            return

    if common[0][1] < 10:
        # Not confident enough
        Logger().log.debug('Not enough votes ({}) to set {} for {}'.format(common[0][1], common[0][0], thing))
        return

    thing.UseServerPrice = True

    # Set BaseMarketValue to most common price
    thing.BaseMarketValue = round(common[0][0], 2)

    Logger().log.debug('New BaseMarketValue ({}) for {}'.format(common[0][0], thing))

    # Add GWP Markup from BaseMarketValue
    markup_price = round(thing.BaseMarketValue + (thing.BaseMarketValue * RuntimeConfig().buy_price_multiplier), 2)

    markup_current_sell_price = round(markup_price * RuntimeConfig().sell_price_multiplier, 2)

    # Set the values.
    thing.CurrentBuyPrice = round(markup_price, 2)
    thing.CurrentSellPrice = round(markup_current_sell_price, 2)

    Logger().log.debug('Setting initial Buy Price ({}) for {}'.format(thing.CurrentBuyPrice, thing))
    Logger().log.debug('Setting initial Sell Price ({}) for {}'.format(thing.CurrentSellPrice, thing))


def __do_sale(all_things):
    db = Database().connection

    current_date = datetime.fromtimestamp(time.time())
    current_epoch = int(current_date.timestamp())

    # Delay items from going on sale again for 30 days.
    delay_epoch = int((current_date + timedelta(days=30)).timestamp())

    # This is a list of ThingID
    current_things_on_sale = db.smembers(consts.KEY_GLITTERBOT_SALE_DATA.format('ThingsOnSaleNow'))

    # Clear out items delayed from being on sale again.
    db.zremrangebyscore(consts.KEY_GLITTERBOT_SALE_DATA.format('ThingsOnSaleBefore'), "-inf", current_epoch)

    pipe = Database().pipeline

    # Add the existing sales items to the list of previously on sale with their expiry date.
    for thing_hash in current_things_on_sale:
        # Remember to zero out the price override.
        thing = all_things[thing_hash]
        thing.BuyPriceOverride = 0
        thing.SellPriceOverride = 0
        pipe.zadd(consts.KEY_GLITTERBOT_SALE_DATA.format('ThingsOnSaleBefore'), {thing_hash: delay_epoch})

    # Execute
    pipe.execute()

    # Fetch the updated list of excluded items.
    excluded_items = set(db.zrangebyscore(consts.KEY_GLITTERBOT_SALE_DATA.format('ThingsOnSaleBefore'),
                                          current_epoch,
                                          "+inf"))

    pipe = Database().pipeline

    # Delete the list of items currently on sale.
    pipe.delete(consts.KEY_GLITTERBOT_SALE_DATA.format('ThingsOnSaleNow'))

    # Only things that have NOT been on sale and have server-side pricing
    eligible_things = list(
        set([thing.Hash for thing in all_things.values() if
             thing.UseServerPrice and thing.Quantity > 0]) - excluded_items)

    random.shuffle(eligible_things)

    # Create a discount range from 5% to 75% in steps of 5%
    discount_range = range(5, 75, 5)

    # Get 30 items to put on Sale
    random_choices = eligible_things[:30]

    sale_things = []
    for thing_hash in random_choices:
        discount = random.choice(discount_range)

        thing = all_things[thing_hash]

        sale_things.append({'thing': thing, 'discount': discount})

        thing.BuyPriceOverride = round(thing.CurrentBuyPrice - ((thing.CurrentBuyPrice / 100) * discount), 2)
        thing.SellPriceOverride = round(thing.CurrentSellPrice - ((thing.CurrentSellPrice / 100) * discount), 2)

        Logger().log.debug("Thing {}, Discounted by {}%".format(thing, discount))

    msg = SaleMessage.prepare(Database().market, sale_things)

    pipe.sadd(consts.KEY_GLITTERBOT_SALE_DATA.format('ThingsOnSaleNow'), *random_choices)
    event.send(msg, pipe)

    pipe.execute()
