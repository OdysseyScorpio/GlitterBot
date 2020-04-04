from typing import Dict

from lib.database import Database
from lib.gwpcc import consts
from lib.log import Logger


def check_integrity():
    connection = Database().connection

    thing_index = connection.smembers(consts.KEY_THING_INDEX)

    Logger().log.info('Checking Thing:Index integrity')

    pipe = connection.pipeline()
    for key in thing_index:
        pipe.exists(consts.KEY_THING_META.format(key))
    results = pipe.execute()

    things: Dict[str, int] = dict(zip(thing_index, results))

    missing_keys = []
    for key, exists in things.items():
        if not exists:
            missing_keys.append(key)
            Logger().log.warning('Removing key {} as there is no matching metadata'.format(key))

    if missing_keys:
        pipe = connection.pipeline()
        for key in missing_keys:
            pipe.srem(consts.KEY_THING_INDEX, key)
        pipe.execute()
