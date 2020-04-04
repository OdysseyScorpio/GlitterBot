from collections import Counter

from lib.database import Database
from lib.gwpcc.consts import KEY_COLONY_FULL_TEXT_INDEX, KEY_COLONY_METADATA, \
    KEY_COLONY_INDEX_BY_ID
from lib.log import Logger


def update():
    db = Database().connection

    Logger().log.debug('Clearing existing colony name indices')

    pipe = db.pipeline()
    for key_to_delete in db.scan_iter(KEY_COLONY_FULL_TEXT_INDEX.format('*'), 10000):
        pipe.delete(key_to_delete)
    pipe.execute()

    Logger().log.debug('Fetching master Colony ID index')

    # Load the colony index
    colony_index = db.lrange(KEY_COLONY_INDEX_BY_ID, 0, -1)

    # For each colony
    pipe = db.pipeline()
    for colony_hash in colony_index:
        pipe.hgetall(KEY_COLONY_METADATA.format(colony_hash))

    colony_results = dict(zip(colony_index, pipe.execute()))

    pipe = db.pipeline()

    data_keys = ['BaseName', 'Planet', 'FactionName']

    Logger().log.debug('Building colony indices')
    # For each thing
    for colony_hash, colony_data in colony_results.items():
        # Now split the new name and update the indices
        for data_key in data_keys:
            try:
                # Count how many times a letter occurs in the word
                scores = Counter(str(colony_data[data_key]).lower())

                for letter, score in scores.items():
                    pipe.zincrby(KEY_COLONY_FULL_TEXT_INDEX.format(letter), score, colony_hash)

            except KeyError as e:
                Logger().log.error('Error processing Colony: {}, Error was {}'.format(colony_hash, e))
                break
    # Execute
    Logger().log.debug('Writing out colony indices to database')
    pipe.execute()
    Logger().log.debug('Finished colony indices')
