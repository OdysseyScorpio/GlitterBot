from collections import Counter

import settings
from lib.database import Database
from lib.gwpcc.consts import KEY_COLONY_INDEX_BY_ID, KEY_COLONY_METADATA, KEY_COLONY_FULL_TEXT_INDEX

for version in settings.API_DB_CONFIG.keys():
    Database().connect_db(version)
    db = Database().connection

    pipe = db.pipeline()
    for key_to_delete in db.scan_iter(KEY_COLONY_FULL_TEXT_INDEX.format('*'), 10000):
        pipe.delete(key_to_delete)
    pipe.execute()

    # Load the colony index
    colony_index = db.lrange(KEY_COLONY_INDEX_BY_ID, 0, -1)

    # For each colony
    pipe = db.pipeline()
    for colony_hash in colony_index:
        pipe.hgetall(KEY_COLONY_METADATA.format(colony_hash))

    colony_results = dict(zip(colony_index, pipe.execute()))

    pipe = db.pipeline()

    data_keys = ['BaseName', 'Planet', 'FactionName']

    # For each thing
    for colony_hash, colony_data in colony_results.items():
        # Now split the new name and update the indices
        for data_key in data_keys:
            try:

                scores = Counter(str(colony_data[data_key]))

                for letter, score in scores.items():
                    pipe.zincrby(KEY_COLONY_FULL_TEXT_INDEX.format(letter), score, colony_hash)

            except KeyError as e:
                print('Error processing Colony: {}, Error was {}'.format(colony_hash, e))
                break
    # Execute
    pipe.execute()
