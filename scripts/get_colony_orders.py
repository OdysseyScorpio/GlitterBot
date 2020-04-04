import json
from datetime import datetime
from timeit import default_timer as timer
from typing import Dict

from lib.database import Database
from lib.gwpcc.consts import KEY_COLONY_METADATA, KEY_COLONY_ALL_ORDERS, KEY_ORDER_MANIFEST, \
    KEY_COLONY_FTMP_INDEX, KEY_COLONY_FULL_TEXT_INDEX, KEY_COLONY_INDEX_BY_STEAM_ID

db_name = input('Which DB: ').lower()

print("*********** Searching {} ***********".format(db_name))
Database().connect_db(db_name)
db = Database().connection


def search_by_id():
    while True:

        name = input('Enter Steam ID: ')

        if name:

            start = timer()
            results = db.lrange(KEY_COLONY_INDEX_BY_STEAM_ID.format(name), 0, -1)
            end = timer()
            print('Query took {0:.4f}'.format(end - start))

            if results:
                pipe = db.pipeline()
                list(map(lambda colony_hash: pipe.hgetall(KEY_COLONY_METADATA.format(colony_hash)), results))
                colony_results = dict(zip(results, pipe.execute()))
                print('Matches: ')
                start = timer()
                for colony_hash, colony_data in colony_results.items():
                    print('Hash: {} Colony Name: {}, Faction: {}, Planet: {}, Created: {}'.format(
                        colony_hash,
                        colony_data['BaseName'],
                        colony_data['FactionName'],
                        colony_data['Planet'],
                        colony_data['DateCreated']
                    ))
                end = timer()
                print('Filter took {0:.4f}'.format(end - start))
                lookup = input('Choose Hash or press enter to refine: ')
                if lookup:
                    break
            else:
                print('Nothing found, try again')
    return lookup


def search_by_name():
    while True:

        name = input('Enter search terms:')

        if name:

            temp_key = KEY_COLONY_FTMP_INDEX.format(datetime.utcnow().timestamp())
            pipe = db.pipeline()
            sets = list(map(lambda key: KEY_COLONY_FULL_TEXT_INDEX.format(key), [c for c in name if c.isalnum()]))
            pipe.zunionstore(temp_key, sets)
            pipe.zrevrangebyscore(temp_key, '+inf', 0, withscores=True)
            pipe.delete(temp_key)
            start = timer()
            results = pipe.execute()
            end = timer()
            print('Query took {0:.4f}'.format(end - start))

            if results and results[1]:
                pipe = db.pipeline()
                colony_keys = [colony_hash[0] for colony_hash in results[1]]
                list(map(lambda colony_hash: pipe.hgetall(KEY_COLONY_METADATA.format(colony_hash)), colony_keys))
                colony_results = dict(zip(colony_keys, pipe.execute()))

                print('Matches: ')
                start = timer()
                for colony_hash, colony_data in colony_results.items():
                    if any(str(colony_data[key]).find(name) > -1 for key in data_keys):
                        print(
                            'Hash: {} Colony Name: {}, Faction: {}, Planet: {}, Created: {}, Owner Type {}, Owner ID {}'.format(
                                colony_hash,
                                colony_data['BaseName'],
                                colony_data['FactionName'],
                                colony_data['Planet'],
                                colony_data['DateCreated'],
                                colony_data['OwnerType'],
                                colony_data['OwnerID']
                            ))
                end = timer()
                print('Filter took {0:.4f}'.format(end - start))
                lookup = input('Choose Hash or press enter to refine: ')
                if lookup:
                    break
            else:
                print('Nothing found, try again')

    return lookup


data_keys = ['BaseName', 'Planet', 'FactionName']

t = input('Steam ID or Search?').lower().strip()

if t == 'steam':
    lookup = search_by_id()
else:
    lookup = search_by_name()

output = open('output.json', 'w')

meta_keys = Database().connection.lrange(KEY_COLONY_ALL_ORDERS.format(lookup), 0, -1)

pipe = Database().connection.pipeline()
for key in meta_keys:
    pipe.hgetall(KEY_ORDER_MANIFEST.format(key))
results = pipe.execute()

order_data: Dict[str, Dict[str, str]] = dict(zip(meta_keys, results))

orders = list(order_data.values())

if len(orders) > 0:
    orders = sorted(orders, key=lambda x: x['DateCreated'])
    output.writelines(json.dumps(orders))
