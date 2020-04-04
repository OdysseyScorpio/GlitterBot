import settings
from lib.database import Database

for version in settings.API_DB_CONFIG.keys():

    Database().connect_db(version)

    print(version)

    # colonies = Database().connection.lrange(consts.COLON)

    # all_thing_hashes = Database().connection.smembers(consts.KEY_THING_INDEX)
    #
    # all_things = Thing.get_many_from_database_by_hash(all_thing_hashes)
    #
    # pipe = Database().pipeline
    # keys = []
    # hashes = []
    # for key in Database().connection.scan_iter('Things:Metadata:*'):
    #     keys.append(key)
    #     pipe.hgetall(key)
    #
    # results = dict(zip(keys, pipe.execute()))
    #
    # pipe = Database().pipeline
    #
    # for key, thing in results.items():
    #     if 'Name' not in thing:
    #         hashes.append(key.split(':')[2])
    #         print('DEL {}'.format(key))
    #         pipe.delete(key)
    #
    # pipe.execute()

    pipe = Database().pipeline
    keys = []
    for key in Database().connection.scan_iter('Orders:Colony:*All'):
        keys.append(key)
        pipe.hgetall(key)

    results = dict(zip(keys, pipe.execute()))

    for key, orders in results.items():
        for order in orders:
            exists = Database().connection.exists(order)
