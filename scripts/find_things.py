from typing import Dict

import settings
from lib.database import Database

name = input('Name? ').lower()

for version in settings.API_DB_CONFIG.keys():
    print("*********** Searching {} ***********".format(version))
    Database().connect_db(version)

    key_iter = Database().connection.scan_iter('Things:Metadata:*', 10000)

    meta_keys = list(key_iter)

    pipe = Database().connection.pipeline()
    for key in meta_keys:
        pipe.hgetall(key)
    results = pipe.execute()

    things: Dict[str, Dict[str, str]] = dict(zip(meta_keys, results))

    for thing in things.values():
        if thing['Name'].lower().find(name) > -1:
            print(thing)
