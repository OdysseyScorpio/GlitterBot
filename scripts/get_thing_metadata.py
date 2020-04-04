from datetime import datetime, timedelta

import settings
from lib.database import Database
from lib.gwpcc.things.thing import Thing

name = input('Name? ')
quality = input('Quality? ')
stuff = input('Stuff? ')

for version in settings.API_DB_CONFIG.keys():
    Database().connect_db(version)
    connection = Database().connection
    loaded_thing = Thing.get_from_database({'Name': name,
                                            'Quality': quality if quality != "" else None,
                                            'StuffType': stuff if stuff != "" else None,
                                            }, connection)
    if loaded_thing.FromDatabase:
        print("Database: {}, Found {}".format(version, loaded_thing))
        print(vars(loaded_thing))

        day = datetime.utcnow()

        for d in range(0, 10):
            delta = timedelta(days=d)
            day = (datetime.utcnow() - delta).strftime('%Y-%m-%d')
            loaded_thing = Thing.get_from_database({'Name': name,
                                                    'Quality': quality if quality != "" else None,
                                                    'StuffType': stuff if stuff != "" else None,
                                                    }, connection, date=day)

            print(day)
            for direction in loaded_thing.TradeHistory.keys():
                print(direction)
                print(loaded_thing.TradeHistory[direction])
            print()


    else:
        print("Database: {}, Not Found".format(version))
