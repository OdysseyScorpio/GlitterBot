import json

from lib.database import Database
from lib.gwpcc import consts
from lib.gwpcc.things.thing import Thing

name = input('Colony ID: ')
version = input('Market: ')

print("*********** Searching {} ***********".format(version))
Database().connect_db(version)
connection = Database().connection

thing_list = connection.get(consts.KEY_COLONY_SUPPORTED_THINGS.format(name))

things = Thing.get_many_from_database(thing_list, connection)

for thing in things.values():
    if thing.FromDatabase:
        print(json.dumps(thing.to_dict()))
