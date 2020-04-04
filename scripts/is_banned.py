import settings
from lib.database import Database
from lib.gwpcc.colonies.colony import Colony

colony_hash = input('Colony Hash? ')

for version in settings.API_DB_CONFIG.keys():
    Database().connect_db(version)
    connection = Database().connection

    print('Searching database {}'.format(version))

    colony = Colony.get_from_database_by_hash(colony_hash, connection)
    if colony:
        print('Found {}'.format(colony))
        print('Owner Type: {}, ID: {}'.format(colony.OwnerType, colony.OwnerID))
        print('Ban status: {}'.format(colony.IsBanned()))
