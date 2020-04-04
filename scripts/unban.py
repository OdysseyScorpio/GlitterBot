import hashlib
from datetime import datetime

from lib.database import Database
from lib.gwpcc import consts

db_name = input('Which DB: ').lower()
user_type = input('User Type: ').lower()
user_id = input('User ID: ')

ban_key = ''
if user_type == 'steam':
    ban_key = consts.KEY_USER_STEAM_ID_BANNED_SET
elif user_type == 'normal':
    ban_key = consts.KEY_USER_NORMAL_ID_BANNED_SET
else:
    print('Wrong user type')
    exit(1)

Database().connect_db(db_name)
db = Database().connection

is_banned = db.sismember(ban_key, user_id)
if not is_banned:
    print('User is not banned. Check ID')
    exit(1)

code = hashlib.sha1("{}".format(datetime.utcnow()).encode('UTF8')).hexdigest()[0:8]

db.set(consts.KEY_USER_ACTIVATION_REQUEST_TOKEN.format(user_id), code, ex=86400)

print('Token is {}, valid for 1 day.'.format(code))
