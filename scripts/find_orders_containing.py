import json
from typing import Dict

from lib.database import Database

name = input('Thing hash: ').lower()

output = open('output.json', 'w')

for version in ['u1']:
    print("*********** Searching {} ***********".format(version))
    Database().connect_db(version)

    key_iter = Database().connection.scan_iter('Orders:Manifest:*', count=1000)

    meta_keys = list(key_iter)

    pipe = Database().connection.pipeline()
    for key in meta_keys:
        pipe.hgetall(key)
    results = pipe.execute()

    order_data: Dict[str, Dict[str, str]] = dict(zip(meta_keys, results))

    orders = []

    for order in order_data.values():

        try:
            bought = json.loads(order['ThingsBoughtFromGwp'])
        except:
            bought = {}

        try:
            sold = json.loads(order['ThingsSoldToGwp'])
        except:
            sold = {}

        has_match = False
        for thing in bought:
            if thing['Hash'] == name:
                has_match = True
                break
        for thing in sold:
            if thing['Hash'] == name:
                has_match = True
                break

        if has_match:
            orders.append(order)

    if len(orders) > 0:
        orders = sorted(orders, key=lambda x: x['DateCreated'])
        output.writelines(json.dumps(orders))
