#type: ignore
from multiprocessing import Process
from threading import Thread
from pybit import spot
# from pybit import
import time
import requests
import json
import pickle as pkl
import numpy as np
import multiprocessing as mp
from random import randint


from database.db import DataBase
db = DataBase()


def handle_orderbook(message):
    if message['type'] == 'data':

        data = message['data']
        symbol = data['s']
        timestamp = data['t']
        bids = sorted(data['b'][:15], reverse=True)
        asks = sorted(data['a'][:15])
        asks_price = np.array([float(x[0]) for x in asks[:15]])
        asks_quantity = np.array([float(x[1]) for x in asks[:15]])
        user_max_amount = db.get_info_col('max_amount')
        quantity = 0
        count = 0
        mean_price = 0
        usdt_quantity = 0
        for i, val in enumerate(asks_quantity):
            if usdt_quantity < user_max_amount:
                quantity += val
                mean_price += asks_price[i] * val
                usdt_quantity += quantity * asks_price[i]
                count += 1

        asks_amount = quantity
        asks_avg_price = mean_price/asks_amount

        bids_price = np.array([float(x[0]) for x in bids[:15]])
        bids_quantity = np.array([float(x[1]) for x in bids[:15]])
        user_max_amount = db.get_info_col('max_amount')
        quantity = 0
        count = 0
        mean_price = 0
        usdt_quantity = 0
        for i, val in enumerate(bids_quantity):
            if usdt_quantity < user_max_amount:
                quantity += val
                mean_price += bids_price[i] * val
                usdt_quantity += quantity * bids_price[i]
                count += 1
        bids_amount = quantity
        bids_avg_price = mean_price/bids_amount

        db.update_db(db_name="bybit", symbol=symbol.lower(), asks_price=asks_avg_price,
                     bids_price=bids_avg_price, asks_amount=asks_amount, bids_amount=bids_amount, count=count, timestamp=int(timestamp))

    if message['type'] == "snapshot":
        data = message['data']
        symbol = data['s']
        timestamp = data['t']
        bids = data['b'][:15]
        asks = data['a'][:15]
        asks_price = np.array([float(x[0]) for x in asks[:15]])
        asks_quantity = np.array([float(x[1]) for x in asks[:15]])
        user_max_amount = float(db.get_info_col('max_amount'))

        quantity = 0
        count = 0
        mean_price = 0
        usdt_quantity = 0
        for i, val in enumerate(asks_quantity):
            if usdt_quantity < user_max_amount:
                quantity += val
                mean_price += asks_price[i] * val
                usdt_quantity += quantity * asks_price[i]
                count += 1

        asks_amount = quantity
        asks_avg_price = mean_price/asks_amount

        bids_price = np.array([float(x[0]) for x in bids[:15]])
        bids_quantity = np.array([float(x[1]) for x in bids[:15]])
        user_max_amount = float(db.get_info_col('max_amount'))
        quantity = 0
        count = 0
        mean_price = 0
        usdt_quantity = 0
        for i, val in enumerate(bids_quantity):
            if usdt_quantity < user_max_amount:
                quantity += val
                mean_price += bids_price[i] * val
                usdt_quantity += quantity * bids_price[i]
                count += 1

        bids_amount = quantity
        bids_avg_price = mean_price/bids_amount

        db.update_db(db_name="bybit", symbol=symbol.lower(), asks_price=asks_avg_price,
                     bids_price=bids_avg_price, asks_amount=asks_amount, bids_amount=bids_amount, count=count, timestamp=int(timestamp))


def proxy(handle_orderbook, symbol):
    ws_spot = spot.WebSocket(testnet=False)
    ws_spot.orderbook_stream(handle_orderbook, symbol)
    while True:
        time.sleep(0.1)


# Similarly, if you want to listen to the WebSockets of other markets:
ws_spot = spot.WebSocket(testnet=False)
http = spot.HTTP()
symbols = [x['s'] for x in http.get_tickers()['result']['list']]
for symbol in symbols:
    db.init_snapshot(db_name="bybit", symbol=symbol.lower(
    ), asks_price=0, bids_price=0, asks_amount=0, bids_amount=0, count=0, timestamp=int(0))
print(f"LEN: {len(symbols)}")
tables = db.get_all_tables()
tables = [x[0] for x in tables]
seen = []
symbols_array = []
all_symbols = []
basic_symbols = np.array(db.get_arb_info('bybit'))[:, 0]
for table in tables:
    if table != 'bybit':
        try:
            symbols = np.array(db.get_arb_info(table))[:, 0]
            symbols_array.append(symbols)
        except:
            continue
for batch in symbols_array:
    all_symbols.extend(list(set(batch) & set(basic_symbols)))
all_symbols = list(set(all_symbols))

for symbol in all_symbols:
    if symbol.upper() not in seen:
        seen.append(symbol.upper())
symbols = seen
print(f"LEN: {len(symbols)}")
for symbol in symbols:
    try:
        t = Process(target=proxy, args=[handle_orderbook, symbol])
        # t.setDaemon(True)
        t.start()
        time.sleep(randint(10, 60))
    except:
        print("can't")
        print(symbol)


while True:
    time.sleep(0.1)
