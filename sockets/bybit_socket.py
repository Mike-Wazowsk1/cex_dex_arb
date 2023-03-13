from pybit import spot
# from pybit import
import time
import requests
import json
import pickle as pkl
import numpy as np
import multiprocessing as mp


from database.db import DataBase
db = DataBase()


def handle_orderbook(message):
    if message['type'] == 'data':
        
        data = message['data']
        symbol = data['s']
        timestamp = data['t']
        bids = sorted(data['b'][:15],reverse=True)
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
    if symbol.lower() == 'ltcusdt':
        print(asks_price)
        print(bids_price)
        


# Similarly, if you want to listen to the WebSockets of other markets:
ws_spot = spot.WebSocket(testnet=False)
http = spot.HTTP()
symbols = [x['s'] for x in http.get_tickers()['result']['list']]
print(symbols)
for symbol in symbols:
    db.init_snapshot(db_name="bybit", symbol=symbol.lower(
    ), asks_price=0, bids_price=0, asks_amount=0, bids_amount=0, count=0, timestamp=int(0))
print(f"LEN: {len(symbols)}")
for symbol in symbols:
    try:
        mp.Process(target=ws_spot.orderbook_stream,args=[handle_orderbook, symbol]).start()
    except:
        print("can't")
        print(symbol)


while True:
    time.sleep(0.1)
