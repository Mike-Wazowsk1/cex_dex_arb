from pybit import spot
# from pybit import
import time
import requests
import json
import pickle as pkl
import numpy as np


from database.db import DataBase
db = DataBase()


def handle_orderbook(message):
    print(message)
    if message['type'] == 'data':
        data = message['data']
        symbol = data['s']
        timestamp = data['t']
        bids = data['b'][:15]
        asks = data['a'][:15]
        asks_price = np.array([float(x[0]) for x in asks[:15]])
        asks_quantity = np.array([float(x[1]) for x in asks[:15]])
        numerator = (asks_price * asks_quantity).sum()
        asks_amount = (asks_quantity).sum()
        if asks_amount == 0:
            asks_avg_price = 0
            return 0
        else:
            asks_avg_price = numerator/asks_amount

        bids_price = np.array([float(x[0]) for x in bids[:15]])
        bids_quantity = np.array([float(x[1]) for x in bids[:15]])
        numerator = (bids_price * bids_quantity).sum()
        bids_amount = (bids_quantity).sum()
        if bids_amount == 0:
            bids_avg_price = 0
            return 0
        else:
            bids_avg_price = numerator/bids_amount

        db.update_db(db_name="bybit", symbol=symbol.lower(), asks_price=asks_avg_price, bids_price=bids_avg_price,
                    asks_amount=asks_amount, bids_amount=bids_amount, timestamp=int(timestamp))
    if message['type'] == "snapshot":
        data = message['data']
        symbol = data['s']
        timestamp = data['t']
        bids = data['b'][:15]
        asks = data['a'][:15]
        asks_price = np.array([float(x[0]) for x in asks[:15]])
        asks_quantity = np.array([float(x[1]) for x in asks[:15]])
        numerator = (asks_price * asks_quantity).sum()
        asks_amount = (asks_quantity).sum()
        if asks_amount == 0:
            asks_avg_price = 0
            return 0
        else:
            asks_avg_price = numerator/asks_amount

        bids_price = np.array([float(x[0]) for x in bids[:15]])
        bids_quantity = np.array([float(x[1]) for x in bids[:15]])
        numerator = (bids_price * bids_quantity).sum()
        bids_amount = (bids_quantity).sum()
        if bids_amount == 0:
            bids_avg_price = 0
            return 0
        else:
            bids_avg_price = numerator/bids_amount
        db.update_db(db_name="bybit", symbol=symbol.lower(), asks_price=asks_avg_price, bids_price=bids_avg_price,
                    asks_amount=asks_amount, bids_amount=bids_amount, timestamp=int(timestamp))



# Similarly, if you want to listen to the WebSockets of other markets:
ws_spot = spot.WebSocket(testnet=False)
http  = spot.HTTP()
symbols = [x['s'] for x in http.get_tickers()['result']['list']]
print(symbols)
for symbol in symbols:
    db.init_snapshot(db_name="bybit", symbol=symbol.lower(
    ), asks_price=0, bids_price=0, asks_amount=0, bids_amount=0, timestamp=int(0))
print(f"LEN: {len(symbols)}")
for symbol in symbols:
    try:
        print(symbol)
        ws_spot.orderbook_stream(handle_orderbook, symbol,)
    except:
        print("can't")
        print(symbol)



while True:
    time.sleep(0.1)
