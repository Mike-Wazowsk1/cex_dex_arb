import asyncio
from itertools import count
import json
import logging
from binance import AsyncClient, BinanceSocketManager
from binance import ThreadedWebsocketManager
import multiprocessing as mp
import threading as th
from math import floor, ceil
import pickle as pkl
import time
# Importing libraries
from binance.client import Client
import configparser
from binance.streams import ThreadedWebsocketManager
import requests
import numpy as np
from database.db import DataBase


db = DataBase()
client = Client()


order_book = {
    "lastUpdateId": 0,
    "bids": [],
    "asks": []
}
def get_snapshot(symbol):
    db.init_snapshot(db_name="binance", symbol=symbol.lower(
    ), asks_price=0, bids_price=0, asks_amount=0, bids_amount=0, count=0, timestamp=int(0))

info = client.get_exchange_info()
symbols = [x['symbol'] for x in info['symbols']]
symbols = [x for x in set(symbols)]
with mp.Pool() as pool:
    pool.map(get_snapshot, symbols)



tables = db.get_all_tables()
tables = [x[0] for x in tables]
seen = []
symbols_array = []
all_symbols = []
basic_symbols = np.array(db.get_arb_info('binance'))[:,0]
for table in tables:
        if table !='binance':
            try:
                symbols = np.array(db.get_arb_info(table))[:,0]
                symbols_array.append(symbols)
            except:
                continue
for batch in symbols_array:
    all_symbols.extend(list(set(batch)&set(basic_symbols)))
all_symbols = list(set(all_symbols))

for symbol in all_symbols:
    if symbol.upper() not in seen:
        seen.append(symbol.upper())
symbols = seen
symbols = ["SUSHIUSDT"]
        


def init_snapshot(symbol):
    global CNT
    print("REST request")
    """
    Retrieve order book
    """
    base_url = f'https://api.binance.com/api/v3/depth?symbol={symbol}&limit=1000'
    msg = requests.get(base_url).json()
    CNT += 1
    if CNT >= 1200:
        time.sleep(30)
        CNT = 0 
    return msg


def manage_order_book(side, update, symbol):
    """
    Updates local order book's bid or ask lists based on the received update ([price, quantity])
    """
    price, quantity = update
    # price exists: remove or update local order
    print(len(manager[symbol.lower()][side]))
    print(update)
    print(manager[symbol.lower()][side])
    
    for i in range(0, len(manager[symbol.lower()][side])-1):
        print(i)
        if price == manager[symbol.lower()][side][i][0]:
            # quantity is 0: remove
            if float(quantity) == 0:
                manager[symbol.lower()][side].pop(i)
            else:
                # quantity is not 0: update the order with new quantity
                manager[symbol.lower()][side][i] = update

    # price not found: add new order
    if float(quantity) != 0:
        manager[symbol.lower()][side].insert(-1, update)
        if side == 'asks':
            # asks prices in ascendant order
            manager[symbol.lower()][side] = sorted(
                manager[symbol.lower()][side], key=lambda x: float(x[0]))
        else:
            manager[symbol.lower()][side] = sorted(manager[symbol.lower()][side], key=lambda x: float(
                x[0]), reverse=True)  # bids prices in descendant order

    if len(manager[symbol.lower()][side]) > 1000:
        manager[symbol.lower()][side].pop(len(manager[symbol.lower()][side])-1)


def process_updates(message, symbol):
    for update in message['b']:
        manage_order_book('bids', update, symbol)

    for update in message['a']:
        manage_order_book('asks', update, symbol)


def message_handler(message, path):
    global order_book, manager
    symbol = path.split("@")[0]
    try:
        if "depthUpdate" in json.dumps(message):
            last_update_id = manager[symbol.lower()]['lastUpdateId']
            if message['u'] <= last_update_id:
                return
            elif message['U'] <= last_update_id + 1 <= message['u']:
                manager[symbol.lower()]['lastUpdateId'] = message['u']
                process_updates(message, symbol)
            else:
                print(
                    f"Out of sync, re-syncing... u: {message['u']} last:  {last_update_id} U: {message['U']}")
                manager[symbol.lower()] = init_snapshot(symbol.upper())
                time.sleep(1)
                last_update_id = manager[symbol.lower()]['lastUpdateId']
                print(f"NEW: u: {message['u']} last:  {last_update_id} U: {message['U']}")

        asks = np.array(sorted(
            manager[symbol.lower()]['asks'], key=lambda x: float(x[0])))[:15]
        bids = np.array(sorted(manager[symbol.lower()]['bids'], key=lambda x: float(
            x[0]), reverse=True))[:15]
        printer(asks, bids, symbol)
    except Exception as e:
        print(e)
        if "lastUpdateId" in str(e):
            print(manager[symbol.lower()])

        print(f"ERROR SYMBOL: {symbol}")



def printer(asks, bids, symbol):
    """
    Function to process the received messages
    param msg: input message
    """
    try:
        if symbol.lower() == 'ltcusdt':
            print(asks)
            print(bids)
        asks_price = asks[:,0].astype(np.float64)
        asks_quantity = asks[:,1].astype(np.float64)
        user_max_amount = float(db.get_info_col('max_amount'))
        quantity = 0
        count = 0
        mean_price = 0
        usdt_quantity = 0
        for i, val in enumerate(asks_quantity):
            if usdt_quantity < user_max_amount:
                quantity += val
                mean_price += asks_price[i]
                usdt_quantity += quantity * asks_price[i]
                count += 1

        asks_amount = quantity
        asks_avg_price = mean_price/count

        bids_price = bids[:,0].astype(np.float64)
        bids_quantity = bids[:,1].astype(np.float64)

        quantity = 0
        count = 0
        mean_price = 0
        usdt_quantity = 0
        for i, val in enumerate(bids_quantity):
            if usdt_quantity < user_max_amount:
                quantity += val
                mean_price += bids_price[i]
                usdt_quantity += quantity * bids_price[i]
                count += 1

        bids_amount = quantity
        timestamp = time.time()
        bids_avg_price = mean_price/count

        db.update_db(db_name="binance", symbol=symbol.lower(), asks_price=asks_avg_price,
                     bids_price=bids_avg_price, asks_amount=asks_amount, bids_amount=bids_amount, count=count, timestamp=int(timestamp))
    except Exception as e:
        print("I'm here")
        print(e)


async def writer(bm, symbol, loop):
    bm.start_depth_socket(callback=message_handler, symbol=symbol)


def reciver(client, current_batch, global_dict):
    global manager,CNT
    CNT = 0 
    twm = ThreadedWebsocketManager()
    twm.start()
    for symbol in current_batch:
        manager[symbol.lower()] = init_snapshot(symbol)
        twm.start_depth_socket(
            callback=message_handler, symbol=symbol)
        time.sleep(3)
    twm.join()




async def main():
    global loop, manager
    manager = {}

    client = Client()

    batch_size = ceil(300)
    bm_count = ceil(len(symbols)/batch_size)
    print(
        f"total pair: {len(symbols)} batch_size: {batch_size} bm_count: {bm_count} ")

    for i in range(bm_count):
        current_batch = symbols[i*batch_size:batch_size*i+batch_size]
        p = mp.Process(target=reciver, args=[client, current_batch, 0])
        p.start()
        time.sleep(5)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
