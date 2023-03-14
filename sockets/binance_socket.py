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
from functools import wraps
import random


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper   

@timeit
def get_snapshot(symbol):
    db.init_snapshot(db_name="binance", symbol=symbol.lower(
    ), asks_price=0, bids_price=0, asks_amount=0, bids_amount=0, count=0, timestamp=int(0))


@timeit
def init_snapshot(symbol,no_wait=False):
    
    global CNT, base_info
    # if base_info.get(symbol.lower(),False) == True:
    #     return manager[symbol.lower()]
    
    # if base_info.get(symbol.lower(),False) == False:
    # base_info[symbol.lower()] = True
    """
    Retrieve order book
    """
    # time.sleep(random.randint(0,5))
    base_url = f'https://api.binance.com/api/v3/depth?symbol={symbol}&limit=20'
    msg = requests.get(base_url).json()
    # print(f"REST request: {symbol}")

    # base_info[symbol.lower()] = False
    return msg


def manage_order_book(side, update, symbol):
    """
    Updates local order book's bid or ask lists based on the received update ([price, quantity])
    """
    price, quantity = update
    # price exists: remove or update local order
    i = 0
    price_found = False
    while i < len(manager[symbol.lower()][side]):
    # for i in range(0, len(manager[symbol.lower()][side])-1):
        if float(price) == float(manager[symbol.lower()][side][i][0]):
            price_found = True
            # quantity is 0: remove
            if float(quantity) == 0:
                manager[symbol.lower()][side].pop(i)
                i -= 1
            else:
                # quantity is not 0: update the order with new quantity
                manager[symbol.lower()][side][i] = update

        i += 1

    # price not found: add new order
    if float(quantity) != 0:   
        if not price_found: 
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


async def message_handler(message, path):
    global order_book, manager,base_info, counter
    t = time.perf_counter_ns()
    symbol = path.split("@")[0]
    counter[symbol.lower()] += 1
    # if counter[symbol.lower()] >= 100:
    #     print(f"REACH limit: {symbol}")
    #     manager[symbol.lower()] = init_snapshot(symbol.upper())
    #     counter[symbol.lower()] = 0
    #     return 


        
    # if base_info[symbol.lower()] >= 50_0000:
    #     print(f"Update symbol: {symbol}")
    #     manager[symbol.lower()] = init_snapshot(symbol.upper())
    #     time.sleep(1)
    #     base_info[symbol.lower()] = 0
    #     return 
    try:
        if "depthUpdate" in json.dumps(message):
            last_update_id = manager[symbol.lower()]['lastUpdateId']
            if message['u'] <= last_update_id:
                pass
            elif message['U'] <= last_update_id + 1 <= message['u']:
                manager[symbol.lower()]['lastUpdateId'] = message['u']
                process_updates(message, symbol)

            else:
                print(
                    f"Out of sync, re-syncing... u: {message['u']} last:  {last_update_id} U: {message['U']} symbol: {symbol}")
                manager[symbol.lower()] = init_snapshot(symbol.upper())
                last_update_id = manager[symbol.lower()]['lastUpdateId']
                print(f"NEW: u: {message['u']} last:  {last_update_id} U: {message['U']} symbol: {symbol}")


        asks = np.array(sorted(
            manager[symbol.lower()]['asks'], key=lambda x: float(x[0])))[:15]
        bids = np.array(sorted(manager[symbol.lower()]['bids'], key=lambda x: float(
            x[0]), reverse=True))[:15]
        printer(asks, bids, symbol)
        # print(f"Execution take: {time.perf_counter_ns() - t}")
    except KeyError as e:
        print(f"Symbol {symbol} not handeled")
    # except Exception as e:
    #     print(e)
  
    #     print(manager[symbol.lower()])


    #     print(f"ERROR SYMBOL: {symbol}")
    #     await asyncio.sleep(10)


@timeit
def printer(asks, bids, symbol):
    """
    Function to process the received messages
    param msg: input message
    """
    asks_price = asks[:,0].astype(np.float64)
    asks_quantity = asks[:,1].astype(np.float64)
    user_max_amount = float(db.get_info_col('max_amount'))
    quantity = 0
    count = 1
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
    count = 1
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

    db.update_db(db_name="binance", symbol=symbol.lower(), asks_price=round(asks_avg_price,10),
                    bids_price=round(bids_avg_price,10), asks_amount=round(asks_amount,10), bids_amount=round(bids_amount,10), count=count, timestamp=int(timestamp))




def reciver(client, current_batch, global_dict):
    global manager,CNT,base_info, counter
    CNT = 0 
    twm = ThreadedWebsocketManager()
    twm.start()
    for symbol in current_batch:
        time.sleep(random.randint(3,10))

        base_info[symbol.lower()] = 0
        counter[symbol.lower()] = 0
        twm.start_depth_socket(callback=message_handler, symbol=symbol)
        print(f"Reciver: {symbol}")
        manager[symbol.lower()] = init_snapshot(symbol,no_wait=True)
        time.sleep(random.randint(1,10))


    twm.join()




async def main():
    global loop, manager, base_info, counter, db
    manager = {}
    base_info = {}
    counter = {}
    db = DataBase()
    client = Client()


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


    batch_size = ceil(10)
    bm_count = ceil(len(symbols)/batch_size)
    print(
        f"total pair: {len(symbols)} batch_size: {batch_size} bm_count: {bm_count} ")

    for i in range(bm_count):
        time.sleep(random.randint(1,10))
        current_batch = symbols[i*batch_size:batch_size*i+batch_size]
        # symbol = [symbol]
        p = mp.Process(target=reciver, args=[client, current_batch, 0])
        # p.setDaemon(True)
        # p.start()
        time.sleep(random.randint(1,10))

        p.start()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
