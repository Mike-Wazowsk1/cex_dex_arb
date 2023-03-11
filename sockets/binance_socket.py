import asyncio
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


def printer(msg, path):
    """
    Function to process the received messages
    param msg: input message
    """
    try:
        symbol = path.split("@")[0]
        timestamp = msg['lastUpdateId']

        asks = msg['asks']
        bids = msg['bids']

        asks_price = np.array([float(x[0]) for x in asks[:15]])
        asks_quantity = np.array([float(x[1]) for x in asks[:15]])
        user_max_amount = float(db.get_info_col('max_amount'))
        quantity = 0
        count = 0
        mean_price = 0
        prev_quantity = 0
        for i, val in enumerate(asks_quantity):
            if quantity < user_max_amount:
                quantity += asks_price[i] * val
                mean_price += (asks_price[i] * val) if quantity < user_max_amount else (
                    asks_price[i] * (user_max_amount - prev_quantity))
                prev_quantity = quantity
                count += 1

        
        asks_amount = min(quantity,user_max_amount)
        asks_avg_price = mean_price/asks_amount

        bids_price = np.array([float(x[0]) for x in bids[:15]])
        bids_quantity = np.array([float(x[1]) for x in bids[:15]])

        quantity = 0
        count = 0
        mean_price = 0
        prev_quantity = 0
        for i, val in enumerate(bids_quantity):
            if quantity < user_max_amount:
                quantity += asks_price[i] * val
                mean_price += (bids_price[i] * val) if quantity < user_max_amount else (
                    bids_price[i] * (user_max_amount - prev_quantity))
                prev_quantity = quantity
                count += 1
        
        bids_amount = min(quantity,user_max_amount)
        bids_avg_price = mean_price/bids_amount

        db.update_db(db_name="binance", symbol=symbol.lower(), asks_price=asks_avg_price,
                        bids_price=bids_avg_price, asks_amount=asks_amount, bids_amount=bids_amount, count=count, timestamp=int(timestamp))
    except Exception as e:
            print("I'm here")
            print(e)
            print(msg)


async def writer(bm, symbol, loop):
    print(symbol)
    bm.start_depth_socket(callback=printer, symbol=symbol,
                          depth=BinanceSocketManager.WEBSOCKET_DEPTH_20)


def reciver(client, current_batch, global_dict):
    twm = ThreadedWebsocketManager()
    twm.start()
    ss = []
    for symbol in current_batch:
        twm.start_depth_socket(
            callback=printer, symbol=symbol, depth=BinanceSocketManager.WEBSOCKET_DEPTH_20)
        ss.append(symbol)
    print(f"Len: {len(ss)} {ss[:3]}")
    twm.join()


def get_snapshot(symbol):
    # base_url = f'https://api.binance.com/api/v3/depth?symbol={symbol}&limit=20'
    # msg = requests.get(base_url).json()
    # try:
    #     timestamp = msg['lastUpdateId']
    # except:
    #     timestamp = 9999
    # asks = msg['asks']
    # bids = msg['bids']

    # asks_price = np.array([float(x[0]) for x in asks[:15]])
    # asks_quantity = np.array([float(x[1]) for x in asks[:15]])
    # numerator = (asks_price * asks_quantity).sum()
    # asks_amount = (asks_quantity).sum()

    # if asks_amount == 0:
    #     asks_avg_price = 0
    #     return 0
    # else:
    #     asks_avg_price = numerator/asks_amount

    # bids_price = np.array([float(x[0]) for x in bids[:15]])
    # bids_quantity = np.array([float(x[1]) for x in bids[:15]])
    # numerator = (bids_price * bids_quantity).sum()
    # bids_amount = (bids_quantity).sum()

    # if bids_amount == 0:
    #     bids_avg_price = 0
    #     return 0
    # else:
    #     bids_avg_price = numerator/bids_amount

    # ,asks_avg_price,bids_avg_price,asks_amount,bids_amount,int(timestamp))
    print("binance", symbol)
    db.init_snapshot(db_name="binance", symbol=symbol.lower(
    ), asks_price=0, bids_price=0, asks_amount=0, bids_amount=0, count=0, timestamp=int(0))


async def main():
    global loop
    client = await AsyncClient.create()

    info = await client.get_exchange_info()
    symbols = [x['symbol'] for x in info['symbols']]
    symbols = [x for x in set(symbols)]
    batch_size = ceil(300)
    bm_count = ceil(len(symbols)/batch_size)
    print(
        f"total pair: {len(symbols)} batch_size: {batch_size} bm_count: {bm_count} ")
    with mp.Pool() as pool:
        pool.map(get_snapshot, symbols)
    for i in range(bm_count):
        current_batch = symbols[i*batch_size:batch_size*i+batch_size]
        p = mp.Process(target=reciver, args=[client, current_batch, 0])
        p.start()
        time.sleep(300)

    await client.close_connection()
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
