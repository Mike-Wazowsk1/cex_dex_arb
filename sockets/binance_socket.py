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
# Loading keys from config file

# This is not REQUIRED
# client = Client(actual_api_key, actual_secret_key, tld="com")
db = DataBase()

def printer(msg, path):
    """
    Function to process the received messages
    param msg: input message
    """
    symbol = path.split("@")[0]

    timestamp = msg['lastUpdateId']
    asks = msg['asks']
    bids = msg['bids']

    asks_price = np.array([float(x[0]) for x in asks[:15]])
    asks_quantity = np.array([float(x[1]) for x in asks[:15]])
    numerator = (asks_price * asks_quantity).sum()
    asks_amount = (asks_quantity).sum()
    asks_avg_price = numerator/asks_amount
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


    db.update_db(db_name="binance",symbol=symbol.lower(),asks_price=asks_avg_price,bids_price=bids_avg_price,asks_amount=asks_amount,bids_amount=bids_amount,timestamp=int(timestamp))

    # with open(f"binance_data/{symbol}_bids.pkl", 'wb') as f:
    #     pkl.dump(bids_avg_price, f)
    # f.close()

    # with open(f"binance_data/{symbol}_asks.pkl", 'wb') as f:
    #     pkl.dump(asks_avg_price, f)
    # f.close()


async def writer(bm, symbol, loop):
    print(symbol)
    bm.start_depth_socket(callback=printer, symbol=symbol,
                          depth=BinanceSocketManager.WEBSOCKET_DEPTH_20)
    time.sleep(0.1)


def reciver(client, current_batch, global_dict):
    twm = ThreadedWebsocketManager()
    twm.start()
    ss = []
    for symbol in current_batch:
        twm.start_depth_socket(
            callback=printer, symbol=symbol, depth=BinanceSocketManager.WEBSOCKET_DEPTH_20)
        ss.append(symbol)
    print(f"Len: {len(ss)} {ss[:3]}")
    # time.sleep(3)
    twm.join()


# def proxy(client,current_batch,loop,type='process'):
#     if type=='process':
#         asyncio.run(reciver(client,current_batch,loop))
#     elif type == 'thread':
#         asyncio.ensure_future(writer(client,current_batch,loop))

async def get_snapshot(symbol):
    base_url = f'https://api.binance.com/api/v3/depth?symbol={symbol}&limit=20'
    msg = requests.get(base_url).json()

    timestamp = msg['lastUpdateId']
    asks = msg['asks']
    bids = msg['bids']

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

    print("binance",symbol,asks_avg_price,bids_avg_price,asks_amount,bids_amount,int(timestamp))
    db.init_snapshot(db_name="binance",symbol=symbol.lower(),asks_price=asks_avg_price,bids_price=bids_avg_price,asks_amount=asks_amount,bids_amount=bids_amount,timestamp=int(timestamp))


    # with open(f"binance_data/{symbol}_bids.pkl", 'wb') as f:
    #     pkl.dump(bids_avg_price, f)
    # f.close()

    # with open(f"binance_data/{symbol}_asks.pkl", 'wb') as f:
    #     pkl.dump(asks_avg_price, f)
    # f.close()



async def main():
    global loop
    client = await AsyncClient.create()

    info = await client.get_exchange_info()
    symbols = [x['symbol'] for x in info['symbols']]
    symbols = [x for x in set(symbols)]
    batch_size = ceil(200)
    bm_count = ceil(len(symbols)/batch_size)
    print(
        f"total pair: {len(symbols)} batch_size: {batch_size} bm_count: {bm_count} ")
    for symbol in symbols:
        await get_snapshot(symbol)

    ps = []
    for i in range(bm_count):
        current_batch = symbols[i*batch_size:batch_size*i+batch_size]
        # print(current_batch)
        print(current_batch[:3])
        p = mp.Process(target=reciver, args=[client, current_batch, 0])
        p.start()
        # p.join()
    # for p in ps:
        #  p.join()

    # start any sockets here, i.e a trade socket
    # for symbol in symbols:
    #     ts = bm.depth_socket(symbol, depth=BinanceSocketManager.WEBSOCKET_DEPTH_20)
    #     await reciver(ts,symbol)

    # # then start receiving messages
    # async with ts as tscm:
    #     while True:
    #         res = await tscm.recv()
    #         print(res)

    await client.close_connection()
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
