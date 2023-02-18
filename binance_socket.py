import asyncio
from binance import AsyncClient, BinanceSocketManager
from binance import ThreadedWebsocketManager
import multiprocessing as mp 
import threading as th
from math import floor,ceil
import pickle as pkl
import time
# Importing libraries
from binance.client import Client
import configparser
from binance.streams import ThreadedWebsocketManager

# Loading keys from config file

# This is not REQUIRED
# client = Client(actual_api_key, actual_secret_key, tld="com")


def printer(msg,path):
    """
    Function to process the received messages
    param msg: input message
    """
    symbol = path.split("@")[0]

    timestamp = msg['lastUpdateId']
    asks = msg['asks']
    bids = msg['bids']
    data = [symbol,timestamp,asks,bids]
    # print(data)
    with open(f"binance_data/{symbol}.pkl", 'wb') as f:
        pkl.dump(data,f)
    f.close()


async def writer(bm,symbol,loop):
        print(symbol)
        bm.start_depth_socket(callback = printer, symbol=symbol, depth=BinanceSocketManager.WEBSOCKET_DEPTH_20)
        time.sleep(0.1)


        # async with ts as tscm:
        #     while True:
        #         res = await tscm.recv()
        #         timestamp = res['lastUpdateId']
        #         asks = res['asks']
        #         bids = res['bids']
        #         data = [symbol,timestamp,asks,bids]
        #         with open(f"binance_data/{symbol}.pkl", 'wb') as f:
        #             pkl.dump(data,f)
        #         f.close()
        #         await asyncio.sleep(0.1)
                


def reciver(client,current_batch,global_dict):
    twm = ThreadedWebsocketManager()
    twm.start()
    ss = []
    for symbol in current_batch:
        twm.start_depth_socket(callback = printer, symbol=symbol, depth=BinanceSocketManager.WEBSOCKET_DEPTH_20)
        ss.append(symbol)
    print(f"Len: {len(ss)} {ss[:3]}")
        # time.sleep(3)
    twm.join()
        



# def proxy(client,current_batch,loop,type='process'):
#     if type=='process':
#         asyncio.run(reciver(client,current_batch,loop))
#     elif type == 'thread':
#         asyncio.ensure_future(writer(client,current_batch,loop))




async def main():
    global loop
    client = await AsyncClient.create()
    
    info = await client.get_exchange_info()
    symbols = [x['symbol'] for x in info['symbols']]
    symbols = [x for x in set(symbols)]
    batch_size = ceil(100)
    bm_count = ceil(len(symbols)/batch_size)
    print(f"batch_size: {batch_size} bm_count: {bm_count} ")
    # bm_count = 1
    ps = []
    for i in range(bm_count):
        current_batch = symbols[0*batch_size:batch_size*i+batch_size]
        # print(current_batch)
        p = mp.Process(target = reciver, args=[client,current_batch,0])
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