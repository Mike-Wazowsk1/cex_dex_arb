from pybit import spot
# from pybit import 
import time
import requests
import json
import pickle as pkl
import numpy as np
ws = spot.WebSocket(
    test=True,
)
def handle_orderbook(message):
    data = message['data'][0]
    symbol = data['s']
    timestamp = data['t']
    bids = data['b'][:15]
    asks = data['a'][:15]

    asks_price = np.array([float(x[0]) for x in asks[:15]])
    asks_quantity = np.array([float(x[1]) for x in asks[:15]])
    numerator = (asks_price * asks_quantity).sum()
    denumerator = (asks_quantity).sum()
    asks_avg_price = numerator/denumerator

    bids_price = np.array([float(x[0]) for x in bids[:15]])
    bids_quantity = np.array([float(x[1]) for x in bids[:15]])
    numerator = (bids_price * bids_quantity).sum()
    denumerator = (bids_quantity).sum()

    bids_avg_price = numerator/denumerator

    with open(f"bybit_data/{symbol}_bids.pkl", 'wb') as f:
        pkl.dump(bids_avg_price, f)
    f.close()

    with open(f"bybit_data/{symbol}_asks.pkl", 'wb') as f:
        pkl.dump(asks_avg_price, f)
    f.close()
    # data = [symbol,timestamp,asks,bids]
    # with open(f"bybit_data/{symbol.lower()}.pkl", 'wb') as f:
    #     pkl.dump(data,f)
    # f.close()



# Similarly, if you want to listen to the WebSockets of other markets:
ws_spot = spot.WebSocket(test=False)
symbols  = requests.get("https://api.bybit.com/v2/public/tickers").json()['result']
symbols = [x['symbol'] for x in symbols]
ws_spot.depth_v1_stream(handle_orderbook, symbols)
time.sleep(2)
# ws_spot.depth_v1_stream(handle_orderbook, "MATICUSDT")

# print(len(symbols))
# for symbol in symbols:
#     print(symbol)
#     try:
#         ws_spot.depth_v2_stream(handle_orderbook, symbol)
#     except Exception as e :
#         print(e)


while True:
    # This while loop is required for the program to run. You may execute
    # additional code for your trading logic here.
    time.sleep(1)