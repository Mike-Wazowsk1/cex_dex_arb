from pybit import spot
# from pybit import 
import time
import requests
import json
import pickle as pkl
import numpy as np


from database.db import DataBase
db = DataBase()


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

    db.update_db(db_name="bybit",symbol=symbol.lower(),asks_price=asks_avg_price,bids_price=bids_avg_price,asks_amount=asks_amount,bids_amount=bids_amount,timestamp=int(timestamp))
    print(symbol)
    


    # with open(f"bybit_data/{symbol}_bids.pkl", 'wb') as f:
    #     pkl.dump(bids_avg_price, f)
    # f.close()

    # with open(f"bybit_data/{symbol}_asks.pkl", 'wb') as f:
    #     pkl.dump(asks_avg_price, f)
    # f.close()
    # data = [symbol,timestamp,asks,bids]
    # with open(f"bybit_data/{symbol.lower()}.pkl", 'wb') as f:
    #     pkl.dump(data,f)
    # f.close()



# Similarly, if you want to listen to the WebSockets of other markets:
ws_spot = spot.WebSocket(test=False)
symbols  = requests.get("https://api.bybit.com/v2/public/tickers").json()['result']
symbols = [x['symbol'] for x in symbols]
for symbol in symbols:
    db.init_snapshot(db_name="bybit",symbol=symbol.lower(),asks_price=0,bids_price=0,asks_amount=0,bids_amount=0,timestamp=int(0))
print(f"LEN: {len(symbols)}")
time.sleep(20)

ws_spot.depth_v1_stream(handle_orderbook, symbols)
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
    time.sleep(0.1)