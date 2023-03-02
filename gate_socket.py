import gate_api
from math import ceil
from gate_api.exceptions import ApiException, GateApiException
import multiprocessing as mp
import numpy as np
import pickle as pkl

LIMITS = 450
DEPTH = 15
INTERVAL = 0
WITH_ID = False
configuration = gate_api.Configuration(
    host = "https://api.gateio.ws/api/v4"
)
def reciver(pairs):
    for pair in pairs:
        api_response = api_instance.list_order_book(pair, interval=INTERVAL, limit=DEPTH, with_id=WITH_ID)
        asks_avg_price,bids_avg_price = get_avg_price(api_response)
        with open(f"gate_data/{pair.replace('_','')}_bids.pkl", 'wb') as f:
            pkl.dump(bids_avg_price, f)
        f.close()

        with open(f"gate_data/{pair.replace('_','')}_asks.pkl", 'wb') as f:
            pkl.dump(asks_avg_price, f)
        f.close()
        


def get_avg_price(api_response):
    bids = api_response.bids
    asks = api_response.asks
    timstamp = api_response.current
    asks_price = np.array([float(x[0]) for x in asks[:15]])
    asks_quantity = np.array([float(x[1]) for x in asks[:15]])
    numerator = (asks_price * asks_quantity).sum()
    denumerator = (asks_quantity).sum()
    asks_avg_price = numerator / denumerator

    bids_price = np.array([float(x[0]) for x in bids[:15]])
    bids_quantity = np.array([float(x[1]) for x in bids[:15]])
    numerator = (bids_price * bids_quantity).sum()
    denumerator = (bids_quantity).sum()

    bids_avg_price = numerator / denumerator
    return asks_avg_price, bids_avg_price

api_client = gate_api.ApiClient(configuration)
api_instance = gate_api.SpotApi(api_client)
# List all currency pairs supported
api_response = api_instance.list_currency_pairs()
pairs = [x.id for x in api_response]
instances_count = ceil(len(pairs)/LIMITS)
for i in range(instances_count):
    mp.Process(target=reciver, args=[pairs[i*LIMITS:LIMITS*i+LIMITS]])
# for pair in pairs:
#     api_response = api_instance.list_order_book(pair, interval=INTERVAL, limit=DEPTH, with_id=WITH_ID)
#     asks_avg_price,bids_avg_price = get_avg_price(api_response)
#     with open(f"gate_data/{pair.replace('_','')}_bids.pkl", 'wb') as f:
#         pkl.dump(bids_avg_price, f)
#     f.close()

#     with open(f"gate_data/{pair.replace('_','')}_asks.pkl", 'wb') as f:
#         pkl.dump(asks_avg_price, f)
#     f.close()

