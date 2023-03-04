import numpy as np
from database.db import DataBase
db = DataBase()

# symbol = "XRPETH"

binance_info = np.array(db.get_arb_info(db_name='binance'))
bybit_info = np.array(db.get_arb_info(db_name='bybit'))
# gate_info = np.array(db.get_arb_info(db_name='gate'))

print(binance_info)
print(bybit_info)
# print(gate_info)

