import numpy as np
from database.db import DataBase
db = DataBase()

# symbol = "XRPETH"

binance_info = np.array(db.get_arb_info(db_name='binance'))
bybit_info = np.array(db.get_arb_info(db_name='bybit'))
# gate_info = np.array(db.get_arb_info(db_name='gate'))

# print(binance_info[:,0])
# print(bybit_info[:,0])
# symbol [:,0]
# asks_price [:,1]
# bids_price [:,2]
# asks_amount [:,3]
# bids_amount [:,4]
# timestamp [:,5]
# Bid - max buy price
# Ask min sell price

print(set(binance_info[:,0])&set(bybit_info[:,0]))
bin_d = np.array(db.get_symbols_data('binance',list(set(binance_info[:,0])&set(bybit_info[:,0]))))
by_d = np.array(db.get_symbols_data('bybit',list(set(binance_info[:,0])&set(bybit_info[:,0]))))
print(len(bin_d),len(by_d))
minimum_amount_binance_bybit = np.minimum(bin_d[:,4],by_d[:,3])
arb_binance_bybit = bin_d[:,2] * minimum_amount_binance_bybit - by_d[:,1] * minimum_amount_binance_bybit
arb_bybit_binance = by_d[:,2] * minimum_amount_binance_bybit - bin_d[:,1] * minimum_amount_binance_bybit 
for i in range(len(arb_binance_bybit)):
    print(f"Bybit-Binance: {bin_d[:,0][i]} - {round(by_d[:,2][i],6)} - {round(bin_d[:,1][i],6)}: {arb_binance_bybit[i]} {minimum_amount_binance_bybit[i]}")
    print(f"Binance-Bybit: {bin_d[:,0][i]} - {round(bin_d[:,2][i],6)} - {round(by_d[:,1][i],6)}: {arb_bybit_binance[i]} {minimum_amount_binance_bybit[i]}")

# amount_1 = np.min(bin_d[:,3],by_d[:,4][:15])
# amount_2 = np.min(bin_d[:,4],by_d[:,3][:15])
# print(amount_1,amount_2)



# print(gate_info)

 