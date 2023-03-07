import numpy as np
from sympy import symbols
from database.db import DataBase
from itertools import permutations

# symbol,asks_price,bids_price, asks_amount,bids_amount,timestamp


class ArbitrageManager:
    def __init__(self) -> None:
        self.db = DataBase()

    def get_all_tables(self):
        exchanges = self.db.get_all_tables()
        return [x[0] for x in exchanges]

    def make_exchange_pairs(self):
        exchanges = self.get_all_tables()
        perm = list(permutations(exchanges, 2))
        return perm

    def get_common_pairs(self, ex1: str, ex2: str):
        ex1_symbols = np.array(self.db.get_arb_info(db_name=ex1))[:, 0]
        ex2_symbols = np.array(self.db.get_arb_info(db_name=ex2))[:, 0]
        return list(set(ex1_symbols) & set(ex2_symbols))
    
    def calc_profit(self,bids,asks,bids_amount, asks_amount):
        value = np.minimum(bids_amount,asks_amount)
        profit = bids * value - asks * value 
        return profit



    def get_profit(self, ex1, ex2):
        symbols = self.get_common_pairs(ex1,ex2)
        ex1_data = np.array(self.db.get_symbols_data(ex1, symbols))
        ex2_data = np.array(self.db.get_symbols_data(ex2, symbols))

        # Asks avg price
        asks_price_ex1 = ex1_data[:, 1]
        asks_price_ex2 = ex2_data[:, 1]

        # Bids avg price
        bids_price_ex1 = ex1_data[:, 2]
        bids_price_ex2 = ex2_data[:, 2]

        # Asks amount
        asks_amount_ex1 = ex1_data[:, 3]
        asks_amount_ex2 = ex2_data[:, 3]

        # Bids amount
        bids_amount_ex1 = ex1_data[:, 4]
        bids_amount_ex2 = ex2_data[:, 4]

        profit = self.calc_profit(bids_price_ex1,asks_price_ex2,bids_amount_ex1,asks_amount_ex2)
        return np.array(symbols),profit
    
    def main(self):
        exs = self.make_exchange_pairs()
        for pair in exs:
            ex1,ex2 = pair
            symbols,profit = self.get_profit(ex1,ex2)
            if len(profit[profit>0]) > 0:
                profitable = profit[profit>0]
                for i in range(profitable):
                    print(f"BUY: {ex1} SELL: {ex2} PAIR: {symbols[i]} PROFIT: {profit[i]}")

            
            

arb = ArbitrageManager()
print(arb.make_exchange_pairs())
print(arb.get_profit("binance", 'bybit'))
arb.main()
# binance_info = np.array(self.db.get_arb_info(db_name='binance'))
# bybit_info = np.array(self.db.get_arb_info(db_name='bybit'))


# bin_d = np.array(db.get_symbols_data('binance',list(set(binance_info[:,0])&set(bybit_info[:,0]))))
# by_d = np.array(db.get_symbols_data('bybit',list(set(binance_info[:,0])&set(bybit_info[:,0]))))
# minimum_amount_binance_bybit = np.minimum(bin_d[:,4],by_d[:,3])
# arb_binance_bybit = bin_d[:,2] * minimum_amount_binance_bybit - by_d[:,1] * minimum_amount_binance_bybit
# arb_bybit_binance = by_d[:,2] * minimum_amount_binance_bybit - bin_d[:,1] * minimum_amount_binance_bybit
# for i in range(len(arb_binance_bybit)):
#     print(f"Bybit-Binance: {bin_d[:,0][i]} - {round(by_d[:,2][i],6)} - {round(bin_d[:,1][i],6)}: {arb_binance_bybit[i]} {minimum_amount_binance_bybit[i]}")
#     print(f"Binance-Bybit: {bin_d[:,0][i]} - {round(bin_d[:,2][i],6)} - {round(by_d[:,1][i],6)}: {arb_bybit_binance[i]} {minimum_amount_binance_bybit[i]}")
