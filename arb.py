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
        profit = asks * value - bids * value 
        return profit, value



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

        profit, value = self.calc_profit(bids_price_ex2,asks_price_ex1,bids_amount_ex2,asks_amount_ex1)
        return np.array(symbols),profit,value,bids_price_ex2,asks_price_ex1
    
    def main(self):
        exs = self.make_exchange_pairs()
        opps = []
        for pair in exs:
            ex1,ex2 = pair
            symbols,profit,value,bids,asks = self.get_profit(ex1,ex2)
            for i in range(len(profit)):
                if profit[i] > 0:
                    opps.append([symbols[i],ex1,ex2,asks[i],bids[i],value[i],profit[i]])
        return opps

            
            

# arb = ArbitrageManager()
# print(arb.main())
