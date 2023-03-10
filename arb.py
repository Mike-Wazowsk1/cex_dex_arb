import numpy as np
from sympy import symbols
from database.db import DataBase
from itertools import permutations

# symbol,asks_price,bids_price, asks_amount,bids_amount,timestamp

#ASK - RED
#BID - GREEN


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
        print(ex1,ex2)
        print(self.db.get_arb_info(db_name=ex1))
        ex1_symbols = np.array(self.db.get_arb_info(db_name=ex1))[:, 0]
        ex2_symbols = np.array(self.db.get_arb_info(db_name=ex2))[:, 0]
        return list(set(ex1_symbols) & set(ex2_symbols))

    def calc_profit(self, bids, asks, bids_amount, asks_amount, min_amount, max_amount):
        value = np.minimum.reduce(
            [bids_amount, asks_amount, np.full_like(asks_amount, max_amount)])
        profit = bids * value - asks * value
        return profit, value

    def get_profit(self, ex1, ex2, min_amount, max_amount):
        #symbol,asks_price,bids_price, asks_amount,bids_amount,count,timestamp
        symbols = self.get_common_pairs(ex1, ex2)
        ex1_data = np.array(self.db.get_symbols_data(ex1, symbols))
        ex2_data = np.array(self.db.get_symbols_data(ex2, symbols))

        # Symbols
        symbols = ex1_data[:, 0]

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

        # Counts
        count_ex1 = ex1_data[:, 5]
        count_ex2 = ex2_data[:, 5]

        profit, value = self.calc_profit(
            bids_price_ex2, asks_price_ex1, bids_amount_ex2, asks_amount_ex1, min_amount, max_amount)
        return symbols, profit, value, bids_price_ex2, asks_price_ex1, count_ex1, count_ex2

    def main(self, min_amount, max_amount, profit_con):
        exs = self.make_exchange_pairs()
        opps = []
        for pair in exs:
            ex1, ex2 = pair
            symbols, profit, value, bids, asks, count1, count2 = self.get_profit(
                ex1, ex2, min_amount, max_amount)
            for i in range(len(profit)):
                if profit[i] > profit_con and value[i] > min_amount:
                    opps.append([symbols[i], ex1, ex2, asks[i],
                                bids[i], value[i], count1[i], count2[i]])
        return opps


# arb = ArbitrageManager()
# print(arb.main())
