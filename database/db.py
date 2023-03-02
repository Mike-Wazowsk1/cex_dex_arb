import code
import psycopg2
from psycopg2.extras import DictCursor
import datetime
from config import DB
from decimal import Decimal


class DataBase:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=DB.host,
            database=DB.dbname,
            user=DB.user,
            password=DB.password
        )
        self.cursor = self.conn.cursor(cursor_factory=DictCursor)
        self.conn.autocommit = True

    def init_snapshot(self, db_name, symbol, asks_price,bids_price, asks_amount,bids_amount, timestamp):
        self.cursor.execute(
            f"INSERT INTO {db_name} (symbol,asks_price,bids_price, asks_amount,bids_amount,timestamp) VALUES ('{symbol}',{Decimal(asks_price)}, {Decimal(bids_price)},{Decimal(asks_amount)},{Decimal(bids_amount)}, {timestamp}) ")
        self.conn.commit()

    def update_db(self, db_name, symbol, asks_price,bids_price, asks_amount,bids_amount, timestamp):
        self.cursor.execute(
            f"UPDATE {db_name} SET asks_price = {asks_price},bids_price = {Decimal(bids_price)},asks_amount = {Decimal(asks_amount)},bids_amount = {Decimal(bids_amount)}, timestamp = {timestamp} WHERE symbol = '{symbol}'")
        self.conn.commit()

    def get_from_db(self, db_name, symbol):
        self.cursor.execute(
            f"SELECT price,amount,timestamp from {db_name} WHERE symbol = {symbol}")
        data = self.cursor.fetchall()
        return data
