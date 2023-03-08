import code
import psycopg2
from psycopg2.extras import DictCursor
import datetime
from config import config
from decimal import Decimal
DB = config.DB


class DataBase:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=DB.host,
            database=DB.dbname,
            user=DB.user,
            password=DB.password
        )
        keepalive_kwargs = {
    "keepalives": 300,
    "keepalives_idle": 30,
    "keepalives_interval": 300,
    "keepalives_count": 300,
}

        self.cursor = self.conn.cursor(cursor_factory=DictCursor,**keepalive_kwargs)
        self.conn.autocommit = True

    def init_snapshot(self, db_name, symbol, asks_price, bids_price, asks_amount, bids_amount, timestamp):
        q = f"INSERT INTO {db_name} (symbol,asks_price,bids_price, asks_amount,bids_amount,timestamp) VALUES ('{symbol}',{Decimal(asks_price)}, {Decimal(bids_price)},{Decimal(asks_amount)},{Decimal(bids_amount)}, {timestamp}) "
        try:
            self.cursor.execute(q)
            self.conn.commit()
        except psycopg2.InterfaceError as exc:
            self.conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password)
            keepalive_kwargs = {
                "keepalives": 300,
                "keepalives_idle": 30,
                "keepalives_interval": 300,
                "keepalives_count": 300,
            }

            self.cursor = self.conn.cursor(cursor_factory=DictCursor,**keepalive_kwargs)
            self.conn.autocommit = True
            self.cursor.execute(q)
            self.conn.commit()

    def update_db(self, db_name, symbol, asks_price, bids_price, asks_amount, bids_amount, timestamp):
        q = f"UPDATE {db_name} SET asks_price = {asks_price},bids_price = {Decimal(bids_price)},asks_amount = {Decimal(asks_amount)},bids_amount = {Decimal(bids_amount)}, timestamp = {timestamp} WHERE symbol = '{symbol}'"
        try:
            self.cursor.execute(q)
            self.conn.commit()
        except psycopg2.InterfaceError as exc:
            self.conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password)
            keepalive_kwargs = {
                "keepalives": 300,
                "keepalives_idle": 30,
                "keepalives_interval": 300,
                "keepalives_count": 300,
            }

            self.cursor = self.conn.cursor(cursor_factory=DictCursor,**keepalive_kwargs)
            self.conn.autocommit = True
            self.cursor.execute(q)
            self.conn.commit()

    def get_from_db(self, db_name, symbol):
        q = f"SELECT asks_price,bids_price, asks_amount,bids_amount,timestamp from {db_name} WHERE symbol = '{symbol}' ORDER BY symbol"
        try:
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            return data
        except psycopg2.InterfaceError as exc:
            self.conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password)
            keepalive_kwargs = {
                "keepalives": 300,
                "keepalives_idle": 30,
                "keepalives_interval": 300,
                "keepalives_count": 300,
            }

            self.cursor = self.conn.cursor(cursor_factory=DictCursor,**keepalive_kwargs)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            return data

    def get_arb_info(self, db_name):
        q = f"SELECT symbol,asks_price,bids_price, asks_amount,bids_amount,timestamp from {db_name} ORDER BY symbol"
        try:
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            return data
        except psycopg2.InterfaceError as exc:
            self.conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password)
            keepalive_kwargs = {
                "keepalives": 300,
                "keepalives_idle": 30,
                "keepalives_interval": 300,
                "keepalives_count": 300,
            }

            self.cursor = self.conn.cursor(cursor_factory=DictCursor,**keepalive_kwargs)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            return data

    def get_symbols_data(self, db_name, symbols):
        q = f"""SELECT symbol,asks_price,bids_price, asks_amount,bids_amount,timestamp from {db_name} WHERE symbol in ({str(symbols)[1:-1]}) ORDER BY symbol"""
        try:
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            return data
        except psycopg2.InterfaceError as exc:
            self.conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password)
            keepalive_kwargs = {
                "keepalives": 300,
                "keepalives_idle": 30,
                "keepalives_interval": 300,
                "keepalives_count": 300,
            }

            self.cursor = self.conn.cursor(cursor_factory=DictCursor,**keepalive_kwargs)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            return data

    def get_all_tables(self):
        q = """
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' and table_catalog='cex_dex'
"""
        try:
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            return data
        except psycopg2.InterfaceError as exc:
            self.conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password)
            keepalive_kwargs = {
                "keepalives": 300,
                "keepalives_idle": 30,
                "keepalives_interval": 300,
                "keepalives_count": 300,
            }

            self.cursor = self.conn.cursor(cursor_factory=DictCursor,**keepalive_kwargs)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            return data
