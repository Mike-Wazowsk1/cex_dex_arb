import code
import psycopg2
from psycopg2.extras import DictCursor
import datetime
from config import config
from decimal import Decimal
DB = config.DB


class DataBase:
    def __init__(self):
        pass

    def init_snapshot(self, db_name, symbol, asks_price, bids_price, asks_amount, bids_amount, timestamp):
        q = f"INSERT INTO {db_name} (symbol,asks_price,bids_price, asks_amount,bids_amount,timestamp) VALUES ('{symbol}',{Decimal(asks_price)}, {Decimal(bids_price)},{Decimal(asks_amount)},{Decimal(bids_amount)}, {timestamp})"
        try:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            self.conn.commit()
            self.cursor.close()
            self.conn.close()
        except:
            self.cursor.close()
            self.conn.close()
            self.conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password, )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            self.conn.commit()

    def update_db(self, db_name, symbol, asks_price, bids_price, asks_amount, bids_amount, timestamp):
        q = f"UPDATE {db_name} SET asks_price = {asks_price},bids_price = {Decimal(bids_price)},asks_amount = {Decimal(asks_amount)},bids_amount = {Decimal(bids_amount)}, timestamp = {timestamp} WHERE symbol = '{symbol}'"
        try:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            self.conn.commit()
            self.cursor.close()
            self.conn.close()
        except:
            self.cursor.close()
            self.conn.close()
            self.conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password, )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            self.conn.commit()

    def get_from_db(self, db_name, symbol):
        q = f"SELECT asks_price,bids_price, asks_amount,bids_amount,timestamp from {db_name} WHERE symbol = '{symbol}' ORDER BY symbol"
        try:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)

            data = self.cursor.fetchall()
            self.cursor.close()
            self.conn.close()
            return data
        except:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)

            data = self.cursor.fetchall()
            self.cursor.close()
            self.conn.close()
            return data

    def get_arb_info(self, db_name):
        q = f"SELECT symbol,asks_price,bids_price, asks_amount,bids_amount,timestamp from {db_name} ORDER BY symbol"
        try:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            self.cursor.close()
            self.conn.close()
            return data
        except:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            self.cursor.close()
            self.conn.close()
            return data

    def get_symbols_data(self, db_name, symbols):
        q = f"""SELECT symbol,asks_price,bids_price, asks_amount,bids_amount,timestamp from {db_name} WHERE symbol in ({str(symbols)[1:-1]}) ORDER BY symbol"""
        try:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            self.cursor.close()
            self.conn.close()
            return data
        except:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            self.cursor.close()
            self.conn.close()
            return data

    def get_all_tables(self):
        q = """
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' and table_catalog='cex_dex' and table_name != 'info'
"""
        try:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            self.cursor.close()
            self.conn.close()
            return data
        except:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            self.cursor.close()
            self.conn.close()
            return data
        
    def del_from_db(self,db_name,symbol):
        q = f"DELETE FROM {db_name} WHERE symbol = '{symbol}'"
        try:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            self.conn.commit()
            self.cursor.close()
            self.conn.close()
        except:
            self.cursor.close()
            self.conn.close()
            self.conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password, )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            self.conn.commit()


    def get_info(self):
        q = f"""SELECT min_profit,min_amount,max_amount FROM info"""
        try:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            self.cursor.close()
            self.conn.close()
            return data
        except:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            self.cursor.close()
            self.conn.close()
            return data
        
    def get_info_col(self,col):
        q = f"""SELECT {col} FROM info"""
        try:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            self.cursor.close()
            self.conn.close()
            print(data[0])
            return Decimal(data[0])
        except:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            self.cursor.close()
            self.conn.close()
            return Decimal(data[0])
        

    def update_info(self, col, val):
        q = f"UPDATE info SET {col} = {val}"
        try:
            self.conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            self.conn.commit()
            self.cursor.close()
            self.conn.close()
        except:
            self.cursor.close()
            self.conn.close()
            self.conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password, )

            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            self.conn.autocommit = True
            self.cursor.execute(q)
            self.conn.commit()
        