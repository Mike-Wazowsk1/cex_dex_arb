import psycopg2
from psycopg2.extras import DictCursor
from config import config
DB = config.DB


class DataBase:
    def __init__(self):
        pass

    cdef init_snapshot(self, db_name, symbol, asks_price, bids_price, asks_amount, bids_amount, count, timestamp):
        q = f"INSERT INTO {db_name} (symbol,asks_price,bids_price, asks_amount,bids_amount,count,timestamp) VALUES ('{symbol}',{asks_price}, {bids_price},{asks_amount},{bids_amount},{count}, {timestamp})"
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

    cdef update_db(self, db_name, symbol, asks_price, bids_price, asks_amount, bids_amount, count, timestamp):
        q = f"UPDATE {db_name} SET asks_price = {asks_price},bids_price = {bids_price},asks_amount = {asks_amount},bids_amount = {bids_amount}, timestamp = {timestamp},count={count} WHERE symbol = '{symbol}'"
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

    cdef get_from_db(self, db_name, symbol):
        q = f"SELECT asks_price,bids_price, asks_amount,bids_amount,count,timestamp from {db_name} WHERE symbol = '{symbol}' ORDER BY symbol"
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

    cdef get_arb_info(self, db_name):
        q = f"SELECT symbol,asks_price,bids_price, asks_amount,bids_amount,count,timestamp from {db_name} ORDER BY symbol"
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

    cdef get_symbols_data(self, db_name, symbols):
        q = f"""SELECT symbol,asks_price,bids_price, asks_amount,bids_amount,count,timestamp from {db_name} WHERE symbol in ({str(symbols)[1:-1]}) ORDER BY symbol"""
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

    cdef get_all_tables(self):
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

    cdef del_from_db(self, db_name, symbol):
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

    cdef get_info(self):
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

    cdef get_info_col(self, col):
        q = f"""SELECT {col} FROM info"""
        self.conn = psycopg2.connect(
            host=DB.host,
            database=DB.dbname,
            user=DB.user,
            password=DB.password)

        self.cursor = self.conn.cursor(cursor_factory=DictCursor)
        self.conn.autocommit = True
        self.cursor.execute(q)
        data = self.cursor.fetchall()
        self.cursor.close()
        self.conn.close()
        return data[0][0]
  

    cdef update_info(self, col, val):
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
            self.cursor.close()
            self.conn.close()

    cdef get_command(self, symbol, db_name):
        q = f"select symbol, asks_price as ask,bids_price as bid,timestamp,count from {db_name} where timestamp!=0 and symbol = '{symbol}' order by symbol"
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
