import psycopg2
from psycopg2.extras import DictCursor
from config import config
DB = config.DB
print("Imported C_DB new suka")

cdef class DataBase:
    def __init__(self):
        pass

    cpdef init_snapshot(self, db_name, symbol, asks_price, bids_price, asks_amount, bids_amount, count, timestamp):
        cdef q = f"INSERT INTO {db_name} (symbol,asks_price,bids_price, asks_amount,bids_amount,count,timestamp) VALUES ('{symbol}',{asks_price}, {bids_price},{asks_amount},{bids_amount},{count}, {timestamp})"
        try:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            conn.commit()
            cursor.close()
            conn.close()
        except:
            conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password, )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            conn.commit()

    cpdef update_db(self, db_name, symbol, asks_price, bids_price, asks_amount, bids_amount, count, timestamp):
        cdef q = f"UPDATE {db_name} SET asks_price = {asks_price},bids_price = {bids_price},asks_amount = {asks_amount},bids_amount = {bids_amount}, timestamp = {timestamp},count={count} WHERE symbol = '{symbol}'"
        try:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            conn.commit()
            cursor.close()
            conn.close()
        except:
            conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password, )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            conn.commit()

    cpdef get_from_db(self, db_name, symbol):
        cdef q = f"SELECT asks_price,bids_price, asks_amount,bids_amount,count,timestamp from {db_name} WHERE symbol = '{symbol}' ORDER BY symbol"
        try:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)

            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return data
        except:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)

            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return data

    cpdef get_arb_info(self, db_name):
        cdef q = f"SELECT symbol,asks_price,bids_price, asks_amount,bids_amount,count,timestamp from {db_name} ORDER BY symbol"
        try:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return data
        except:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return data

    cpdef get_symbols_data(self, db_name, symbols):
        cdef q = f"""SELECT symbol,asks_price,bids_price, asks_amount,bids_amount,count,timestamp from {db_name} WHERE symbol in ({str(symbols)[1:-1]}) ORDER BY symbol"""
        try:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return data
        except:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return data

    cpdef get_all_tables(self):
        cdef q = """
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' and table_catalog='cex_dex' and table_name != 'info'
"""
        try:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return data
        except:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return data

    cpdef del_from_db(self, db_name, symbol):
        cdef q = f"DELETE FROM {db_name} WHERE symbol = '{symbol}'"
        try:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            conn.commit()
            cursor.close()
            conn.close()
        except:
            conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password, )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            conn.commit()

    cpdef get_info(self):
        cdef q = f"""SELECT min_profit,min_amount,max_amount FROM info"""
        try:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return data
        except:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return data

    cpdef get_info_col(self, col):
        cdef q = f"""SELECT {col} FROM info"""
        conn = psycopg2.connect(
            host=DB.host,
            database=DB.dbname,
            user=DB.user,
            password=DB.password)

        cursor = conn.cursor(cursor_factory=DictCursor)
        conn.autocommit = True
        cursor.execute(q)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data[0][0]
  

    cpdef update_info(self, col, val):
        cdef q = f"UPDATE info SET {col} = {val}"
        try:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            conn.commit()
            cursor.close()
            conn.close()
        except:
            conn = psycopg2.connect(host=DB.host,
                                         database=DB.dbname,
                                         user=DB.user,
                                         password=DB.password, )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            conn.commit()
            cursor.close()
            conn.close()

    cpdef get_command(self, symbol, db_name):
        cdef q = f"select symbol, asks_price as ask,bids_price as bid,timestamp,count from {db_name} where timestamp!=0 and symbol = '{symbol}' order by symbol"
        try:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )

            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return data
        except:
            conn = psycopg2.connect(
                host=DB.host,
                database=DB.dbname,
                user=DB.user,
                password=DB.password,

            )
            cursor = conn.cursor(cursor_factory=DictCursor)
            conn.autocommit = True
            cursor.execute(q)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return data
