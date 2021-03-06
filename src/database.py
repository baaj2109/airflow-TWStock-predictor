import pymysql
from sqlalchemy import create_engine
import pandas as pd

class StockDataBase():

    def __init__(self):
        # self.conn = None
        self.conn = self.connect()

    def get_db_setting(self):
        return {
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "password",
            "db": "stockDB",
            "charset": "utf8"
        }

    def connect(self):
        try: 
            conn = pymysql.connect(**self.get_db_setting())
            # with conn.cursor() as cursor:
            self.init_database(conn)
            return conn
        except Exception as ex:
            print(ex)
            return None

    def is_database_connected(self):
        if self.conn == None:
            print("database not connected.")
            return False
        return True

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def init_database(self, conn):  
        with conn.cursor() as cursor:
            sql = """
                CREATE TABLE IF NOT EXISTS stock (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    stock_id INT NOT NULL,
                    date DATE NOT NULL,
                    number_shares INT NOT NULL,
                    total BIGINT(50) NOT NULL,
                    open FLOAT NOT NULL,
                    max FLOAT NOT NULL,
                    min FLOAT NOT NULL,
                    end FLOAT NOT NULL,
                    diff FLOAT NOT NULL,
                    number_trades INT NOT NULL,
                    predict_price FLOAT);
            """
            self.cursor.execute(sql)

    def insert(self, stock_id, data):
        if self.is_database_connected() == False: return  
        with self.conn.cursor() as cursor:
            query = """
                INSERT INTO stock(stock_id, date, number_shares, total, 
                                  open, max, min, end, 
                                  diff, number_trades, predict_price)
                SELECT * FROM ( 
                    SELECT %s AS stock_id, %s AS date, %s AS number_shares, 
                           %s AS total, %s AS open, %s AS max, 
                           %s AS min, %s AS end, %s AS diff, 
                           %s AS number_trades, null AS predict_price)  AS tmp
                WHERE NOT EXISTS (
                    SELECT stock_id, date 
                    FROM stock 
                    WHERE stock_id = %s AND date = %s
                ) LIMIT 1;
            """%(stock_id, data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], stock_id, data[0])
            
            try:
                # Execute the SQL command
                cursor.execute(query)
                # Commit your changes in the database
                self.conn.commit()
                # print("insert complete")
            except Exception as ex:
                # Rollback in case there is any error
                print(f"[database] failed to insert data, {ex}")
                self.conn.rollback()
                
    def update_predict_price(stock_id, datetime, predct_price):
        settings = self.get_db_setting()
        with self.conn.cursor() as cursor:
            query = """
                UPDATE stock
                SET predict_price = %s
                WHERE stock_id = %s and date = %s
            """.format(predict_price, stock_id, datetime)
            try:
                cursor.execute(query)
                self.conn.commit()
            except Exception as ex:
                print(f"[database] fail to update data, {ex}")
                self.conn.rollback()

    def read_db_to_dataframe(query: str):
        settings = self.get_db_setting()
        db_connection_str = f'mysql+pymysql://{settings['user']}:{settings['password']}@{settings['host']}/{settings['db']}'
        db_connection = create_engine(db_connection_str)
        return pd.read_sql(query, con = db_connection)

if __name__ == "__main__":
    db = StockDataBase()
    