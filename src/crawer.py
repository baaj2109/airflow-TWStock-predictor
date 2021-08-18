import requests
import time
import pymysql
import schedule

from database import StockDataBase

class Crawer:
    def __init__(self):
        self.try_max_time = 5
        self.stock_id = 2303 ## 聯電
        self.data = None
        self.db = StockDataBase()

    def get_stock_data(self, stock_code, date):
        try:
            r = requests.get(f'http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock_code}')
            data = r.json()
            if data.get("status", 200) == 200:
                title = data["fields"]  
                ## ['日期', '成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差', '成交筆數']
                self.data = data["data"]
                
        except requests.exceptions.RequestException as ex:
            if self.try_max_time < 0:
                print("request overtime")
                return False
            else:
                self.try_max_time -= 1
                self.get_stock_data(stock_code, date)
        finally:
            print("get data ")
            return True

    def start(self):
        now = int(time.time())
        date = time.localtime(now)
        dateStr = time.strftime("%Y-%m-%d %H:%M:%S", date)
        request_date = time.strftime("%Y%m%d", date)
        flag = True
        flag &= self.get_stock_data(2303, request_date)
        if flag:
            print("crawer done.")
            return True
        else:
            print("crawer failed.")
            return False

    def transform(self, data):
        '''
        covert 
        ['110/08/02', '340,901,871', '19,876,433,538', '59.30', '59.70', '56.80', '57.50', '-0.30', '120,660']
        to 
        [20210802, 340901871, 19876433538, 59.30, 59.70, 56.80, 57.50, -0.3, 120660]
        '''
        date = [x for x in data[0].split("/")]
        date[0] = str(1911 + int(date[0]))
        data[0] = int("".join([x for x in date]))

        data[1] = int(data[1].replace(",",""))
        data[2] = int(data[2].replace(",",""))
        data[3] = float(data[3])
        data[4] = float(data[4])
        data[5] = float(data[5])
        data[6] = float(data[6])
        data[7] = float(data[7])
        data[8] = int(data[8].replace(",",""))
        return data

    def save_data(self, stock_id, data_list):
        for data in data_list:
            self.db.insert(stock_id, data_list)
        print("save data into database complete.")

    def process(self,):
        if self.start() and self.data:
            transformed_data = [self.transform(d) for d in self.data]
            self.save_data(self.stock_id, transformed_data)            
        self.db.disconnect()
        print("process complete.")



def crawer_start():
    Crawer().process()

if __name__ == "__main__":
    # get_stock_data(2303, 20190202)
    crawer = Crawer()
    # crawer.start()
    crawer.process()

    # def job():
    #     crawer.start()
    # schedule.every(20).seconds.do(job)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)