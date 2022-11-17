import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import schedule
import time


#Constants
DATATYPES = ('real time price', 'olhc', 'financial statement')
default_stocks = ('BID', 'CTG', 'MBB', 'VCB')


# Static methods
def realTimePrice(ticket):
    r = requests.get(f'https://finance.yahoo.com/quote/{ticket}?p={ticket}')
    soup = BeautifulSoup(r.text, 'lxml')
    price_content = soup.find('div', {'class': "D(ib) Mend(20px)"})
    price = float(soup.find('div', {'class': "D(ib) Mend(20px)"})\
                        .find('fin-streamer').text)
    return price


# Classes
class Scraper:
    
    def __init__(self, datatype: str = 'real time price', stocklist: list = default_stocks):
        assert datatype.lower() in DATATYPES, f'Scraping {datatype.lower()} is not yet included, please choose either "stock price" or "financial statement".'
        super().__init__()
        self.datatype = datatype.lower()
        self.stocklist = list(set(stocklist)) # In case there are duplicates in stocklist
        self.datatable = None
        self.initialized = True

    def scrapeRealTime(self):
        # assert period >= 5, f'Chill. We cannot execute that fast'
        prices = []
        pseudo_frame = []
        time_stamp = datetime.now().strftime("%Y-&m-%d %H:%M:%S")
        for ticket in self.stocklist:
            prices.append(realTimePrice(ticket))
        row = [ticket].append(time_stamp).extend(prices)
        self.datatable.append(row)

    def scrape(self):
        if self.datatype == 'real time price':
            schedule.every(5).seconds.do(self.scrapeRealTime)
        elif self.datatype == 'olhc':
            pass
        else:
            pass
