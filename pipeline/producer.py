import json
import requests
import datetime
import numpy as np
from pytz import timezone
from util.config import config
from util.util import TIME_ZONE, SYMBOL_LIST
from kafka import KafkaProducer
from multiprocessing import Pool
from itertools import repeat
import schedule
from bs4 import BeautifulSoup
import random


# =============================================================================
# Step 1: run zookeeper_starter.sh to start zookeeper
# Step 2: run kafka_starter.sh to start Kafka
# Step 3: run cassandra_starter.sh to start Cassandra
# Step 4: run producer.py to start sending data through Kafka
# =============================================================================


# logging.basicConfig(level=logging.DEBUG)


def get_historical_data(symbol='AAPL', outputsize='full'):
    """
    :param symbol: (str) to know which company's historical data we are getting
    :param outputsize: (str) default to 'full' to get 20 years historical data;
                                        'compact' to get the most recent 100 days' historical data
    :return: (dict) latest minute's stock price information 
        e.g.:
            {"symbol": 'AAPL',
             "time"  : '2019-07-26 16:00:00',
             'open'  : 207.98,
             'high'  : 208.0,
             'low'   : 207.74,
             'close' : 207.75,
             'adjusted_close': 207.74,
             'volume': 354454.0,
             'dividend_amount': 0.0,
             'split_coefficient': 1.0
            }
    
    """

    # get data using AlphaVantage's API

    url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={}" \
          "&outputsize={}&interval=1min&apikey={}" \
        .format(symbol, outputsize, config['api_key'])

    req = requests.get(url)
    if req.status_code == 200:
        raw_data = json.loads(req.content)
        try:
            price = raw_data['Time Series (Daily)']

        except KeyError:
            print(raw_data)
            price = None
            exit()

        rename = {
            'symbol': 'symbol',
            'time': 'time',
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. adjusted close': 'adjusted_close',
            '6. volume': 'volume',
            '7. dividend amount': 'dividend_amount',
            '8. split coefficient': 'split_coefficient'
        }

        for k, v in price.items():
            v.update({
                'symbol': symbol,
                'time': k
            })
        price = dict((key, dict((rename[k], v) for (k, v) in value.items())) for (key, value) in price.items())
        price = list(price.values())
        print("Get {}/'s historical data today.".format(symbol))

        return price

def check_trading_hour(data_time):
    if data_time.time() < datetime.time(9, 30):
        last_day = data_time - datetime.timedelta(days=1)
        data_time = datetime.datetime(last_day.year, last_day.month, last_day.day, 16, 0, 0)

    elif data_time.time() > datetime.time(16, 0):
        data_time = datetime.datetime(data_time.year, data_time.month, data_time.day, 16, 0, 0)
    return data_time


def get_tick_intraday_data(symbol='AAPL'):
    # get data using AlphaVantage's API
    time_zone = TIME_ZONE
    url = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={}&apikey={}"\
        .format(symbol, config['api_key2'])
    req = requests.get(url)
    data_time = datetime.datetime.now(timezone(time_zone))
    data_time = check_trading_hour(data_time)
    print(url)
    # if request success
    if req.status_code == 200:
        raw_data = json.loads(req.content)
        try:
            price = raw_data['Global Quote']

        except KeyError:
            print("Raw data")
            price = None
            exit()
        print(price)
        # organize data to dict
        value = {"symbol": symbol,
                 "time": str(data_time)[:19],
                 "open": price['02. open'],
                 "high": price['03. high'],
                 "low": price['04. low'],
                 "close": price['05. price'],
                 "volume": price['06. volume'],
                 "last_trading_day": price['07. latest trading day'],
                 "previous_close": price['08. previous close'],
                 "change": price['09. change'],
                 "change_percent": price['10. change percent']}

        print('Get {}\'s latest tick data at {}'.format(symbol, data_time))

    # if request failed, return a fake data point
    else:
        print('Failed: Cannot get {}\'s data at {}'.format(symbol, data_time))
        value = {"symbol": symbol,
                 "time": str(data_time)[:19],
                 "open": 0.,
                 "high": 0.,
                 "low": 0.,
                 "close": 0.,
                 "volume": 0.,
                 "last_trading_day": '',
                 "previous_close": 0.,
                 "change": 0.,
                 "change_percent": 0.}
    return value, time_zone


def get_news():
    response = requests.get("https://www.tinnhanhchungkhoan.vn/")
    soup = BeautifulSoup(response.content, "html.parser")
    news=[]
    news_rank_1 = soup.find("div", class_="rank-1")
    a_tag_rank_1 = news_rank_1.find("a")
    img_rank_1 = a_tag_rank_1.find("img")
    news.append({
      "title": img_rank_1.get('alt'),
      "source": a_tag_rank_1.get('href'),
      "img": img_rank_1.get("src")
    })
    news_rank_2 = soup.find("div", class_="rank-2")
    a_tag_rank_2 = news_rank_2.find_all("a")
    # print(a_tag_rank_2)
    for a_tag in a_tag_rank_2:
      img_rank_2 = a_tag.find("img")
      if(img_rank_2):
        news.append({
          "title": img_rank_2.get('alt'),
          "source": a_tag.get('href'),
          "img": img_rank_2.get("src")
        })
    return news


def kafka_producer_single(kafka_producer, symbols):
    """
    :param kafka_producer: (KafkaProducer) an instance of KafkaProducer with configuration written in config.py
    :param symbol: (str) symbol of the stock
    :param tick: (bool)
    :return: None
    
    """
    # get data
    for symbol in symbols: 
        value, time_zone = get_tick_intraday_data(symbol)

        now_timezone = datetime.datetime.now(timezone(time_zone))
        # transform ready-to-send data to bytes, record sending-time adjusted to the trading timezone
        kafka_producer.send(topic=config['topic_name2'], value=bytes(str(value), 'utf-8'))
        print("Sent {}'s tick data at {}".format(symbol, now_timezone))


# def kafka_producer_all(kafka_producer, symbols=SYMBOL_LIST, tick=False, fake=False):
#     if not fake:
#         with Pool(len(symbols)) as pool:
#             pool.starmap(kafka_producer_single, zip(repeat(kafka_producer), symbols, repeat(tick)))
#     else:
#         with Pool(len(symbols)) as pool:
#             pool.starmap(kafka_producer_fake, zip(repeat(kafka_producer), symbols, repeat(False)))


def kafka_producer_news(kafka_producer):
    news = get_news()
    # print(news)
    now_timezone = datetime.datetime.now(timezone(TIME_ZONE))
    kafka_producer.send(topic='news', value=bytes(str(news), 'utf-8'))
    print("Sent economy news : {}".format(now_timezone))


def kafka_producer_fake(kafka_producer, symbols):
    """
    send fake data to test visualization
    :param kafka_producer: (KafkaProducer) an instance of KafkaProducer with configuration written in config.py
    :param symbol: (str)
    :return: None
    """
    for symbol in symbols:
        close = 2
        close = close + random.randint(-100, 100)*0.01
        previous_close = close + random.randint(-100, 100)*0.01
        change = close - previous_close
        change_percent = (close - previous_close)/previous_close * 100
        value = {"symbol": symbol,
                "time": int(datetime.datetime.now(timezone(TIME_ZONE)).timestamp()*1000),
                "open": close + random.randint(-100, 100)*0.01,
                "high": close + random.randint(0, 100)*0.01,
                "low": close + random.randint(-100, 0)*0.01,
                "close": close,
                "volume": int(random.choices(range(0,1000), k=1)[0]),
                "previous_close": previous_close,
                "change":  change,
                "change_percent": change_percent,
                "last_trading_day": int(datetime.datetime.now(timezone(TIME_ZONE)).timestamp()*1000)}
        kafka_producer.send(topic=config['topic_name2'], value=bytes(str(value), 'utf-8'))
        print("Sent {}'s fake data.".format(symbol[0]))
        print(value)


if __name__ == "__main__":
    test_producer = KafkaProducer(bootstrap_servers=config['kafka_broker'])
    # kafka_producer(producer)

    # schedule to send data every minute
    if datetime.datetime.now(timezone(TIME_ZONE)).time() > datetime.time(16, 0, 0) or datetime.datetime.now(
            timezone(TIME_ZONE)).time() < datetime.time(9, 30, 0):
        schedule.every(60).seconds.do(kafka_producer_single, test_producer, SYMBOL_LIST)
    else:
        schedule.every(60).seconds.do(kafka_producer_fake, test_producer, SYMBOL_LIST)
    schedule.every(900).seconds.do(kafka_producer_news, test_producer)
    while True:
        schedule.run_pending()
