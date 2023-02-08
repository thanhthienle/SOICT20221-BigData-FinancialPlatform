import json
import requests
import datetime
import numpy as np
from pytz import timezone
from util.config import config
from util.util import SYMBOL_LIST, convertChange, convertPrice, convertToDate, convertDate, convertSingle
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


def get_historical_data(symbol='AAPL'):
    response = requests.get('https://s.cafef.vn/Lich-su-giao-dich-{}-1.chn#data'.format(symbol))
    soup = BeautifulSoup(response.content, "html.parser")
    info = soup.find("tr", id="ContentPlaceHolder1_ctl03_rptData2_altitemTR_1")
    data = [{
        "symbol": symbol,
        "date": convertToDate(info.find("td", class_="Item_DateItem").text),
        "close": convertPrice(info.find_all("td", class_="Item_Price10")[1].text),
        "change": convertChange(info.find("td", class_="Item_ChangePrice").text),
        "volume": convertPrice(info.find_all("td", class_="Item_Price10")[2].text),
        "open": convertPrice(info.find_all("td", class_="Item_Price10")[5].text),
        "high": convertPrice(info.find_all("td", class_="Item_Price10")[6].text),
        "low": convertPrice(info.find_all("td", class_="Item_Price10")[7].text),
    }]
    return data

def check_trading_hour(data_time):
    if data_time.time() < datetime.time(9, 30):
        last_day = data_time - datetime.timedelta(days=1)
        data_time = datetime.datetime(last_day.year, last_day.month, last_day.day, 16, 0, 0)

    elif data_time.time() > datetime.time(16, 0):
        data_time = datetime.datetime(data_time.year, data_time.month, data_time.day, 16, 0, 0)
    return data_time


def get_tick_intraday_data(symbol='AAPL'):
    response = requests.get("https://s.cafef.vn/hose/{}-.chn".format(symbol))
    soup = BeautifulSoup(response.content, "html.parser")
    info = soup.find("div", class_="dlt-left")
    time = soup.find("div", class_="dltlu-time")
    price_detail = info.find("ul", class_="dtlu-price-detail")
    data = {
        "symbol": symbol,
        "date": convertDate(time.find_all("div")[1].text),
        "volume": convertSingle(info.find("div", class_="v2").text),
        "close": convertSingle(info.find("div", class_="dltlu-point").text),
        "ref": convertSingle(info.find("div", id="REF").text),
        "ceil": convertSingle(info.find("div", id="CE").text),
        "floor": convertSingle(info.find("div", id="FL").text),
        "open": convertSingle(price_detail.find_all("div", class_="right")[0].text),
        "high": convertSingle(price_detail.find_all("div", class_="right")[1].text),
        "low": convertSingle(price_detail.find_all("div", class_="right")[2].text),
    }

    return data


def get_news():
    response = requests.get("https://www.tinnhanhchungkhoan.vn/")
    soup = BeautifulSoup(response.content, "html.parser")
    news=[]
    news_rank_1 = soup.find("div", class_="rank-1")
    a_tag_rank_1 = news_rank_1.find("a")
    img_rank_1 = a_tag_rank_1.find("img")
    time = news_rank_1.find("time")
    news.append({
        "title": img_rank_1.get('alt'),
        "source": a_tag_rank_1.get('href'),
        "img": img_rank_1.get("src"),
        "time": time.get('data-time') + "000",
    })
    news_rank_2 = soup.find("div", class_="rank-2")
    articles_rank_2 = news_rank_2.find_all("article")
    # print(a_tag_rank_2)
    for article in articles_rank_2:
        a_tag = article.find("a")
        img_rank_2 = a_tag.find("img")
        time_rank_2 = article.find("time")
        if(img_rank_2):
            news.append({
                "title": img_rank_2.get('alt'),
                "source": a_tag.get('href'),
                "img": img_rank_2.get("src"),
                "time": time_rank_2.get('data-time') + "000"
            })
    return news

def kafka_producer_update_history(kafka_producer, symbols):
    for symbol in symbols: 
        value = get_historical_data(symbol)
        # transform ready-to-send data to bytes, record sending-time adjusted to the trading timezone
        kafka_producer.send(topic=config['topic_name3'], value=bytes(str(value), 'utf-8'))
        time.sleep(10)
        print("Sent {}'s historical data".format(symbol))

def kafka_producer_single(kafka_producer, symbols):
    """
    :param kafka_producer: (KafkaProducer) an instance of KafkaProducer with configuration written in config.py
    :param symbol: (str) symbol of the stock
    :param tick: (bool)
    :return: None
    
    """
    # get data
    for symbol in symbols: 
        value = get_tick_intraday_data(symbol)

        # transform ready-to-send data to bytes, record sending-time adjusted to the trading timezone
        kafka_producer.send(topic=config['topic_name2'], value=bytes(str(value), 'utf-8'))
        print("Sent {}'s tick data".format(symbol))

def kafka_producer_news(kafka_producer):
    news = get_news()
    # print(news)
    kafka_producer.send(topic=config['topic_name1'], value=bytes(str(news), 'utf-8'))
    print("Sent economy news ")


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
        # previous_close = close + random.randint(-100, 100)*0.01
        # change = close - previous_close
        # change_percent = (close - previous_close)/previous_close * 100
        # value = {"symbol": symbol,
        #         # "time": int(datetime.datetime.now(timezone(TIME_ZONE)).timestamp()*1000),
        #         "open": close + random.randint(-100, 100)*0.01,
        #         "high": close + random.randint(0, 100)*0.01,
        #         "low": close + random.randint(-100, 0)*0.01,
        #         "close": close,
        #         "volume": int(random.choices(range(0,1000), k=1)[0]),
        #         "previous_close": previous_close,
        #         "change":  change,
        #         "change_percent": change_percent,
        #         # "last_trading_day": int(datetime.datetime.now(timezone(TIME_ZONE)).timestamp()*1000)}}
        # kafka_producer.send(topic=config['topic_name2'], value=bytes(str(value), 'utf-8'))
        # print("Sent {}'s fake data.".format(symbol[0]))
        # print(value)


if __name__ == "__main__":
    test_producer = KafkaProducer(bootstrap_servers=config['kafka_broker'])
    # kafka_producer(producer)

    # schedule to send data every minute
    if datetime.datetime.now().time() > datetime.time(15, 0, 0) or datetime.datetime.now(
            ).time() < datetime.time(9, 30, 0):
        schedule.every(60).seconds.do(kafka_producer_single, test_producer, SYMBOL_LIST)
    else:
        schedule.every(60).seconds.do(kafka_producer_fake, test_producer, SYMBOL_LIST)
    schedule.every(900).seconds.do(kafka_producer_news, test_producer)
    schedule.every().day.at("18:00").do(kafka_producer_update_history, test_producer, SYMBOL_LIST)
    while True:
        schedule.run_pending()
