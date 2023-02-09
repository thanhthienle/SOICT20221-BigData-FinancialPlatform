import ast
import time
from util.util import string_to_float, convertDate
from util.util import SYMBOL_LIST
from util.config import config
from kafka import KafkaConsumer
from cassandra.cluster import Cluster, NoHostAvailable
from producer import get_historical_data
import datetime


# =============================================================================
# Step 1: run zookeeper_starter.sh to start zookeeper
# Step 2: run kafka_starter.sh to start Kafka
# Step 3: run cassandra_starter.sh to start Cassandra
# Step 4: run producer.py to start sending data through Kafka
# =============================================================================


class CassandraStorage(object):
    """
    Kafka consumer reads the message and store the received data in Cassandra database
    
    """

    def __init__(self):
        # Run the kafka consumers
        self.consumer1 = None
        self.consumer2 = None
        self.consumer3 = None
        self.kafka_consumer()
        self.key_space = config['key_space']

        # init a Cassandra cluster instance
        cluster = Cluster()

        # start Cassandra server before connecting       
        try:
            self.session = cluster.connect()
        except NoHostAvailable:
            print("Fatal Error: need to connect Cassandra server")
        else:
            self.create_table()

    def create_table(self):
        """
        create Cassandra table of stock if not exist
        :return: None
        
        """
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = "
                             "{'class': 'SimpleStrategy', 'replication_factor': '3'} "
                             "AND durable_writes = 'true'" % config['key_space'])
        self.session.set_keyspace(self.key_space)

        # create table for historical data
        self.session.execute(
            """CREATE TABLE IF NOT EXISTS HISTORICAL (
                TIME date,
                SYMBOl text,
                OPEN float,
                HIGH float,
                LOW float,
                CLOSE float,
                VOLUME float,
                CHANGE float,
                RSI float,
                EMA float,
                PRIMARY KEY (SYMBOL, TIME)
                );""")

        # create table for tick data
        self.session.execute(
            """CREATE TABLE IF NOT EXISTS TICK2 (
            DATE timestamp,
            SYMBOL text,
            OPEN float,
            HIGH float,
            LOW float,
            CLOSE float,
            VOLUME float,
            REF float, 
            CEIL float, 
            FLOOR float,
            CHANGE float,
            PRIMARY KEY (SYMBOL, DATE)
            );""")
        
        self.session.execute(
            """CREATE TABLE IF NOT EXISTS FAKE (
            DATE timestamp,
            SYMBOL text,
            OPEN float,
            HIGH float,
            LOW float,
            CLOSE float,
            VOLUME float,
            REF float, 
            CEIL float, 
            FLOOR float,
            CHANGE float,
            PRIMARY KEY (SYMBOL, DATE),
            );""")

        # create table for news
        self.session.execute(
            """CREATE TABLE IF NOT EXISTS NEWS (
            TIME timestamp,
            TITLE text,
            SOURCE text,
            IMG text,
            PRIMARY KEY (TITLE, TIME)
            )""")

    def kafka_consumer(self):
        self.consumer1 = KafkaConsumer(
            config['topic_name1'],
            bootstrap_servers=config['kafka_broker'])
        self.consumer2 = KafkaConsumer(
            config['topic_name2'],
            bootstrap_servers=config['kafka_broker'])
        self.consumer3 = KafkaConsumer(
            'news',
            bootstrap_servers=config['kafka_broker'])

    def historical_to_cassandra(self, data):
        for dict_data in price:
            for key in ['open', 'high', 'low', 'close', 'volume', 'adjusted_close', 'dividend_amount',
                        'split_coefficient']:
                dict_data[key] = string_to_float(dict_data[key])
            query = "INSERT INTO HISTORICAL (time, symbol, open, high, low, close, adjusted_close, volume, " \
                    "dividend_amount, split_coefficient) VALUES ('{}','{}', {}, {}, {}, {}, {}, {}, {}, {});" \
                .format(dict_data['time'], dict_data['symbol'],
                        dict_data['open'], dict_data['high'], dict_data['low'],
                        dict_data['close'], dict_data['adjusted_close'], dict_data['volume'],
                        dict_data['dividend_amount'], dict_data['split_coefficient'])
            self.session.execute(query)
            print("Stored {}\'s historical data at {}".format(dict_data['symbol'], dict_data['time']))

    def tick_stream_to_cassandra(self):
        for msg in self.consumer2:
            # decode msg value from byte to utf-8
            dict_data = ast.literal_eval(msg.value.decode("utf-8"))
            print(dict_data)
            # transform price data from string to float
            for key in ['volume', 'close', 'ref', 'ceil', 'floor', 'open', 'high', 'low','change']:
                dict_data[key] = dict_data[key].replace(",", "")
                dict_data[key] = string_to_float(dict_data[key])
            dict_data['date'] = datetime.datetime.utcfromtimestamp(dict_data['date'])
            # dict_data['change_percent'] = float(dict_data['change_percent'].strip('%')) / 100.
            #dict_data['change_percent'] = float(dict_data['change_percent']) / 100.
            query = "INSERT INTO TICK2 (SYMBOL, DATE, VOLUME, CLOSE, REF, CEIL, FLOOR, OPEN, HIGH, LOW, CHANGE)" \
                    "VALUES ('{}','{}', {}, {}, {}, {}, {}, {}, {}, {},{});" \
                .format( dict_data['symbol'],dict_data['date'],
                        dict_data['volume'], dict_data['close'], dict_data['ref'], dict_data['ceil'], dict_data['floor'],
                        dict_data['open'], dict_data['high'], dict_data['low'], dict_data['change'])
            self.session.execute(query)
            print("Stored {}\'s tick data at {}".format(dict_data['symbol'], dict_data['date']))

    def update_cassandra_after_trading_day(self):
        for symbol in SYMBOL_LIST[:]:
            value_daily = get_historical_data(symbol=symbol, outputsize='full')

            self.historical_to_cassandra(value_daily)
            time.sleep(15)

    def news_to_cassandra(self):
        for msg in self.consumer3:
            dict_data = ast.literal_eval(msg.value.decode("utf-8"))
            print(dict_data)
            # publishtime = dict_data['publishedAt'][:10] + ' ' + dict_data['publishedAt'][11:19]
            # try:
            #     dict_data['description'] = dict_data['description'].replace('\'', '@@')
            # except Exception as e:
            #     print(e)
            for data in dict_data:
                query = "INSERT INTO NEWS (time, title, source, img) " \
                "VALUES ('{}', '{}', '{}', '{}');" \
                    .format(data['time'], data['title'], data['source'], data['img'])
                self.session.execute(query)

            # print("Stored news '{}' at {}".format(dict_data['title'],dict_data['publishedAt']))

    def delete_table(self, table_name):
        self.session.execute("DROP TABLE {}".format(table_name))


def main_realtime():
    database = CassandraStorage()
    database.kafka_consumer()
    database.tick_stream_to_cassandra()

def main_realtime_news():
    database = CassandraStorage()
    database.kafka_consumer()
    database.news_to_cassandra()


def main_aftertradingday():
    for symbol in SYMBOL_LIST[:]:
        value_daily = get_historical_data(symbol=symbol)

        database = CassandraStorage()
        database.kafka_consumer()

        database.historical_to_cassandra(value_daily)
        time.sleep(15)


if __name__ == "__main__":
    # historical
    # main_aftertradingday()
    # tick
    main_realtime()
    main_realtime_news()