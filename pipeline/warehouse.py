import ast
import time
from pipeline.util.util import string_to_float
from definitions import SYMBOL_LIST
from pipeline.util.config import config
from kafka import KafkaConsumer
from cassandra.cluster import Cluster, NoHostAvailable
from producer import get_intraday_data, get_historical_data


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
        # self.symbol = symbol.replace('^', '')
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

        # create table for intraday data
        self.session.execute(
            """CREATE TABLE IF NOT EXISTS INTRADAY (
            TIME timestamp,
            SYMBOL text,
            OPEN float,
            HIGH float,
            LOW float,              
            CLOSE float,             
            VOLUME float,
            PRIMARY KEY (SYMBOL, TIME)
            );""")

        # create table for historical data
        self.session.execute(
            """CREATE TABLE IF NOT EXISTS HISTORICAL (
            TIME timestamp,
            SYMBOL text,
            OPEN float,
            HIGH float,
            LOW float,
            CLOSE float,
            ADJUSTED_CLOSE float,
            VOLUME float,
            dividend_amount float,
            split_coefficient float,
            PRIMARY KEY (SYMBOL, TIME)
            );""")

        # create table for tick data
        self.session.execute(
            """CREATE TABLE IF NOT EXISTS TICK (
            TIME timestamp,
            SYMBOL text,
            OPEN float,
            HIGH float,
            LOW float,
            CLOSE float,
            VOLUME float,
            last_trading_day text,
            previous_close float,
            change float,
            change_percent float,
            PRIMARY KEY (SYMBOL, TIME)
            );""")

        # create table for news
        self.session.execute(
            """CREATE TABLE IF NOT EXISTS NEWS (
            DATE date,
            publishedAt timestamp,
            TITLE text,
            SOURCE text,
            description text,
            url text,
            PRIMARY KEY (DATE, publishedAt)
            ) WITH CLUSTERING ORDER BY (publishedAt ASC);""")

    def kafka_consumer(self):
        """
        initialize a Kafka consumer 
        :return: None
        
        """
        self.consumer1 = KafkaConsumer(
            config['topic_name1'],
            bootstrap_servers=config['kafka_broker'])
        self.consumer2 = KafkaConsumer(
            config['topic_name2'],
            bootstrap_servers=config['kafka_broker'])
        self.consumer3 = KafkaConsumer(
            'news',
            bootstrap_servers=config['kafka_broker'])

    def historical_to_cassandra(self, price, intraday=False):
        """
        store historical data to Cassandra database
            :primary key: time,symbol
        :return: None

        """
        if intraday is False:
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
        else:
            for dict_data in price:
                for key in ['open', 'high', 'low', 'close', 'volume']:
                    dict_data[key] = string_to_float(dict_data[key])

                query = "INSERT INTO INTRADAY (time, symbol, open, high, low, close, volume) " \
                        "VALUES ('{}', '{}', {}, {}, {}, {}, {});" \
                    .format(dict_data['time'], dict_data['symbol'],
                            dict_data['open'], dict_data['high'], dict_data['low'],
                            dict_data['close'], dict_data['volume'])

                self.session.execute(query)
                print("Stored {}\'s full length intraday data at {}".format(dict_data['symbol'], dict_data['time']))

    def stream_to_cassandra(self):
        """
        store streaming data of 1min frequency to Cassandra database
            :primary key: time,symbol
        :return: None
        
        """
        for msg in self.consumer1:
            # decode msg value from byte to utf-8
            dict_data = ast.literal_eval(msg.value.decode("utf-8"))

            # transform price data from string to float
            for key in ['open', 'high', 'low', 'close', 'volume']:
                dict_data[key] = string_to_float(dict_data[key])

            query = "INSERT INTO INTRADAY (time, symbol, open, high, low, close, volume) " \
                    "VALUES ('{}','{}', {}, {}, {}, {}, {});" \
                .format(dict_data['time'], dict_data['symbol'],
                        dict_data['open'], dict_data['high'], dict_data['low'], dict_data['close'],
                        dict_data['volume'])

            self.session.execute(query)
            print("Stored {}\'s min data at {}".format(dict_data['symbol'], dict_data['time']))

    def tick_stream_to_cassandra(self):
        """
        store streaming data of second frequency to Cassandra database
            :primary key: time,symbol
        :return: None
        
        """
        for msg in self.consumer2:
            # decode msg value from byte to utf-8
            dict_data = ast.literal_eval(msg.value.decode("utf-8"))

            # transform price data from string to float
            for key in ['open', 'high', 'low', 'close', 'volume', 'previous_close', 'change']:
                dict_data[key] = string_to_float(dict_data[key])

            dict_data['change_percent'] = float(dict_data['change_percent'].strip('%')) / 100.

            query = "INSERT INTO TICK (time, symbol, open, high, low, close, volume, previous_close, " \
                    "change, change_percent, last_trading_day) " \
                    "VALUES ('{}','{}', {}, {}, {}, {}, {}, {}, {}, {}, '{}');" \
                .format(dict_data['time'], dict_data['symbol'],
                        dict_data['open'], dict_data['high'], dict_data['low'], dict_data['close'], dict_data['volume'],
                        dict_data['previous_close'], dict_data['change'], dict_data['change_percent'],
                        dict_data['last_trading_day'])

            self.session.execute(query)
            print("Stored {}\'s tick data at {}".format(dict_data['symbol'], dict_data['time']))

    def update_cassandra_after_trading_day(self):
        """
        main function to update recent trading day's daily price (mainly for updating the adjusted close price),
        and 1min frequency price(to fill in empty data points caused by errors)
        """
        for symbol in SYMBOL_LIST[:]:
            value_daily = get_historical_data(symbol=symbol, outputsize='full')
            value_min, _ = get_intraday_data(symbol=symbol, outputsize='full', freq='1min')

            self.historical_to_cassandra(value_min, True)
            self.historical_to_cassandra(value_daily, False)
            time.sleep(15)

    def news_to_cassandra(self):
        for msg in self.consumer3:
            dict_data = ast.literal_eval(msg.value.decode("utf-8"))
            publishtime = dict_data['publishedAt'][:10] + ' ' + dict_data['publishedAt'][11:19]
            try:
                dict_data['description'] = dict_data['description'].replace('\'', '@@')
            except Exception as e:
                print(e)
            query = "INSERT INTO NEWS (date, publishedAt, source, title, description, url) " \
                    "VALUES ('{}', '{}', '{}', '{}', '{}', '{}');" \
                .format(publishtime[:10], publishtime,
                        dict_data['source']['name'],
                        dict_data['title'].replace('\'', '@@'),
                        dict_data['description'],
                        dict_data['url'])
            self.session.execute(query)

            # print("Stored news '{}' at {}".format(dict_data['title'],dict_data['publishedAt']))

    def delete_table(self, table_name):
        self.session.execute("DROP TABLE {}".format(table_name))


def main_realtime(symbol='^GSPC', tick=True):
    """
    main function to store realtime data;
    recommend to set tick=False, as getting tick data would cause rate limiting error from API
    """
    database = CassandraStorage()
    database.kafka_consumer()
    if tick is True:
        database.tick_stream_to_cassandra()
    else:
        database.stream_to_cassandra()


def main_realtime_news():
    database = CassandraStorage()
    database.kafka_consumer()
    database.news_to_cassandra()


def main_aftertradingday():
    """
    main function to update recent trading day's daily price (mainly for updating the adjusted close price),
    and 1min frequency price(to fill in empty data points caused by errors)
    """
    for symbol in SYMBOL_LIST[:]:
        value_daily = get_historical_data(symbol=symbol, outputsize='full')
        value_min, _ = get_intraday_data(symbol=symbol, outputsize='full', freq='1min')

        database = CassandraStorage()
        database.kafka_consumer()

        database.historical_to_cassandra(value_min, True)
        database.historical_to_cassandra(value_daily, False)
        time.sleep(15)


if __name__ == "__main__":
    database = CassandraStorage()
    pass
