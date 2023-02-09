import ast
import time
from util.util import string_to_float, computeEMA, computeRSI
from util.util import SYMBOL_LIST
from util.config import config
from kafka import KafkaConsumer
from cassandra.cluster import Cluster, NoHostAvailable
import pandas as pd


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
                TIME timestamp,
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
            """CREATE TABLE IF NOT EXISTS TICK (
            TIME timestamp,
            SYMBOL text,
            OPEN float,
            HIGH float,
            LOW float,
            CLOSE float,
            VOLUME float,
            REF float,
            CEIL float,
            FLOOR float,
            PRIMARY KEY (SYMBOL, TIME)
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
            config['topic_name1'],
            bootstrap_servers=config['kafka_broker'])

    def tick_stream_to_cassandra(self):
        for msg in self.consumer2:
            # decode msg value from byte to utf-8
            dict_data = ast.literal_eval(msg.value.decode("utf-8"))
            print(dict_data)
            # transform price data from string to float
            for key in ['open', 'high', 'low', 'close', 'volume', 'previous_close', 'change']:
                dict_data[key] = string_to_float(dict_data[key])

            # dict_data['change_percent'] = float(dict_data['change_percent'].strip('%')) / 100.
            dict_data['change_percent'] = float(dict_data['change_percent']) / 100.
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
        for msg in self.consumer3:
            dict_data = ast.literal_eval(msg.value.decode("utf-8"))
            sql_query = "SELECT * FROM {}.{} WHERE SYMBOL = '{}' ORDER BY time DESC LIMIT 20".format("stocks", "historical", dict_data[0]['symbol'])
            df = pd.DataFrame()
            # df = df.sort_values(by='time').reset_index(drop=True)
            for row in self.session.execute(sql_query):
                df = df.append(pd.DataFrame(row, index=[0]))
            df = df.drop(columns=["ema", "rsi", "change"])
            df = df.head(20)
            df_2 = pd.DataFrame(dict_data)
            df_new = pd.concat([df_2, df])
            df_new['RSI'] = computeRSI(df_new['close'])
            df_new['EMA'] = computeEMA(df_new['close'])
            df_new['change'] = df_new['close'].pct_change()
            query = "INSERT INTO HISTORICAL (time, symbol, open, high, low, close, volume, change, rsi, ema)"\
              "VALUES ('{}','{}', {}, {}, {}, {}, {}, {}, {}, {});" \
            .format(df_new.loc[0]['date'], df_new.loc[0]['symbol'], df_new.loc[0]['open'], df_new.loc[0]['high'], df_new.loc[0]['low'], df_new.loc[0]['close'], df_new.loc[0]['volume'], df_new.loc[0]['change'], df_new.loc[0]['RSI'], df_new.loc[0]['EMA'] )
            self.session.execute(query)
            print("Stored {}\'s historical data at {}".format(df_new.loc[0]['symbol'], df_new.loc[0]['time']))

    def news_to_cassandra(self):
        for msg in self.consumer1:
            dict_data = ast.literal_eval(msg.value.decode("utf-8"))
            print(dict_data)
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
    database = CassandraStorage()
    database.kafka_consumer()
    database.update_cassandra_after_trading_day()


if __name__ == "__main__":
    # historical
    # main_aftertradingday()
    # tick
    # main_realtime()
    main_realtime_news()