from cassandra.cluster import Cluster
from pipeline.util.util import normalize_data, calculateEma, calculateRsi, SYMBOL_LIST, convertToDate
from pyspark.sql.functions import lag
from pyspark.sql.window import Window
from pyspark.sql.functions import lit

cluster = Cluster()
session = cluster.connect()

session.set_keyspace("stocks")
session.execute(
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

def insertToCassandra(row):
    query = "INSERT INTO HISTORICAL (time, symbol, open, high, low, close, volume, change, rsi, ema)"\
                  "VALUES ('{}','{}', {}, {}, {}, {}, {}, {}, {}, {});" \
                .format(convertToDate(row.date), row.symbol, row.open, row.high, row.low, row.close, row.volume, row.change, row.RSI, row.EMA )
    print(query)
    session.execute(query)

      
for symbol in SYMBOL_LIST:
    df = normalize_data(symbol)
    # df = df.withColumn('RSI', computeRSI(df.close))
    # df = df.withColumn('EMA', computeEMA(df.close))
    df = df.withColumn('symbol', lit(symbol))
    df = df.withColumn('change', (df.close - lag(df.close).over(Window.partitionBy('symbol').orderBy('date'))))
    df = calculateEma(df)
    df = calculateRsi(df)
    df = df.sort(df.date.asc())
    df = df.na.fill(0)
    # print(df.show())
    for row in df.collect():
        query = "INSERT INTO HISTORICAL (time, symbol, open, high, low, close, volume, change, rsi, ema)"\
            "VALUES ('{}','{}', {}, {}, {}, {}, {}, {}, {}, {});" \
          .format(convertToDate(row.date), row.symbol, row.open, row.high, row.low, row.close, row.volume, row.change, row.RSI, row.EMA )
        session.execute(query)
    print("Store {}'s historical data".format(symbol))