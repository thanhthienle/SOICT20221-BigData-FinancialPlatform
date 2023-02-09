from cassandra.cluster import Cluster, NoHostAvailable
from pipeline.util.util import normalize_data, computeRSI, computeEMA, SYMBOL_LIST

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

for symbol in SYMBOL_LIST:
  df = normalize_data(symbol)
  df['RSI'] = computeRSI(df['close'])
  df['EMA'] = computeEMA(df['close'])
  df['change'] = df['close'].pct_change()
  for i in range(14, len(df)):
    query = "INSERT INTO HISTORICAL (time, symbol, open, high, low, close, volume, change, rsi, ema)"\
              "VALUES ('{}','{}', {}, {}, {}, {}, {}, {}, {}, {});" \
            .format(df.loc[i]['date'], symbol, df.loc[i]['open'], df.loc[i]['high'], df.loc[i]['low'], df.loc[i]['close'], df.loc[i]['volume'], df.loc[i]['change'], df.loc[i]['RSI'], df.loc[i]['EMA'] )
    session.execute(query)
  print(df)